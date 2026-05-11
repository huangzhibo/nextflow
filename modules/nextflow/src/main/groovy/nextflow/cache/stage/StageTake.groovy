/*
 * Copyright 2013-2026, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nextflow.cache.stage

import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.util.concurrent.ConcurrentMap

import com.google.common.hash.Hashing
import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * The {@code take} structure of a named workflow invocation — a serializable,
 * canonical view of every declared input at the moment the workflow runs.
 *
 * <p>The 16-char SHA-256 of the canonicalized take is used as the archive
 * directory name (see {@link #archiveDirName}). The hashed projection
 * deliberately excludes file {@code path} fields so the same input set
 * resolves to the same digest across machines / cluster relocations.
 *
 * <p>File elements in the stored take additionally carry {@code path} so a
 * later run, faced with a deleted source file, can scan prior archives'
 * takes and recover the original checksum
 * (see {@link #computeFileChecksum}).
 *
 * <p>Storage shape (per input name):
 * <pre>
 *   {
 *     "type": "queue" | "value",
 *     "items": [
 *       [ { "type":"value", "data":... },
 *         { "type":"file",  "name":..., "path":..., "checksum":"sha256:..." } ],
 *       ...
 *     ]
 *   }
 * </pre>
 */
@Slf4j
@CompileStatic
class StageTake {

    static final String SHA256_PREFIX = 'sha256:'
    static final int ARCHIVE_DIR_LEN = 16

    private final Map<String, Map> data

    StageTake(Map<String, Map> data) {
        this.data = data
    }

    /** @return the take structure as stored in stage.json (inputs canonicalized by name). */
    Map<String, Map> toStorageMap() {
        return canonicalize(data) as Map<String, Map>
    }

    /** @return 16-char hex hash over the canonical JSON of the take with {@code path} fields stripped. */
    String archiveDirName() {
        final stripped = stripPaths(data)
        final canonical = canonicalize(stripped)
        final bytes = JsonOutput.toJson(canonical).getBytes('UTF-8')
        return Hashing.sha256().hashBytes(bytes).toString().substring(0, ARCHIVE_DIR_LEN)
    }

    // -- build --

    /**
     * Build a {@link StageTake} from a workflow's declared inputs.
     *
     * @param stageName        the workflow name (used in scan-fallback diagnostics)
     * @param declaredInputs   ordered map of (input name → static value), in declaration order;
     *                         entries for channel inputs are placeholders and ignored
     * @param channelEmissions map of (input name → collected emissions) for channel inputs
     * @param valueChannel     map of (input name → isValue) for channel inputs
     * @param archive          archive root, used by step-3 fallback on file-not-found
     * @param knownChecksums   shared (Path → checksum) registry; mutated by step-3 fallback
     */
    static StageTake build(String stageName,
                           Map<String, Object> declaredInputs,
                           Map<String, List<Object>> channelEmissions,
                           Map<String, Boolean> valueChannel,
                           StageArchive archive,
                           ConcurrentMap<Path, String> knownChecksums) {
        final out = new LinkedHashMap<String, Map>()
        for( final name : declaredInputs.keySet().toSorted() ) {
            if( channelEmissions != null && channelEmissions.containsKey(name) ) {
                final emissions = channelEmissions.get(name)
                final isValue = Boolean.TRUE.equals(valueChannel?.get(name))
                final items = new ArrayList<List<Map>>()
                for( final em : emissions )
                    items.add(serializeValue(em, stageName, archive, knownChecksums))
                out.put(name, [type: isValue ? 'value' : 'queue', items: items] as Map)
            }
            else {
                final value = declaredInputs.get(name)
                final items = [serializeValue(value, stageName, archive, knownChecksums)]
                out.put(name, [type: 'value', items: items] as Map)
            }
        }
        return new StageTake(out)
    }

    // -- 3-step file-checksum fallback --

    /**
     * Resolve the SHA-256 checksum of {@code path} via, in order:
     * <ol>
     *   <li>direct read of the file if readable;</li>
     *   <li>{@code knownChecksums} registry lookup;</li>
     *   <li>lazy scan of this stage's historical archives (via
     *       {@link StageArchive#scanThisStageArchives}), then retry the registry.</li>
     * </ol>
     *
     * @throws IllegalStateException if the file is gone and no archive of
     *         this stage has ever recorded a checksum for it.
     */
    static String computeFileChecksum(String stageName,
                                      Path path,
                                      StageArchive archive,
                                      ConcurrentMap<Path, String> knownChecksums) {
        final canonical = path.toAbsolutePath().normalize()

        if( Files.isReadable(canonical) )
            return SHA256_PREFIX + sha256OfFile(canonical)

        def cached = knownChecksums?.get(canonical)
        if( cached != null )
            return cached

        if( archive != null && knownChecksums != null ) {
            archive.scanThisStageArchives(stageName, knownChecksums)
            cached = knownChecksums.get(canonical)
            if( cached != null )
                return cached
        }

        throw new IllegalStateException(
                "Input ${canonical} of stage ${stageName} does not exist and has never been recorded in any archive of this stage")
    }

    // -- internals --

    private static List<Map> serializeValue(Object value,
                                            String stageName,
                                            StageArchive archive,
                                            ConcurrentMap<Path, String> knownChecksums) {
        if( value instanceof List ) {
            final list = (List) value
            return list.collect { el -> serializeElement(el, stageName, archive, knownChecksums) }
        }
        return [serializeElement(value, stageName, archive, knownChecksums)]
    }

    private static Map serializeElement(Object el,
                                        String stageName,
                                        StageArchive archive,
                                        ConcurrentMap<Path, String> knownChecksums) {
        if( el instanceof Path ) {
            final canonical = ((Path) el).toAbsolutePath().normalize()
            final checksum = computeFileChecksum(stageName, canonical, archive, knownChecksums)
            return [type: 'file', name: canonical.fileName.toString(), path: canonical.toString(), checksum: checksum]
        }
        return [type: 'value', data: el]
    }

    private static String sha256OfFile(Path file) {
        final md = MessageDigest.getInstance('SHA-256')
        file.withInputStream { is ->
            final buf = new byte[8192]
            int n
            while( (n = is.read(buf)) > 0 ) md.update(buf, 0, n)
        }
        return md.digest().encodeHex().toString()
    }

    /** Deep-copy {@code take}, dropping {@code path} from every file element. */
    private static Map<String, Map> stripPaths(Map<String, Map> take) {
        final out = new LinkedHashMap<String, Map>()
        for( final e : take.entrySet() ) {
            final chData = e.value
            final items = chData.get('items') as List<List>
            final newItems = items.collect { row ->
                (row as List).collect { el ->
                    final m = el as Map
                    if( m.get('type') == 'file' )
                        return [type: 'file', name: m.get('name'), checksum: m.get('checksum')]
                    return m
                }
            }
            out.put(e.key, [type: chData.get('type'), items: newItems] as Map)
        }
        return out
    }

    /** Recursively wrap maps in {@link TreeMap} so canonical JSON has deterministic key order. */
    private static Object canonicalize(Object o) {
        if( o instanceof Map ) {
            final sorted = new TreeMap<String, Object>()
            for( final e : ((Map) o).entrySet() )
                sorted.put(e.key as String, canonicalize(e.value))
            return sorted
        }
        if( o instanceof List )
            return ((List) o).collect { canonicalize(it) }
        return o
    }
}
