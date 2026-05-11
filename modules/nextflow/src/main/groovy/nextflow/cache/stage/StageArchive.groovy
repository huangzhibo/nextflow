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
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.security.DigestInputStream
import java.security.MessageDigest
import java.time.OffsetDateTime
import java.util.concurrent.atomic.AtomicInteger

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowWriteChannel
import nextflow.Channel
import nextflow.extension.CH
import nextflow.extension.DataflowHelper
import nextflow.script.ChannelOut

/**
 * Stage archive read/write under {@link #archiveRoot}.
 *
 * Layout:
 * <pre>
 *   archiveRoot/
 *     STAGE_NAME/
 *       &lt;digest-prefix-16&gt;/
 *         stage.json                    metadata + serialized channel data
 *         0/                            files for emission 0 (if any)
 *           foo.bam
 *         1/                            ...
 * </pre>
 *
 * {@code stage.json} schema (v1):
 * <pre>
 * {
 *   "schema_version": "v1",
 *   "stage": "ALIGN",
 *   "content_digest": "sha256:...",
 *   "created_at": "2026-05-11T...",
 *   "emit": {
 *     "bam": {
 *       "type": "queue",
 *       "items": [
 *         [ { "type":"value", "data":{...} }, { "type":"file", "name":"x.bam", "checksum":"sha256:...", "size":N } ],
 *         ...
 *       ]
 *     }
 *   }
 * }
 * </pre>
 *
 * <p>Archive completeness is implied by the presence of {@code stage.json}:
 * {@link #writeArchive} writes every per-emission file directory <em>before</em>
 * the {@code stage.json} marker, so any {@code stage.json} on disk is the
 * commit point of a fully-written archive.
 */
@Slf4j
@CompileStatic
class StageArchive {

    static final String SCHEMA_VERSION = 'v1'
    static final int DIGEST_PREFIX_LEN = 16

    private final Path archiveRoot

    StageArchive(Path archiveRoot) {
        this.archiveRoot = archiveRoot
    }

    Path getArchiveRoot() { archiveRoot }

    Path archivePath(String stageName, String digest) {
        final raw = digest.startsWith(StageDigest.PREFIX) ? digest.substring(StageDigest.PREFIX.length()) : digest
        final prefix = raw.length() > DIGEST_PREFIX_LEN ? raw.substring(0, DIGEST_PREFIX_LEN) : raw
        return archiveRoot.resolve(stageName).resolve(prefix)
    }

    /**
     * @return the parsed {@code stage.json} when an archive exists for the
     *         given stage + content digest; null on absence or on a corrupted
     *         file (warnings logged for the corrupted case).
     */
    Map findArchive(String stageName, String contentDigest) {
        final stageJson = archivePath(stageName, contentDigest).resolve('stage.json')
        if( !Files.isRegularFile(stageJson) )
            return null
        try {
            return new JsonSlurper().parse(stageJson.toFile()) as Map
        }
        catch( Exception e ) {
            log.warn "Failed to read stage.json at ${stageJson}, ignoring: ${e.message}"
            return null
        }
    }

    /**
     * Reconstruct an emission value from its archived form. The result is
     * either a single {@code Path}/value (single-element emission) or a
     * {@code List} of mixed paths/values (tuple emission).
     */
    static Object rebuildValue(List<Map> elements, Path itemDir) {
        if( elements.size() == 1 )
            return rebuildElement(elements[0], itemDir)
        return elements.collect { rebuildElement(it, itemDir) }
    }

    /**
     * Subscribe once to {@code output}, forwarding each emission to the matching
     * placeholder AND collecting it for archiving. Single subscription avoids
     * the "double consume" problem on {@link groovyx.gpars.dataflow.DataflowQueue}.
     *
     * When all queue channels have completed, persists {@code stage.json} and
     * the per-emission file directories under {@link #archivePath(String, String)}.
     */
    void archiveWithForward(String stageName,
                            String contentDigest,
                            ChannelOut output,
                            Map<String, DataflowWriteChannel> placeholders) {
        final names = output.getNames()
        if( !names ) return

        final collected = new LinkedHashMap<String, List<Object>>()
        final channelTypes = new LinkedHashMap<String, String>()
        int queueCount = 0

        for( final name : names ) {
            final ch = output.getProperty(name)
            final isValue = CH.isValue(ch)
            channelTypes.put(name, isValue ? 'value' : 'queue')
            if( isValue ) {
                final value = ((DataflowReadChannel) ch).getVal()
                collected.put(name, [value] as List<Object>)
                final dstCh = placeholders.get(name)
                if( dstCh != null ) dstCh.bind(value)
            }
            else {
                collected.put(name, Collections.synchronizedList(new ArrayList<Object>()))
                queueCount++
            }
        }

        if( queueCount == 0 ) {
            writeArchive(stageName, contentDigest, collected, channelTypes)
            return
        }

        final pending = new AtomicInteger(queueCount)
        for( final name : names ) {
            if( channelTypes.get(name) == 'value' ) continue
            // per-iteration capture: closures fire asynchronously, must not
            // share the loop variable across iterations
            final String capturedName = name
            final DataflowWriteChannel capturedDst = placeholders.get(capturedName)
            final readCh = CH.getReadChannel(output.getProperty(capturedName))
            DataflowHelper.subscribeImpl(readCh, [
                onNext: { Object value ->
                    collected.get(capturedName).add(value)
                    if( capturedDst != null ) capturedDst.bind(value)
                } as Closure,
                onComplete: {
                    if( capturedDst != null ) capturedDst.bind(Channel.STOP)
                    if( pending.decrementAndGet() == 0 )
                        writeArchive(stageName, contentDigest, collected, channelTypes)
                } as Closure
            ] as Map<String, Closure>)
        }
    }

    // -- private --

    private void writeArchive(String stageName,
                              String contentDigest,
                              Map<String, List<Object>> collected,
                              Map<String, String> channelTypes) {
        final path = archivePath(stageName, contentDigest)
        // single-writer arbitration: first writer wins, subsequent writers no-op
        if( Files.exists(path.resolve('stage.json')) ) {
            log.debug "Stage ${stageName} archive already exists at ${path}, skipping write"
            return
        }
        Files.createDirectories(path)

        final emitJson = new LinkedHashMap<String, Map>()
        for( final chEntry : collected.entrySet() ) {
            final name = chEntry.key
            final itemsJson = new ArrayList<List>()
            int idx = 0
            for( final value : chEntry.value ) {
                final itemDir = path.resolve(String.valueOf(idx))
                itemsJson.add(serializeValue(value, itemDir))
                idx++
            }
            emitJson.put(name, [type: channelTypes.get(name), items: itemsJson])
        }

        final stageData = [
            schema_version: SCHEMA_VERSION,
            stage         : stageName,
            content_digest: contentDigest,
            created_at    : OffsetDateTime.now().toString(),
            emit          : emitJson,
        ]
        // stage.json is the commit marker: write it last so any presence-on-disk
        // implies a fully-written archive (see writeArchive doc on the class).
        final json = JsonOutput.prettyPrint(JsonOutput.toJson(stageData))
        Files.write(path.resolve('stage.json'), json.getBytes('UTF-8'))
        log.info "Stage ${stageName} archived to ${path}"
    }

    private static List<Map> serializeValue(Object value, Path itemDir) {
        if( value instanceof List ) {
            final list = (List) value
            if( list.any { it instanceof Path } )
                Files.createDirectories(itemDir)
            return list.collect { el -> serializeElement(el, itemDir) }
        }
        if( value instanceof Path )
            Files.createDirectories(itemDir)
        return [serializeElement(value, itemDir)]
    }

    private static Map serializeElement(Object el, Path itemDir) {
        if( el instanceof Path ) {
            final file = (Path) el
            final fileName = file.fileName.toString()
            final target = itemDir.resolve(fileName)
            final checksum = copyWithChecksum(file, target)
            return [type: 'file', name: fileName, checksum: checksum, size: Files.size(target)]
        }
        return [type: 'value', data: el]
    }

    private static Object rebuildElement(Map el, Path itemDir) {
        if( el.get('type') == 'file' )
            return itemDir.resolve(el.get('name') as String)
        return el.get('data')
    }

    static String copyWithChecksum(Path source, Path target) {
        final digest = MessageDigest.getInstance('SHA-256')
        source.withInputStream { raw ->
            Files.copy(new DigestInputStream(raw, digest), target, StandardCopyOption.REPLACE_EXISTING)
        }
        return StageDigest.PREFIX + digest.digest().encodeHex().toString()
    }
}
