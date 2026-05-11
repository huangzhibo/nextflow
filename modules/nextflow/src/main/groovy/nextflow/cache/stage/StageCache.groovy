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
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowWriteChannel
import nextflow.Channel
import nextflow.Global
import nextflow.Session
import nextflow.extension.CH
import nextflow.extension.DataflowHelper
import nextflow.script.ChannelOut
import nextflow.script.WorkflowDef

/**
 * Stage cache orchestrator.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>{@link WorkflowDef#run0} calls {@link #isEnabled} to gate the hook</li>
 *   <li>If enabled, the hook calls {@link #runStage} with a snapshot of the
 *       workflow's declared inputs (post-{@code ChannelOut.spread}), the
 *       declared emit names, and a {@code proceed} closure that runs the
 *       workflow body</li>
 * </ol>
 *
 * <p>Two execution paths based on whether any take slot is a channel:
 * <ul>
 *   <li><b>Pure-static</b> (no channel inputs): digest is computable up front.
 *       On HIT, {@code proceed} is never called — the workflow's processes are
 *       neither registered nor executed. On MISS, {@code proceed} runs and
 *       {@link StageArchive#archiveWithForward} registers async subscriptions
 *       that capture emissions as they arrive. See {@link #runStageStatic}.</li>
 *   <li><b>Has-channel</b>: clone-substitute pattern — channel inputs are
 *       replaced with empty clones, the body wires processes onto the clones,
 *       and on first emit (from a worker thread) we decide hit/miss and
 *       either feed archived data and STOP the clones (gating processes) or
 *       feed real input through.</li>
 * </ul>
 *
 * <p>{@link #knownChecksums} is a per-run registry used by
 * {@link StageTake#computeFileChecksum} for the source-deletion fallback —
 * populated lazily by scanning historical archives of the same stage when a
 * declared input path is no longer readable.
 */
@Slf4j
@CompileStatic
@Singleton
class StageCache {

    private volatile boolean initialized
    private StageConfig config0
    private StageArchive archive0
    private Path cachedStagesTsv
    private volatile boolean headerWritten
    private final ConcurrentHashMap<Path, String> knownChecksums = new ConcurrentHashMap<>()

    /** Reset state (for tests). */
    synchronized void reset() {
        initialized = false
        config0 = null
        archive0 = null
        cachedStagesTsv = null
        headerWritten = false
        knownChecksums.clear()
    }

    /** @return whether stage cache is configured (archiveRoot set). */
    boolean isEnabled() {
        ensureInit()
        config0?.enabled
    }

    StageArchive getArchive() {
        ensureInit()
        archive0
    }

    /**
     * Run a named workflow under stage cache control.
     *
     * <p>The {@code inputs} map is a writable snapshot of the workflow's
     * declared inputs (post-spread). For every channel-typed input, this
     * method substitutes a clone in-place; the {@code proceed} closure is
     * expected to sync the mutated map back into the workflow body's binding
     * before running the body.
     *
     * <p>Returns a placeholder {@link ChannelOut}. The caller may use it
     * immediately as the workflow result; the placeholder's channels are
     * filled asynchronously once the cache decision is made.
     */
    Object runStage(WorkflowDef workflow,
                    Map<String, Object> inputs,
                    List<String> declaredOutputs,
                    Closure proceed) {
        ensureInit()
        if( !config0?.enabled )
            return proceed.call()

        // Clone channel inputs in-place; static entries stay as original values
        // so StageTake.build can serialize them directly.
        final clonedChannels = new LinkedHashMap<String, ClonedChannel>()
        for( final inputName : new ArrayList<String>(inputs.keySet()) ) {
            final value = inputs.get(inputName)
            if( CH.isChannel(value) ) {
                final isValue = CH.isValue(value)
                final clone = CH.create(isValue)
                clonedChannels.put(inputName, new ClonedChannel(inputName, value, clone, isValue))
                inputs.put(inputName, clone)
            }
        }

        // Pure-static: digest computable without waiting for any channel.
        // Decide before calling proceed so HIT can skip running the body.
        if( clonedChannels.isEmpty() ) {
            return runStageStatic(workflow.name, inputs, declaredOutputs, proceed)
        }

        // Has-channel: clone-trick + async decide on first emit.
        final realOutput = proceed.call() as ChannelOut
        final placeholders = buildPlaceholders(realOutput)
        subscribeAndCollect(workflow.name, inputs, realOutput, placeholders, clonedChannels)
        return new ChannelOut(placeholders)
    }

    // -- internals --

    private Object runStageStatic(String stageName,
                                  Map<String, Object> inputs,
                                  List<String> declaredOutputs,
                                  Closure proceed) {
        final take = StageTake.build(stageName, inputs, null, null, archive0, knownChecksums)
        final archiveDirName = take.archiveDirName()
        final cached = archive0.findArchive(stageName, archiveDirName)

        if( cached != null ) {
            try {
                log.info "Reusing archived stage ${stageName} (${archiveDirName})"
                final placeholders = buildStaticPlaceholders(declaredOutputs, cached)
                emitArchive(stageName, archiveDirName, cached, placeholders)
                stopUnarchivedPlaceholders(placeholders, cached)
                appendCachedStageEntry(stageName, archiveDirName, cached)
                return new ChannelOut(placeholders)
            }
            catch( Exception e ) {
                log.error "Stage ${stageName} archive reuse failed, falling back to execution: ${e.message}", e
            }
        }

        log.info "Executing stage ${stageName} (no archive for ${archiveDirName})"
        final realOutput = proceed.call() as ChannelOut
        final placeholders = buildPlaceholders(realOutput)
        try {
            if( config0.writable )
                archive0.archiveWithForward(stageName, take, realOutput, placeholders)
            else
                forwardOutputs(realOutput, placeholders)
        }
        catch( Exception e ) {
            log.error "Stage ${stageName} archive write failed, forwarding without archive: ${e.message}", e
            forwardOutputs(realOutput, placeholders)
        }
        return new ChannelOut(placeholders)
    }

    private synchronized void ensureInit() {
        if( initialized ) return
        final session = Global.session as Session
        config0 = StageConfig.getConfig(session)
        if( config0.enabled ) {
            final launchDir = Paths.get('.').toRealPath()
            archive0 = new StageArchive(launchDir.resolve(config0.archiveRoot))
            cachedStagesTsv = launchDir.resolve(config0.cachedStagesFile)
            // each run starts a fresh report — cached-stages.tsv is per-run
            Files.deleteIfExists(cachedStagesTsv)
            headerWritten = false
            log.debug "Stage cache initialized: archiveRoot=${archive0.archiveRoot}, writable=${config0.writable}, cachedStagesFile=${cachedStagesTsv}"
        }
        initialized = true
    }

    private static final String TSV_HEADER = "stage\tdigest\tarchive_path\tarchived_at\n"

    private synchronized void appendCachedStageEntry(String stageName, String archiveDirName, Map cached) {
        if( cachedStagesTsv == null ) return
        final archivedAt = cached.get('created_at') as String
        final archivePath = archive0.archivePath(stageName, archiveDirName)
        if( !headerWritten ) {
            Files.write(cachedStagesTsv, TSV_HEADER.getBytes('UTF-8'))
            headerWritten = true
        }
        final line = "${stageName}\t${archiveDirName}\t${archivePath}\t${archivedAt}\n"
        Files.write(cachedStagesTsv, line.getBytes('UTF-8'), StandardOpenOption.APPEND)
    }

    private static Map<String, DataflowWriteChannel> buildPlaceholders(ChannelOut realOutput) {
        final result = new LinkedHashMap<String, DataflowWriteChannel>()
        for( final name : realOutput.getNames() ) {
            final ch = realOutput.getProperty(name)
            result.put(name, CH.create(CH.isValue(ch)))
        }
        return result
    }

    /**
     * Build placeholders for a pure-static HIT, keyed by declared emit names.
     * Channel type (value vs queue) is taken from the archive when available;
     * declared names missing from the archive default to queue (will be
     * STOP-terminated by {@link #stopUnarchivedPlaceholders}).
     */
    private static Map<String, DataflowWriteChannel> buildStaticPlaceholders(
            List<String> declaredOutputs, Map cached) {
        final emitMap = cached.get('emit') as Map<String, Map>
        final result = new LinkedHashMap<String, DataflowWriteChannel>()
        for( final name : declaredOutputs ) {
            final chData = emitMap?.get(name)
            final isValue = chData != null && chData.get('type') == 'value'
            result.put(name, CH.create(isValue))
        }
        return result
    }

    /** Bind STOP to queue placeholders that have no corresponding archive emit. */
    private static void stopUnarchivedPlaceholders(Map<String, DataflowWriteChannel> placeholders,
                                                   Map cached) {
        final emitMap = cached.get('emit') as Map<String, Map>
        for( final entry : placeholders.entrySet() ) {
            if( !emitMap.containsKey(entry.key) && !CH.isValue(entry.value) ) {
                entry.value.bind(Channel.STOP)
            }
        }
    }

    /**
     * Subscribe once per original channel; accumulate emissions; when all complete,
     * build the take and dispatch the hit/miss decision.
     */
    private void subscribeAndCollect(String stageName,
                                     Map<String, Object> declaredInputs,
                                     ChannelOut realOutput,
                                     Map<String, DataflowWriteChannel> placeholders,
                                     Map<String, ClonedChannel> clonedChannels) {
        final collected = new LinkedHashMap<String, List<Object>>()
        final isValueMap = new LinkedHashMap<String, Boolean>()
        final pending = new AtomicInteger(clonedChannels.size())

        for( final cc : clonedChannels.values() ) {
            final String capturedName = cc.inputName
            collected.put(capturedName, Collections.synchronizedList(new ArrayList<Object>()))
            isValueMap.put(capturedName, cc.isValue)
            DataflowHelper.subscribeImpl(CH.getReadChannel(cc.original), [
                onNext: { Object value ->
                    collected.get(capturedName).add(value)
                } as Closure,
                onComplete: {
                    if( pending.decrementAndGet() == 0 ) {
                        final take = StageTake.build(stageName, declaredInputs, collected, isValueMap, archive0, knownChecksums)
                        decide(stageName, take, realOutput, placeholders, clonedChannels, collected)
                    }
                } as Closure
            ] as Map<String, Closure>)
        }
    }

    private void decide(String stageName,
                        StageTake take,
                        ChannelOut realOutput,
                        Map<String, DataflowWriteChannel> placeholders,
                        Map<String, ClonedChannel> clonedChannels,
                        Map<String, List<Object>> collected) {
        try {
            // -- Stage 1: lookup. placeholders/clones not yet touched, so a
            //    failure here can safely fall back to normal execution.
            //    findArchive already swallows JSON corruption (returns null),
            //    so reaching this catch is unusual.
            String archiveDirName
            Map cached
            try {
                archiveDirName = take.archiveDirName()
                cached = archive0.findArchive(stageName, archiveDirName)
            }
            catch( Exception e ) {
                log.error "Stage ${stageName} cache lookup failed, executing without cache: ${e.message}", e
                feedClones(clonedChannels, collected)
                forwardOutputs(realOutput, placeholders)
                return
            }

            // -- Stage 2: act on the decision. Once placeholders/clones are
            //    partially driven, re-doing fallback would double-bind. Abort
            //    the session so the pipeline fails fast instead of producing
            //    corrupted downstream data.
            try {
                if( cached != null ) {
                    log.info "Reusing archived stage ${stageName} (${archiveDirName})"
                    emitArchive(stageName, archiveDirName, cached, placeholders)
                    stopClones(clonedChannels)
                }
                else {
                    log.info "Executing stage ${stageName} (no archive for ${archiveDirName})"
                    feedClones(clonedChannels, collected)
                    if( config0.writable )
                        archive0.archiveWithForward(stageName, take, realOutput, placeholders)
                    else
                        forwardOutputs(realOutput, placeholders)
                }
            }
            catch( Exception e ) {
                log.error "Stage ${stageName} ${cached != null ? 'reuse' : 'execute'} failed mid-operation; aborting session to avoid corrupted output: ${e.message}", e
                (Global.session as Session)?.abort(e)
                return
            }

            // -- Stage 3: audit tsv. Best-effort; failure here is non-fatal.
            if( cached != null ) {
                try {
                    appendCachedStageEntry(stageName, archiveDirName, cached)
                }
                catch( Exception e ) {
                    log.warn "Stage ${stageName}: failed to write cached-stages report: ${e.message}"
                }
            }
        }
        finally {
            collected.clear()    // release references once feed/stop has fired
        }
    }

    private static void stopClones(Map<String, ClonedChannel> clonedChannels) {
        for( final cc : clonedChannels.values() ) {
            cc.clone.bind(Channel.STOP)
        }
    }

    private static void feedClones(Map<String, ClonedChannel> clonedChannels,
                                   Map<String, List<Object>> collected) {
        for( final cc : clonedChannels.values() ) {
            final values = collected.get(cc.inputName)
            if( values != null ) {
                for( final value : values ) {
                    cc.clone.bind(value)
                }
            }
            if( !cc.isValue ) cc.clone.bind(Channel.STOP)
        }
    }

    private void emitArchive(String stageName,
                             String archiveDirName,
                             Map cached,
                             Map<String, DataflowWriteChannel> placeholders) {
        final basePath = archive0.archivePath(stageName, archiveDirName)
        final emitMap = cached.get('emit') as Map<String, Map>

        for( final entry : emitMap.entrySet() ) {
            final placeholder = placeholders.get(entry.key)
            if( placeholder == null ) continue
            final chData = entry.value
            final items = chData.get('items') as List<List>
            final isValue = chData.get('type') == 'value'

            int idx = 0
            for( final elems : items ) {
                placeholder.bind(StageArchive.rebuildValue(elems as List<Map>, basePath.resolve(String.valueOf(idx))))
                idx++
            }
            if( !isValue )
                placeholder.bind(Channel.STOP)
        }
    }

    private static void forwardOutputs(ChannelOut realOutput,
                                       Map<String, DataflowWriteChannel> placeholders) {
        for( final name : realOutput.getNames() ) {
            // per-iteration capture for async callback safety
            final DataflowWriteChannel capturedDst = placeholders.get(name)
            final srcCh = realOutput.getProperty(name)
            final boolean capturedIsValue = CH.isValue(srcCh)
            DataflowHelper.subscribeImpl(CH.getReadChannel(srcCh), [
                onNext: { Object value -> capturedDst.bind(value) } as Closure,
                onComplete: {
                    if( !capturedIsValue ) capturedDst.bind(Channel.STOP)
                } as Closure
            ] as Map<String, Closure>)
        }
    }

    @CompileStatic
    static class ClonedChannel {
        final String inputName
        final Object original                         // DataflowReadChannel or DataflowBroadcast
        final DataflowWriteChannel clone
        final boolean isValue

        ClonedChannel(String inputName, Object original, DataflowWriteChannel clone, boolean isValue) {
            this.inputName = inputName
            this.original = original
            this.clone = clone
            this.isValue = isValue
        }
    }
}
