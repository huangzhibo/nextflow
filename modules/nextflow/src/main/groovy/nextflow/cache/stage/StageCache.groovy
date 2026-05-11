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
import groovyx.gpars.dataflow.Dataflow
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
 *       workflow's declared inputs (post-{@code ChannelOut.spread}) and a
 *       {@code proceed} closure that runs the workflow body</li>
 * </ol>
 *
 * <p>The clone-substitute pattern is the only way to defer execution between
 * {@code closure.call()} (which synchronously registers processes) and the
 * asynchronous archive lookup decision (which has to wait for input channels
 * to drain).
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
    Object runStage(WorkflowDef workflow, Map<String, Object> inputs, Closure proceed) {
        ensureInit()
        if( !config0?.enabled )
            return proceed.call()

        // clone channel inputs in-place; the `inputs` snapshot retains the
        // *original* value for static entries so StageTake.build can serialize them
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

        // proceed registers processes wired onto the clones (which are still empty)
        final realOutput = proceed.call() as ChannelOut
        final placeholders = buildPlaceholders(realOutput)

        if( clonedChannels.isEmpty() ) {
            // Pure-static stage (no channel inputs in `take:`). Two notes:
            //
            //   1. Dispatch decide() to a GPars worker thread. archiveWithForward
            //      uses blocking getVal() on value-channel emits, which would
            //      deadlock on the main thread — the workflow body has registered
            //      the process but the task can't fire until the Nextflow barrier
            //      starts, which can't start until entry workflow returns, which
            //      can't return until this call does.
            //
            //   2. Cache hits *are* recorded and downstream placeholders are
            //      bound to archived emit, but the workflow's own process is
            //      not gated. Without channel inputs there is no clone we own —
            //      Nextflow auto-wraps the raw `take:` value into a channel
            //      when invoking the process, so by the time decide() resolves
            //      the hit, the process has already been scheduled. The
            //      orphaned process output is harmless (no downstream subscribers)
            //      but its execution cost is paid. Users who care can wrap the
            //      take value explicitly: WORKFLOW(Channel.value(params.x)).
            final take = StageTake.build(workflow.name, inputs, null, null, archive0, knownChecksums)
            Dataflow.task {
                decide(workflow.name, take, realOutput, placeholders, clonedChannels, null)
            }
        }
        else {
            subscribeAndCollect(workflow.name, inputs, realOutput, placeholders, clonedChannels)
        }

        return new ChannelOut(placeholders)
    }

    // -- internals --

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
            final archiveDirName = take.archiveDirName()
            final cached = archive0.findArchive(stageName, archiveDirName)
            if( cached != null ) {
                log.info "Reusing archived stage ${stageName} (${archiveDirName})"
                emitArchive(stageName, archiveDirName, cached, placeholders)
                stopClones(clonedChannels)
                appendCachedStageEntry(stageName, archiveDirName, cached)
                return
            }

            log.info "Executing stage ${stageName} (no archive for ${archiveDirName})"
            feedClones(clonedChannels, collected)
            if( config0.writable ) {
                archive0.archiveWithForward(stageName, take, realOutput, placeholders)
            }
            else {
                forwardOutputs(realOutput, placeholders)
            }
        }
        catch( Exception e ) {
            log.error "Stage ${stageName} archive/reuse failed, falling back to normal execution: ${e.message}", e
            feedClones(clonedChannels, collected)
            forwardOutputs(realOutput, placeholders)
        }
        finally {
            collected?.clear()    // release references once feed/stop has fired
        }
    }

    private static void stopClones(Map<String, ClonedChannel> clonedChannels) {
        for( final cc : clonedChannels.values() ) {
            cc.clone.bind(Channel.STOP)
        }
    }

    private static void feedClones(Map<String, ClonedChannel> clonedChannels,
                                   Map<String, List<Object>> collected) {
        if( collected == null ) {
            stopClones(clonedChannels)
            return
        }
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
