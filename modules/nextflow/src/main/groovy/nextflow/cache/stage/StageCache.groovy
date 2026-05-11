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
import java.util.concurrent.atomic.AtomicInteger

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowReadChannel
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
 * Lifecycle:
 * <ol>
 *   <li>{@link WorkflowDef#run0} calls {@link #isEnabled} to gate the hook</li>
 *   <li>If enabled, the hook calls {@link #runStage} with a snapshot of the
 *       workflow's declared inputs (post-{@code ChannelOut.spread}) and a
 *       {@code proceed} closure that runs the workflow body</li>
 * </ol>
 *
 * The clone-substitute pattern (see comments on {@link #runStage}) is the only
 * way to defer execution between {@code closure.call()} (which synchronously
 * registers processes) and the asynchronous {@code archive.findArchive}
 * decision (which has to wait for input channels to drain).
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

    /** Reset state (for tests). */
    synchronized void reset() {
        initialized = false
        config0 = null
        archive0 = null
        cachedStagesTsv = null
        headerWritten = false
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

        // clone channel inputs in-place; static inputs feed straight into the digest
        final clonedChannels = new LinkedHashMap<String, ClonedChannel>()
        final staticInputs = new LinkedHashMap<String, Object>()

        for( final inputName : new ArrayList<String>(inputs.keySet()) ) {
            final value = inputs.get(inputName)
            if( CH.isChannel(value) ) {
                final isValue = CH.isValue(value)
                final clone = CH.create(isValue)
                clonedChannels.put(inputName, new ClonedChannel(inputName, value, clone, isValue))
                inputs.put(inputName, clone)
            }
            else {
                staticInputs.put(inputName, value)
            }
        }

        final staticDigest = StageDigest.computeStatic(workflow.name, staticInputs)

        // proceed registers processes wired onto the clones (which are still empty)
        final realOutput = proceed.call() as ChannelOut
        final placeholders = buildPlaceholders(realOutput)

        if( clonedChannels.isEmpty() ) {
            // no channel inputs → digest already known, decide immediately
            final digest = StageDigest.computeFinal(staticDigest, Collections.<String, List<Object>> emptyMap())
            decide(workflow.name, digest, realOutput, placeholders, clonedChannels, null)
        }
        else {
            subscribeAndCollect(workflow.name, staticDigest, realOutput, placeholders, clonedChannels)
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

    private static final String TSV_HEADER = "stage\tdigest\ttask_count\tarchive_path\tarchived_at\tmode\n"

    private synchronized void appendCachedStageEntry(Map cached, String mode) {
        if( cachedStagesTsv == null ) return
        final stage = cached.get('stage') as String
        final digest = cached.get('content_digest') as String
        final archivedAt = cached.get('created_at') as String
        final archivePath = archive0.archivePath(stage, digest)
        // task_count is 0 until task-hash back-fill lands (M3+)
        if( !headerWritten ) {
            Files.write(cachedStagesTsv, TSV_HEADER.getBytes('UTF-8'))
            headerWritten = true
        }
        final line = "${stage}\t${digest}\t0\t${archivePath}\t${archivedAt}\t${mode}\n"
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
     * compute the final digest and dispatch the hit/miss decision.
     */
    private void subscribeAndCollect(String stageName,
                                     String staticDigest,
                                     ChannelOut realOutput,
                                     Map<String, DataflowWriteChannel> placeholders,
                                     Map<String, ClonedChannel> clonedChannels) {
        final collected = new LinkedHashMap<String, List<Object>>()
        final pending = new AtomicInteger(clonedChannels.size())

        for( final cc : clonedChannels.values() ) {
            final String capturedName = cc.inputName
            collected.put(capturedName, Collections.synchronizedList(new ArrayList<Object>()))
            DataflowHelper.subscribeImpl(CH.getReadChannel(cc.original), [
                onNext: { Object value ->
                    collected.get(capturedName).add(value)
                } as Closure,
                onComplete: {
                    if( pending.decrementAndGet() == 0 ) {
                        final digest = StageDigest.computeFinal(staticDigest, collected)
                        decide(stageName, digest, realOutput, placeholders, clonedChannels, collected)
                    }
                } as Closure
            ] as Map<String, Closure>)
        }
    }

    private void decide(String stageName,
                        String digest,
                        ChannelOut realOutput,
                        Map<String, DataflowWriteChannel> placeholders,
                        Map<String, ClonedChannel> clonedChannels,
                        Map<String, List<Object>> collected) {
        try {
            final cached = archive0.findArchive(stageName, digest)
            if( cached != null ) {
                log.info "Reusing archived stage ${stageName} (${digest})"
                emitArchive(cached, placeholders)
                stopClones(clonedChannels)
                appendCachedStageEntry(cached, 'reuse')
                return
            }

            log.info "Executing stage ${stageName} (no archive for ${digest})"
            feedClones(clonedChannels, collected)
            if( config0.writable ) {
                archive0.archiveWithForward(stageName, digest, realOutput, placeholders)
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

    private void emitArchive(Map cached, Map<String, DataflowWriteChannel> placeholders) {
        final stageName = cached.get('stage') as String
        final contentDigest = cached.get('content_digest') as String
        final basePath = archive0.archivePath(stageName, contentDigest)
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
