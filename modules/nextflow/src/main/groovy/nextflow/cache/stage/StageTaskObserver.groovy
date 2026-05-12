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

import java.util.concurrent.ConcurrentHashMap

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.trace.TraceObserverV2
import nextflow.trace.event.TaskEvent

/**
 * Collects task work-dir hash logs grouped by stage name, then patches each
 * archived {@code stage.json} with a {@code task_hashes} list at flow
 * completion.
 *
 * <p>Stage name is taken as the prefix of the qualified process name before
 * the first {@code ':'} (e.g. {@code ALIGN:BWA_MEM} → {@code ALIGN}).
 * Top-level processes (no colon) are ignored.
 *
 * <p>Patching only fires for stages recorded in
 * {@link StageCache#getArchivedStages}, i.e. stages whose MISS path actually
 * wrote a {@code stage.json} this run. On HIT, the archived {@code stage.json}
 * already carries {@code task_hashes} from the original archiving run and is
 * left untouched.
 */
@Slf4j
@CompileStatic
class StageTaskObserver implements TraceObserverV2 {

    private final ConcurrentHashMap<String, List<String>> stageTasks = new ConcurrentHashMap<>()

    @Override
    void onTaskComplete(TaskEvent event) {
        final task = event.handler?.task
        if( task == null ) return
        final processName = task.processor?.name
        if( processName == null ) return
        final sep = processName.indexOf(':')
        if( sep < 0 ) return

        final stageName = processName.substring(0, sep)
        final hashLog = task.getHashLog()
        if( !hashLog ) return

        stageTasks
            .computeIfAbsent(stageName, { Collections.synchronizedList(new ArrayList<String>()) })
            .add(hashLog)
    }

    @Override
    void onFlowComplete() {
        final cache = StageCache.instance
        final archive = cache.getArchive()
        if( archive == null ) return

        for( final entry : cache.getArchivedStages().entrySet() ) {
            final hashes = stageTasks.remove(entry.key)
            if( hashes != null && !hashes.isEmpty() )
                archive.patchTaskHashes(entry.key, entry.value, hashes)
        }
        stageTasks.clear()
    }

    /** Test hook: returns a snapshot of the current per-stage task hashes. */
    Map<String, List<String>> getStageTasksSnapshot() {
        final out = new LinkedHashMap<String, List<String>>()
        for( final e : stageTasks.entrySet() )
            out.put(e.key, new ArrayList<String>(e.value))
        return out
    }
}
