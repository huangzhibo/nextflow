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

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import nextflow.Global
import nextflow.Session
import nextflow.processor.TaskHandler
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import nextflow.trace.event.TaskEvent
import spock.lang.Specification
import spock.lang.TempDir

class StageTaskObserverTest extends Specification {

    @TempDir
    Path tempDir

    def cleanup() {
        Global.session = null
        StageCache.instance.reset()
    }

    private TaskEvent eventFor(String processName, String hashLog) {
        def processor = Mock(TaskProcessor) { getName() >> processName }
        def task = Mock(TaskRun) {
            getProcessor() >> processor
            getHashLog() >> hashLog
        }
        def handler = Mock(TaskHandler) { getTask() >> task }
        return new TaskEvent(handler, null)
    }

    private void writeArchive(Path archiveRoot, String stage, String dirName, Map extra = [:]) {
        def stageDir = archiveRoot.resolve(stage).resolve(dirName)
        Files.createDirectories(stageDir)
        def data = [schema_version: 'v1', stage: stage, emit: [:]] + extra
        Files.write(stageDir.resolve('stage.json'), JsonOutput.toJson(data).getBytes('UTF-8'))
    }

    // -- onTaskComplete --

    def 'collects hashLog grouped by stage prefix'() {
        given:
        def observer = new StageTaskObserver()
        when:
        observer.onTaskComplete(eventFor('ALIGN:BWA_MEM', '91/445199'))
        observer.onTaskComplete(eventFor('ALIGN:SORT', '13/f7a4af'))
        observer.onTaskComplete(eventFor('CALL:GATK', 'd2/598317'))
        then:
        observer.stageTasksSnapshot == [
            ALIGN: ['91/445199', '13/f7a4af'],
            CALL : ['d2/598317'],
        ]
    }

    def 'skips top-level processes without a stage prefix'() {
        given:
        def observer = new StageTaskObserver()
        when:
        observer.onTaskComplete(eventFor('TOP_LEVEL_PROC', '70/36cab9'))
        then:
        observer.stageTasksSnapshot.isEmpty()
    }

    def 'skips when hashLog is null or empty'() {
        given:
        def observer = new StageTaskObserver()
        when:
        observer.onTaskComplete(eventFor('ALIGN:BWA', null))
        observer.onTaskComplete(eventFor('ALIGN:BWA', ''))
        then:
        observer.stageTasksSnapshot.isEmpty()
    }

    // -- onFlowComplete --

    def 'onFlowComplete patches stage.json for stages recorded in archivedStages'() {
        given: 'stage cache enabled with one archived stage'
        def archiveRoot = tempDir.resolve('archive')
        Global.session = Mock(Session) {
            getConfig() >> [stage: [archiveRoot: archiveRoot.toString()]]
        }
        and: 'a pre-written archive directory'
        writeArchive(archiveRoot, 'ALIGN', 'abcdef1234567890')
        StageCache.instance.with {
            isEnabled()  // force init
            getArchivedStages().put('ALIGN', 'abcdef1234567890')
        }
        and:
        def observer = new StageTaskObserver()
        observer.onTaskComplete(eventFor('ALIGN:BWA_MEM', '91/445199'))
        observer.onTaskComplete(eventFor('ALIGN:SORT', '13/f7a4af'))

        when:
        observer.onFlowComplete()

        then:
        def patched = new JsonSlurper().parse(
            archiveRoot.resolve('ALIGN/abcdef1234567890/stage.json').toFile()) as Map
        patched.task_hashes == ['91/445199', '13/f7a4af']
    }

    def 'onFlowComplete does not patch stages not in archivedStages (cache HIT path)'() {
        given:
        def archiveRoot = tempDir.resolve('archive')
        Global.session = Mock(Session) {
            getConfig() >> [stage: [archiveRoot: archiveRoot.toString()]]
        }
        and: 'archive exists from an earlier run but the current run only HIT it'
        writeArchive(archiveRoot, 'ALIGN', 'abcdef1234567890',
            [task_hashes: ['original/aaa']])
        StageCache.instance.isEnabled() // force init, but no archivedStages put
        and:
        def observer = new StageTaskObserver()
        observer.onTaskComplete(eventFor('ALIGN:BWA_MEM', 'new/bbb'))

        when:
        observer.onFlowComplete()
        then: 'archive untouched — original hashes preserved'
        def data = new JsonSlurper().parse(
            archiveRoot.resolve('ALIGN/abcdef1234567890/stage.json').toFile()) as Map
        data.task_hashes == ['original/aaa']
    }

    def 'onFlowComplete is a no-op when stage cache is disabled'() {
        given: 'no stage config'
        Global.session = Mock(Session) { getConfig() >> [:] }
        StageCache.instance.isEnabled() // init as disabled
        and:
        def observer = new StageTaskObserver()
        observer.onTaskComplete(eventFor('ALIGN:BWA_MEM', '91/445199'))

        when:
        observer.onFlowComplete()
        then:
        noExceptionThrown()
    }
}
