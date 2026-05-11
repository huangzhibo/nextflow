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

import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable
import nextflow.Global
import nextflow.Session
import nextflow.extension.CH
import spock.lang.Specification

class StageCacheTest extends Specification {

    def setup() {
        StageCache.instance.reset()
    }

    def cleanup() {
        Global.session = null
        StageCache.instance.reset()
    }

    def 'isEnabled is false when no stage config'() {
        given:
        Global.session = Mock(Session) {
            getConfig() >> [:]
        }
        expect:
        !StageCache.instance.isEnabled()
    }

    def 'isEnabled is true when archiveRoot configured'() {
        given:
        Global.session = Mock(Session) {
            getConfig() >> [stage: [archiveRoot: '.archive-test']]
        }
        expect:
        StageCache.instance.isEnabled()
    }

    def 'runStage falls through to proceed when disabled'() {
        given:
        Global.session = Mock(Session) {
            getConfig() >> [:]
        }
        def expected = 'untouched'
        when:
        def result = StageCache.instance.runStage(null, [:], [], { expected })
        then:
        result == expected
    }

    // -- ClonedChannel DTO --

    def 'ClonedChannel records the four fields'() {
        given:
        def orig = new DataflowQueue()
        def clone = new DataflowVariable()
        when:
        def cc = new StageCache.ClonedChannel('fastq', orig, clone, true)
        then:
        cc.inputName == 'fastq'
        cc.original.is(orig)
        cc.clone.is(clone)
        cc.isValue
    }
}
