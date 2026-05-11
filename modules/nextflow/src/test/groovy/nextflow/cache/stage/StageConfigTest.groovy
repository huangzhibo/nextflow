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

import nextflow.Session
import spock.lang.Specification
import spock.lang.Unroll

class StageConfigTest extends Specification {

    @Unroll
    def 'archiveRoot value drives enabled state'() {
        expect:
        new StageConfig([archiveRoot: VALUE]).enabled == EXPECTED

        where:
        VALUE                | EXPECTED
        null                 | false
        ''                   | false
        '   '                | true   // any non-empty string enables; user owns path validation
        '.nf-stage-archive'  | true
    }

    def 'should default cachedStagesFile and writable'() {
        when:
        def cfg = new StageConfig([archiveRoot: '.archive'])
        then:
        cfg.cachedStagesFile == 'cached-stages.tsv'
        cfg.writable
    }

    def 'should honor explicit cachedStagesFile and writable'() {
        when:
        def cfg = new StageConfig([
            archiveRoot     : '.archive',
            cachedStagesFile: 'my-stages.tsv',
            writable        : false
        ])
        then:
        cfg.cachedStagesFile == 'my-stages.tsv'
        !cfg.writable
    }

    def 'should read from session config when present'() {
        given:
        def session = Mock(Session) {
            getConfig() >> [stage: [archiveRoot: '.foo', writable: false]]
        }
        when:
        def cfg = StageConfig.getConfig(session)
        then:
        cfg.enabled
        cfg.archiveRoot == '.foo'
        !cfg.writable
    }

    @Unroll
    def 'getConfig disables when stage block or session absent'() {
        expect:
        !StageConfig.getConfig(SESSION).enabled

        where:
        DESC                | SESSION
        'null session'      | null
        'no stage block'    | Mock(Session) { getConfig() >> [:] }
    }
}
