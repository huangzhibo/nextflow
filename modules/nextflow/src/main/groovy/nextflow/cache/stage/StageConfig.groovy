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

import groovy.transform.CompileStatic
import nextflow.Session
import nextflow.config.spec.ConfigOption
import nextflow.config.spec.ConfigScope
import nextflow.config.spec.ScopeName
import nextflow.script.dsl.Description

@ScopeName("stage")
@Description("""
    The `stage` scope enables named-workflow-level archiving and reuse. When configured, each named workflow's emit outputs are archived on first run; on subsequent runs with the same input content the archived outputs are replayed and the workflow body is skipped.
""")
@CompileStatic
class StageConfig implements ConfigScope {

    static final String DEFAULT_CACHED_STAGES_FILE = 'cached-stages.tsv'

    @ConfigOption
    @Description("""
        Root directory for stage archives. Stage cache is enabled when this option is set; leave unset to disable.
    """)
    final String archiveRoot

    @ConfigOption
    @Description("""
        File path for the per-run cached stages report (default: `cached-stages.tsv`).
    """)
    final String cachedStagesFile

    @ConfigOption
    @Description("""
        Whether this run may write new archives (default: `true`). Set `false` for read-only reuse of a shared archive.
    """)
    final boolean writable

    /* required by extension point -- do not remove */
    StageConfig() {}

    StageConfig(Map opts) {
        archiveRoot = opts.archiveRoot as String
        cachedStagesFile = (opts.cachedStagesFile ?: DEFAULT_CACHED_STAGES_FILE) as String
        writable = opts.writable == null ? true : opts.writable as boolean
    }

    boolean isEnabled() { archiveRoot != null && !archiveRoot.isEmpty() }

    static StageConfig getConfig(Session session) {
        new StageConfig((session?.config?.stage ?: Collections.emptyMap()) as Map)
    }
}
