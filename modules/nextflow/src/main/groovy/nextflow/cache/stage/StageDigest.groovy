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

import com.google.common.hash.Hashing
import groovy.transform.CompileStatic
import nextflow.util.CacheHelper
import nextflow.util.HashBuilder

/**
 * SHA-256 digest computation for stage cache lookup.
 *
 * The digest is computed in two phases because static inputs are known
 * immediately when intercept fires while channel inputs arrive asynchronously:
 *
 *   1. {@link #computeStatic} — hash workflow name + static (non-channel) inputs
 *   2. {@link #computeFinal}  — mix the static hash with collected channel contents
 *
 * Input names are sorted before hashing so the digest is independent of
 * map iteration order. Channel contents are hashed with {@link CacheHelper.HashMode#SHA256}
 * so {@code Path} emissions reflect file content; static inputs are hashed
 * with the default mode so {@code Path} statics reflect the path string.
 */
@CompileStatic
class StageDigest {

    static final String PREFIX = 'sha256:'

    /**
     * @param workflowName the named workflow's name
     * @param staticInputs map of (declared input name → static value) for non-channel inputs
     * @return hex-encoded SHA-256 (no prefix) over (workflowName, sorted static inputs)
     */
    static String computeStatic(String workflowName, Map<String, Object> staticInputs) {
        final hasher = Hashing.sha256().newHasher()
        hasher.putUnencodedChars(workflowName)
        for( final name : staticInputs.keySet().toSorted() ) {
            hasher.putUnencodedChars(name)
            new HashBuilder().withHasher(hasher).with(staticInputs.get(name))
        }
        return hasher.hash().toString()
    }

    /**
     * @param staticDigest hex string returned by {@link #computeStatic}
     * @param channelContents map of (declared input name → list of emissions) for channel inputs
     * @return "sha256:" prefixed hex digest mixing static + channel contents
     */
    static String computeFinal(String staticDigest, Map<String, List<Object>> channelContents) {
        final hasher = Hashing.sha256().newHasher()
        hasher.putUnencodedChars(staticDigest)
        for( final name : channelContents.keySet().toSorted() ) {
            hasher.putUnencodedChars(name)
            new HashBuilder()
                .withHasher(hasher)
                .withMode(CacheHelper.HashMode.SHA256)
                .with(channelContents.get(name))
        }
        return PREFIX + hasher.hash().toString()
    }
}
