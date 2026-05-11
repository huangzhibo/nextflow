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

import spock.lang.Specification
import spock.lang.TempDir

class StageDigestTest extends Specification {

    @TempDir Path tmp

    def 'should produce different digests for different workflow names'() {
        when:
        def a = StageDigest.computeStatic('STAGE_A', [:])
        def b = StageDigest.computeStatic('STAGE_B', [:])
        then:
        a != b
        a.length() == 64
        b.length() == 64
    }

    def 'should be insensitive to static input map order'() {
        when:
        def d1 = StageDigest.computeStatic('S', [a: 1, b: 2, c: 3])
        def d2 = StageDigest.computeStatic('S', [c: 3, a: 1, b: 2])
        then:
        d1 == d2
    }

    def 'should change digest when static input value changes'() {
        when:
        def d1 = StageDigest.computeStatic('S', [param: 'foo'])
        def d2 = StageDigest.computeStatic('S', [param: 'bar'])
        then:
        d1 != d2
    }

    def 'should distinguish swapped values across input names'() {
        when:
        def d1 = StageDigest.computeStatic('S', [a: 'X', b: 'Y'])
        def d2 = StageDigest.computeStatic('S', [a: 'Y', b: 'X'])
        then:
        d1 != d2
    }

    def 'final digest should carry sha256 prefix'() {
        when:
        def s = StageDigest.computeStatic('S', [:])
        def f = StageDigest.computeFinal(s, [:])
        then:
        f.startsWith('sha256:')
        f.length() == 7 + 64
    }

    def 'final digest should change when channel contents change'() {
        given:
        def s = StageDigest.computeStatic('S', [:])
        when:
        def d1 = StageDigest.computeFinal(s, [ch: [1, 2, 3]])
        def d2 = StageDigest.computeFinal(s, [ch: [1, 2, 4]])
        then:
        d1 != d2
    }

    def 'final digest should be insensitive to channel map order'() {
        given:
        def s = StageDigest.computeStatic('S', [:])
        when:
        def d1 = StageDigest.computeFinal(s, [a: [1], b: [2]])
        def d2 = StageDigest.computeFinal(s, [b: [2], a: [1]])
        then:
        d1 == d2
    }

    def 'final digest should reflect file content for channel paths'() {
        given:
        def s = StageDigest.computeStatic('S', [:])
        def f1 = Files.write(tmp.resolve('f1.txt'), 'alpha'.bytes)
        def f2 = Files.write(tmp.resolve('f2.txt'), 'beta'.bytes)
        when:
        def d1 = StageDigest.computeFinal(s, [ch: [f1]])
        def d2 = StageDigest.computeFinal(s, [ch: [f2]])
        then: 'different content → different digest'
        d1 != d2
    }

    def 'should produce identical digests for identical inputs'() {
        given:
        def s = StageDigest.computeStatic('STAGE', [version: '1.0', count: 42])
        when:
        def d1 = StageDigest.computeFinal(s, [samples: ['s1', 's2']])
        def d2 = StageDigest.computeFinal(s, [samples: ['s1', 's2']])
        then:
        d1 == d2
    }
}
