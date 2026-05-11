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

import groovy.json.JsonSlurper
import nextflow.Channel
import nextflow.extension.CH
import nextflow.script.ChannelOut
import spock.lang.Specification
import spock.lang.TempDir

class StageArchiveTest extends Specification {

    @TempDir
    Path tempDir

    StageArchive archive

    def setup() {
        archive = new StageArchive(tempDir)
    }

    // -- archivePath --

    @spock.lang.Unroll
    def 'archivePath strips sha256: prefix and uses first 16 hex chars'() {
        expect:
        archive.archivePath(STAGE, DIGEST) == tempDir.resolve(EXPECTED)

        where:
        STAGE   | DIGEST                          | EXPECTED
        'ALIGN' | 'sha256:abcdef1234567890aaaa'   | 'ALIGN/abcdef1234567890'
        'STAGE' | 'sha256:1234567890abcdef1111'   | 'STAGE/1234567890abcdef'
        'STAGE' | 'sha256:abc'                    | 'STAGE/abc'                 // short digest: no truncation
    }

    // -- findArchive --

    def 'findArchive should return null when no archive exists'() {
        expect:
        archive.findArchive('ALIGN', 'sha256:abcdef1234567890aaaa') == null
    }

    def 'findArchive should return data when stage.json exists'() {
        given:
        def p = archive.archivePath('ALIGN', 'sha256:abcdef1234567890aaaa')
        Files.createDirectories(p)
        Files.write(p.resolve('stage.json'),
            '{"stage":"ALIGN","content_digest":"sha256:abcdef1234567890aaaa"}'.getBytes())

        when:
        def result = archive.findArchive('ALIGN', 'sha256:abcdef1234567890aaaa')

        then:
        result != null
        result.stage == 'ALIGN'
        result.content_digest == 'sha256:abcdef1234567890aaaa'
    }

    def 'findArchive should return null on malformed JSON'() {
        given:
        def p = archive.archivePath('ALIGN', 'sha256:abcdef1234567890aaaa')
        Files.createDirectories(p)
        Files.write(p.resolve('stage.json'), 'not json'.getBytes())

        expect:
        archive.findArchive('ALIGN', 'sha256:abcdef1234567890aaaa') == null
    }

    // -- serializeValue / rebuildValue round-trip --

    def 'serialize+rebuild scalar'() {
        given:
        def itemDir = tempDir.resolve('items/0')

        when:
        def serialized = StageArchive.metaClass.invokeStaticMethod(StageArchive, 'serializeValue', [42, itemDir] as Object[]) as List<Map>
        then:
        serialized[0].type == 'value'
        serialized[0].data == 42

        when:
        def rebuilt = StageArchive.rebuildValue(serialized, itemDir)
        then:
        rebuilt == 42
    }

    def 'serialize+rebuild map'() {
        given:
        def meta = [id: 'S1', type: 'WGS']
        def itemDir = tempDir.resolve('items/0')

        when:
        def serialized = StageArchive.metaClass.invokeStaticMethod(StageArchive, 'serializeValue', [meta, itemDir] as Object[]) as List<Map>
        then:
        serialized[0].type == 'value'
        serialized[0].data == meta

        when:
        def rebuilt = StageArchive.rebuildValue(serialized, itemDir)
        then:
        rebuilt == meta
    }

    def 'serialize+rebuild single Path with content + checksum'() {
        given:
        def src = tempDir.resolve('src/test.bam')
        Files.createDirectories(src.parent)
        Files.write(src, 'bam content'.getBytes())
        def itemDir = tempDir.resolve('items/0')

        when:
        def serialized = StageArchive.metaClass.invokeStaticMethod(StageArchive, 'serializeValue', [src, itemDir] as Object[]) as List<Map>
        then:
        serialized.size() == 1
        serialized[0].type == 'file'
        serialized[0].name == 'test.bam'
        serialized[0].checksum.startsWith('sha256:')
        serialized[0].size == 11
        Files.exists(itemDir.resolve('test.bam'))

        when:
        def rebuilt = StageArchive.rebuildValue(serialized, itemDir) as Path
        then:
        rebuilt.fileName.toString() == 'test.bam'
        Files.readString(rebuilt) == 'bam content'
    }

    def 'serialize+rebuild tuple [meta, file1, file2]'() {
        given:
        def srcDir = tempDir.resolve('src')
        Files.createDirectories(srcDir)
        Files.write(srcDir.resolve('S1.bam'), 'bam'.getBytes())
        Files.write(srcDir.resolve('S1.bai'), 'idx'.getBytes())
        def itemDir = tempDir.resolve('items/0')
        def tuple = [[id: 'S1'], srcDir.resolve('S1.bam'), srcDir.resolve('S1.bai')]

        when:
        def serialized = StageArchive.metaClass.invokeStaticMethod(StageArchive, 'serializeValue', [tuple, itemDir] as Object[]) as List<Map>
        then:
        serialized.size() == 3
        serialized[0].type == 'value'
        serialized[1].type == 'file'
        serialized[2].type == 'file'

        when:
        def rebuilt = StageArchive.rebuildValue(serialized, itemDir) as List
        then:
        rebuilt.size() == 3
        rebuilt[0] == [id: 'S1']
        (rebuilt[1] as Path).fileName.toString() == 'S1.bam'
        Files.readString(rebuilt[1] as Path) == 'bam'
    }

    // -- copyWithChecksum --

    def 'copyWithChecksum produces deterministic sha256'() {
        given:
        def src = tempDir.resolve('src.txt')
        Files.write(src, 'hello'.getBytes())

        when:
        def c1 = StageArchive.copyWithChecksum(src, tempDir.resolve('a.txt'))
        def c2 = StageArchive.copyWithChecksum(src, tempDir.resolve('b.txt'))

        then:
        c1.startsWith('sha256:')
        c1 == c2
        Files.readString(tempDir.resolve('a.txt')) == 'hello'
    }

    // -- archiveWithForward end-to-end --

    def 'archiveWithForward writes stage.json with v1 schema for value-channel-only output'() {
        given:
        def valueCh = CH.create(true)
        def output = new ChannelOut(out: valueCh as groovyx.gpars.dataflow.DataflowWriteChannel)
        valueCh.bind('hello')
        and:
        def placeholderOut = CH.create(true)
        def placeholders = ['out': placeholderOut] as Map<String, groovyx.gpars.dataflow.DataflowWriteChannel>

        when:
        archive.archiveWithForward('STAGE', 'sha256:abcdef1234567890zzzz', output, placeholders)
        then:
        def stageJson = archive.archivePath('STAGE', 'sha256:abcdef1234567890zzzz').resolve('stage.json')
        Files.exists(stageJson)
        def data = new JsonSlurper().parse(stageJson.toFile()) as Map
        data.schema_version == 'v1'
        data.stage == 'STAGE'
        data.content_digest == 'sha256:abcdef1234567890zzzz'
        !data.containsKey('integrity')                            // dropped from v1
        data.emit.out.type == 'value'
        data.emit.out.items.size() == 1
        data.emit.out.items[0][0].type == 'value'
        data.emit.out.items[0][0].data == 'hello'
        and: 'placeholder receives the forwarded value'
        placeholderOut.val == 'hello'
    }

    def 'archiveWithForward correctly routes mixed value + queue channels (per-iteration capture)'() {
        // regression: a loop-variable capture bug previously routed all
        // async onNext callbacks to the same `collected[name]` bucket
        given:
        def valueCh = CH.create(true)
        valueCh.bind(100)
        def queueCh = CH.create(false)
        and:
        def output = new ChannelOut(
            total : valueCh as groovyx.gpars.dataflow.DataflowWriteChannel,
            counts: queueCh as groovyx.gpars.dataflow.DataflowWriteChannel
        )
        def placeholders = [
            'total' : CH.create(true),
            'counts': CH.create(false)
        ] as Map<String, groovyx.gpars.dataflow.DataflowWriteChannel>

        when:
        archive.archiveWithForward('STAGE', 'sha256:abcdef1234567890mixd', output, placeholders)
        and: 'queue produces 2 emissions then completes'
        queueCh.bind([id: 'S1'])
        queueCh.bind([id: 'S2'])
        queueCh.bind(nextflow.Channel.STOP)
        // give async subscription a moment
        Thread.sleep(200)

        then:
        def stageJson = archive.archivePath('STAGE', 'sha256:abcdef1234567890mixd').resolve('stage.json')
        Files.exists(stageJson)
        def data = new JsonSlurper().parse(stageJson.toFile()) as Map
        and: 'total stays a value channel with exactly 1 item = 100'
        data.emit.total.type == 'value'
        data.emit.total.items.size() == 1
        data.emit.total.items[0][0].data == 100
        and: 'counts gets both queue emissions, NOT mis-routed elsewhere'
        data.emit.counts.type == 'queue'
        data.emit.counts.items.size() == 2
        data.emit.counts.items[0][0].data == [id: 'S1']
        data.emit.counts.items[1][0].data == [id: 'S2']
    }

    def 'archiveWithForward should no-op when archive already exists'() {
        given:
        def p = archive.archivePath('STAGE', 'sha256:abcdef1234567890zzzz')
        Files.createDirectories(p)
        Files.write(p.resolve('stage.json'), '{"original":"keep"}'.getBytes())
        and:
        def valueCh = CH.create(true)
        valueCh.bind('overwrite-attempt')
        def output = new ChannelOut(out: valueCh as groovyx.gpars.dataflow.DataflowWriteChannel)
        def placeholders = ['out': CH.create(true)] as Map<String, groovyx.gpars.dataflow.DataflowWriteChannel>

        when:
        archive.archiveWithForward('STAGE', 'sha256:abcdef1234567890zzzz', output, placeholders)
        then: 'existing stage.json is preserved (first writer wins)'
        def data = new JsonSlurper().parse(p.resolve('stage.json').toFile()) as Map
        data.original == 'keep'
    }
}
