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
import java.util.concurrent.ConcurrentHashMap

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import nextflow.Channel
import nextflow.extension.CH
import nextflow.script.ChannelOut
import spock.lang.Specification
import spock.lang.TempDir
import spock.util.concurrent.PollingConditions

class StageArchiveTest extends Specification {

    @TempDir
    Path tempDir

    StageArchive archive

    def setup() {
        archive = new StageArchive(tempDir)
    }

    private static StageTake takeFor(String label) {
        // hand-craft a take whose archiveDirName depends on `label` so different
        // tests don't collide on archive directory
        return new StageTake([
            'k': [type: 'value', items: [[[type: 'value', data: label]]]] as Map
        ])
    }

    // -- archivePath --

    def 'archivePath joins root + stage + dir name verbatim'() {
        expect:
        archive.archivePath('ALIGN', 'abcdef1234567890') == tempDir.resolve('ALIGN/abcdef1234567890')
    }

    // -- findArchive --

    def 'findArchive returns null when no archive exists'() {
        expect:
        archive.findArchive('ALIGN', 'abcdef1234567890') == null
    }

    def 'findArchive returns parsed stage.json'() {
        given:
        def p = archive.archivePath('ALIGN', 'abcdef1234567890')
        Files.createDirectories(p)
        Files.write(p.resolve('stage.json'),
            '{"stage":"ALIGN","schema_version":"v1"}'.getBytes())
        when:
        def result = archive.findArchive('ALIGN', 'abcdef1234567890')
        then:
        result != null
        result.stage == 'ALIGN'
        result.schema_version == 'v1'
    }

    def 'findArchive returns null on malformed JSON'() {
        given:
        def p = archive.archivePath('ALIGN', 'abcdef1234567890')
        Files.createDirectories(p)
        Files.write(p.resolve('stage.json'), 'not json'.getBytes())
        expect:
        archive.findArchive('ALIGN', 'abcdef1234567890') == null
    }

    // -- rebuildValue round-trip --

    def 'rebuild scalar value'() {
        given:
        def itemDir = tempDir.resolve('x')
        expect:
        StageArchive.rebuildValue([[type: 'value', data: 42] as Map], itemDir) == 42
    }

    def 'rebuild map value'() {
        given:
        def itemDir = tempDir.resolve('x')
        def meta = [id: 'S1', type: 'WGS']
        expect:
        StageArchive.rebuildValue([[type: 'value', data: meta] as Map], itemDir) == meta
    }

    def 'rebuild single file element resolves under itemDir'() {
        given:
        def itemDir = tempDir.resolve('items/0')
        Files.createDirectories(itemDir)
        when:
        def rebuilt = StageArchive.rebuildValue(
            [[type: 'file', name: 'test.bam'] as Map], itemDir) as Path
        then:
        rebuilt.fileName.toString() == 'test.bam'
        rebuilt.parent == itemDir
    }

    def 'rebuild tuple [meta, file]'() {
        given:
        def itemDir = tempDir.resolve('items/0')
        Files.createDirectories(itemDir)
        when:
        def rebuilt = StageArchive.rebuildValue([
            [type: 'value', data: [id: 'S1']] as Map,
            [type: 'file',  name: 'S1.bam'] as Map,
        ], itemDir) as List
        then:
        rebuilt.size() == 2
        rebuilt[0] == [id: 'S1']
        (rebuilt[1] as Path).fileName.toString() == 'S1.bam'
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

    def 'archiveWithForward writes v1 stage.json with take + emit; no content_digest'() {
        given:
        def take = takeFor('value-only')
        def valueCh = CH.create(true)
        def output = new ChannelOut(out: valueCh as groovyx.gpars.dataflow.DataflowWriteChannel)
        valueCh.bind('hello')
        and:
        def placeholderOut = CH.create(true)
        def placeholders = ['out': placeholderOut] as Map<String, groovyx.gpars.dataflow.DataflowWriteChannel>
        def conditions = new PollingConditions(timeout: 2)

        when:
        archive.archiveWithForward('STAGE', take, output, placeholders)
        then:
        def stageDir = archive.archivePath('STAGE', take.archiveDirName())
        def stageJson = stageDir.resolve('stage.json')
        conditions.eventually { assert Files.exists(stageJson) }
        def data = new JsonSlurper().parse(stageJson.toFile()) as Map
        data.schema_version == 'v1'
        data.stage == 'STAGE'
        !data.containsKey('content_digest')
        !data.containsKey('integrity')
        and: 'take is stored'
        data.take != null
        data.take.k.items[0][0].data == 'value-only'
        and: 'emit is stored'
        data.emit.out.type == 'value'
        data.emit.out.items.size() == 1
        data.emit.out.items[0][0].data == 'hello'
        and: 'placeholder receives the forwarded value'
        placeholderOut.val == 'hello'
    }

    def 'archiveWithForward correctly routes mixed value + queue channels (per-iteration capture)'() {
        // regression: a loop-variable capture bug previously routed all
        // async onNext callbacks to the same `collected[name]` bucket
        given:
        def take = takeFor('mixed')
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
        def conditions = new PollingConditions(timeout: 2)

        when:
        archive.archiveWithForward('STAGE', take, output, placeholders)
        and: 'queue produces 2 emissions then completes'
        queueCh.bind([id: 'S1'])
        queueCh.bind([id: 'S2'])
        queueCh.bind(Channel.STOP)

        then:
        def stageJson = archive.archivePath('STAGE', take.archiveDirName()).resolve('stage.json')
        conditions.eventually { assert Files.exists(stageJson) }
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

    def 'archiveWithForward no-ops when archive already exists (first writer wins)'() {
        given:
        def take = takeFor('exists')
        def p = archive.archivePath('STAGE', take.archiveDirName())
        Files.createDirectories(p)
        Files.write(p.resolve('stage.json'), '{"original":"keep"}'.getBytes())
        and:
        def valueCh = CH.create(true)
        valueCh.bind('overwrite-attempt')
        def output = new ChannelOut(out: valueCh as groovyx.gpars.dataflow.DataflowWriteChannel)
        def placeholders = ['out': CH.create(true)] as Map<String, groovyx.gpars.dataflow.DataflowWriteChannel>

        when:
        archive.archiveWithForward('STAGE', take, output, placeholders)
        then: 'existing stage.json is preserved'
        def data = new JsonSlurper().parse(p.resolve('stage.json').toFile()) as Map
        data.original == 'keep'
    }

    // -- scanThisStageArchives --

    def 'scanThisStageArchives populates knownChecksums from take.file elements'() {
        given:
        def known = new ConcurrentHashMap<Path, String>()
        def stageDir = tempDir.resolve('STAGE').resolve('aaaa111122223333')
        Files.createDirectories(stageDir)
        def take = new StageTake([
            reads: [type: 'queue', items: [[
                [type: 'file', name: 'a.fq', path: '/data/a.fq', checksum: 'sha256:aaa']
            ], [
                [type: 'file', name: 'b.fq', path: '/data/b.fq', checksum: 'sha256:bbb']
            ]]] as Map
        ])
        Files.write(stageDir.resolve('stage.json'),
            JsonOutput.toJson([schema_version: 'v1', stage: 'STAGE',
                               take: take.toStorageMap(), emit: [:]]).getBytes('UTF-8'))

        when:
        archive.scanThisStageArchives('STAGE', known)
        then:
        known.size() == 2
        known.get(java.nio.file.Paths.get('/data/a.fq').toAbsolutePath().normalize()) == 'sha256:aaa'
        known.get(java.nio.file.Paths.get('/data/b.fq').toAbsolutePath().normalize()) == 'sha256:bbb'
    }

    def 'scanThisStageArchives skips corrupt stage.json without failing the scan'() {
        given:
        def known = new ConcurrentHashMap<Path, String>()
        // good archive
        def goodDir = tempDir.resolve('STAGE').resolve('aaaa111122223333')
        Files.createDirectories(goodDir)
        def take = new StageTake([
            reads: [type: 'queue', items: [[
                [type: 'file', name: 'good.fq', path: '/data/good.fq', checksum: 'sha256:good']
            ]]] as Map
        ])
        Files.write(goodDir.resolve('stage.json'),
            JsonOutput.toJson([take: take.toStorageMap()]).getBytes('UTF-8'))
        // bad archive
        def badDir = tempDir.resolve('STAGE').resolve('bbbb222233334444')
        Files.createDirectories(badDir)
        Files.write(badDir.resolve('stage.json'), 'not json'.getBytes('UTF-8'))

        when:
        archive.scanThisStageArchives('STAGE', known)
        then: 'good archive recorded, scan did not crash'
        known.size() == 1
        known.get(java.nio.file.Paths.get('/data/good.fq').toAbsolutePath().normalize()) == 'sha256:good'
    }

    def 'scanThisStageArchives is a no-op when stage directory does not exist'() {
        given:
        def known = new ConcurrentHashMap<Path, String>()
        when:
        archive.scanThisStageArchives('NONE', known)
        then:
        known.isEmpty()
        noExceptionThrown()
    }

    def 'scanThisStageArchives respects putIfAbsent: pre-existing entry wins'() {
        given:
        def known = new ConcurrentHashMap<Path, String>()
        def existing = java.nio.file.Paths.get('/data/x.fq').toAbsolutePath().normalize()
        known.put(existing, 'sha256:cached')
        and:
        def stageDir = tempDir.resolve('STAGE').resolve('aaaa111122223333')
        Files.createDirectories(stageDir)
        def take = new StageTake([
            reads: [type: 'queue', items: [[
                [type: 'file', name: 'x.fq', path: '/data/x.fq', checksum: 'sha256:from-archive']
            ]]] as Map
        ])
        Files.write(stageDir.resolve('stage.json'),
            JsonOutput.toJson([take: take.toStorageMap()]).getBytes('UTF-8'))

        when:
        archive.scanThisStageArchives('STAGE', known)
        then:
        known.get(existing) == 'sha256:cached'
    }
}
