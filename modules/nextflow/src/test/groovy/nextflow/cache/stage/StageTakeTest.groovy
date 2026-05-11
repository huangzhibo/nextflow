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

import spock.lang.Specification
import spock.lang.TempDir

class StageTakeTest extends Specification {

    @TempDir
    Path tempDir

    StageArchive archive

    def setup() {
        archive = new StageArchive(tempDir.resolve('archive'))
    }

    // -- archiveDirName determinism --

    def 'archiveDirName is stable for identical takes'() {
        given:
        def a = new StageTake([x: [type: 'value', items: [[[type: 'value', data: 1]]]] as Map])
        def b = new StageTake([x: [type: 'value', items: [[[type: 'value', data: 1]]]] as Map])
        expect:
        a.archiveDirName() == b.archiveDirName()
        a.archiveDirName().length() == 16
    }

    def 'archiveDirName is independent of input-name iteration order'() {
        given:
        def a = new StageTake([
            alpha: [type: 'value', items: [[[type: 'value', data: 'A']]]] as Map,
            beta : [type: 'value', items: [[[type: 'value', data: 'B']]]] as Map,
        ])
        def b = new StageTake([
            beta : [type: 'value', items: [[[type: 'value', data: 'B']]]] as Map,
            alpha: [type: 'value', items: [[[type: 'value', data: 'A']]]] as Map,
        ])
        expect:
        a.archiveDirName() == b.archiveDirName()
    }

    def 'archiveDirName excludes file path (cross-cluster portability)'() {
        given: 'two takes with same checksum + name but different paths'
        def a = new StageTake([
            samples: [type: 'queue', items: [[
                [type: 'file', name: 'S1.fq', path: '/cluster-a/data/S1.fq', checksum: 'sha256:abc']
            ]]] as Map
        ])
        def b = new StageTake([
            samples: [type: 'queue', items: [[
                [type: 'file', name: 'S1.fq', path: '/cluster-b/runs/S1.fq', checksum: 'sha256:abc']
            ]]] as Map
        ])
        expect:
        a.archiveDirName() == b.archiveDirName()
    }

    def 'archiveDirName differs when checksum differs'() {
        given:
        def a = new StageTake([
            x: [type: 'value', items: [[[type: 'file', name: 'f', path: '/p/f', checksum: 'sha256:aaa']]]] as Map
        ])
        def b = new StageTake([
            x: [type: 'value', items: [[[type: 'file', name: 'f', path: '/p/f', checksum: 'sha256:bbb']]]] as Map
        ])
        expect:
        a.archiveDirName() != b.archiveDirName()
    }

    def 'toStorageMap preserves path fields'() {
        given:
        def take = new StageTake([
            x: [type: 'value', items: [[[type: 'file', name: 'f', path: '/abs/f', checksum: 'sha256:xyz']]]] as Map
        ])
        when:
        def stored = take.toStorageMap()
        then:
        ((stored.x as Map).items as List)[0][0].path == '/abs/f'
        ((stored.x as Map).items as List)[0][0].checksum == 'sha256:xyz'
    }

    // -- build --

    def 'build with static-only inputs produces value items'() {
        given:
        def known = new ConcurrentHashMap<Path, String>()
        def take = StageTake.build('S', [meta: 'M', count: 42], null, null, archive, known)
        when:
        def stored = take.toStorageMap()
        then:
        stored.count.type == 'value'
        stored.count.items == [[[type: 'value', data: 42]]]
        stored.meta.type == 'value'
    }

    def 'build with channel emissions of values'() {
        given:
        def known = new ConcurrentHashMap<Path, String>()
        def emissions = [samples: [[id: 'S1'], [id: 'S2']]] as Map<String, List<Object>>
        def isValue = [samples: false] as Map<String, Boolean>
        def take = StageTake.build('S', [samples: 'dummy'], emissions, isValue, archive, known)
        when:
        def stored = take.toStorageMap()
        then:
        stored.samples.type == 'queue'
        (stored.samples.items as List).size() == 2
        ((stored.samples.items as List)[0][0] as Map).data == [id: 'S1']
    }

    def 'build with Path emissions computes file checksum'() {
        given:
        def known = new ConcurrentHashMap<Path, String>()
        def file = tempDir.resolve('S1.fq')
        Files.write(file, 'hello'.getBytes())
        def emissions = [reads: [file]] as Map<String, List<Object>>
        def isValue = [reads: false] as Map<String, Boolean>
        def take = StageTake.build('S', [reads: 'dummy'], emissions, isValue, archive, known)
        when:
        def stored = take.toStorageMap()
        def fileElem = (stored.reads.items as List)[0][0] as Map
        then:
        fileElem.type == 'file'
        fileElem.name == 'S1.fq'
        fileElem.path == file.toAbsolutePath().normalize().toString()
        (fileElem.checksum as String).startsWith('sha256:')
    }

    // -- computeFileChecksum 3-step fallback --

    def 'computeFileChecksum step 1 reads file directly'() {
        given:
        def known = new ConcurrentHashMap<Path, String>()
        def file = tempDir.resolve('a.txt')
        Files.write(file, 'data'.getBytes())
        when:
        def c = StageTake.computeFileChecksum('S', file, archive, known)
        then:
        c.startsWith('sha256:')
        // step 1 should not register into knownChecksums
        known.isEmpty()
    }

    def 'computeFileChecksum step 2 returns cached value'() {
        given:
        def known = new ConcurrentHashMap<Path, String>()
        def missing = tempDir.resolve('ghost.txt').toAbsolutePath().normalize()
        known.put(missing, 'sha256:cached')
        expect:
        StageTake.computeFileChecksum('S', missing, archive, known) == 'sha256:cached'
    }

    def 'computeFileChecksum step 3 scans archives and returns'() {
        given: 'an archive recording this stage previously saw this path'
        def known = new ConcurrentHashMap<Path, String>()
        def missing = tempDir.resolve('ghost.fq').toAbsolutePath().normalize()
        // hand-craft a prior archive
        def stageDir = archive.archiveRoot.resolve('STAGE').resolve('aaaa111122223333')
        Files.createDirectories(stageDir)
        def take = new StageTake([
            reads: [type: 'queue', items: [[
                [type: 'file', name: 'ghost.fq', path: missing.toString(), checksum: 'sha256:from-archive']
            ]]] as Map
        ])
        Files.write(stageDir.resolve('stage.json'), groovy.json.JsonOutput.toJson([
            schema_version: 'v1', stage: 'STAGE', take: take.toStorageMap(), emit: [:]
        ]).getBytes('UTF-8'))
        expect:
        StageTake.computeFileChecksum('STAGE', missing, archive, known) == 'sha256:from-archive'
        known.get(missing) == 'sha256:from-archive'
    }

    def 'computeFileChecksum throws when scan finds nothing'() {
        given:
        def known = new ConcurrentHashMap<Path, String>()
        def missing = tempDir.resolve('nowhere.txt')
        when:
        StageTake.computeFileChecksum('S', missing, archive, known)
        then:
        def e = thrown(IllegalStateException)
        e.message.contains('nowhere.txt')
        e.message.contains('stage S')
    }
}
