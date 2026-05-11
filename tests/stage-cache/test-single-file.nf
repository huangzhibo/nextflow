/*
 * Test: emit single Path (not tuple).
 */

process MERGE_FILES {
    input:  path(files)
    output: path("merged.txt")
    script: "cat ${files} > merged.txt"
}

process GENERATE {
    input:  tuple val(meta), path(fastq)
    output: path("${meta.id}.out")
    script: "cp ${fastq} ${meta.id}.out"
}

workflow GENERATE_STAGE {
    take: input
    main: GENERATE(input)
    emit: files = GENERATE.out   // each item is a single Path, not a tuple
}

workflow {
    ch = Channel.of(
        [[id: 'S1'], file('data/sample1.fq')],
        [[id: 'S2'], file('data/sample2.fq')]
    )
    result = GENERATE_STAGE(ch)
    result.files.view { f -> "file: ${f}" }
}
