/*
 * Test: regular process output passed to named workflow.
 * Verifies clone approach handles process outputs outside named workflows.
 */

process BUILD_INDEX {
    output: path("index.txt")
    script: "echo 'GRCh38 index data' > index.txt"
}

process ALIGN_READS {
    input:  tuple val(meta), path(fastq); path(index)
    output: tuple val(meta), path("${meta.id}.bam")
    script: "echo 'aligned '\$(cat ${fastq})' with '\$(cat ${index}) > ${meta.id}.bam"
}

workflow ALIGN_STAGE {
    take: fastq; index
    main: ALIGN_READS(fastq, index)
    emit: bam = ALIGN_READS.out
}

workflow {
    // index comes from a regular process (not a named workflow)
    index = BUILD_INDEX()

    ch = Channel.of(
        [[id: 'S1'], file('data/sample1.fq')],
        [[id: 'S2'], file('data/sample2.fq')]
    )
    result = ALIGN_STAGE(ch, index)
    result.bam.view { meta, f -> "${meta.id}: ${f}" }
}
