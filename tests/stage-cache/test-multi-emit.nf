/*
 * Test: workflow with multiple emit channels.
 */

process FASTQC {
    input:  tuple val(meta), path(fastq)
    output: tuple val(meta), path("${meta.id}.qc.txt")
    script: "echo 'qc report for ${meta.id}' > ${meta.id}.qc.txt"
}

process TRIM {
    input:  tuple val(meta), path(fastq)
    output: tuple val(meta), path("${meta.id}.trimmed.fq")
    script: "echo 'trimmed '\$(cat ${fastq}) > ${meta.id}.trimmed.fq"
}

workflow QC_AND_TRIM {
    take: input
    main:
        FASTQC(input)
        TRIM(input)
    emit:
        report = FASTQC.out
        trimmed = TRIM.out
}

workflow {
    ch = Channel.of(
        [[id: 'S1'], file('data/sample1.fq')],
        [[id: 'S2'], file('data/sample2.fq')]
    )
    result = QC_AND_TRIM(ch)
    result.report.view  { meta, f -> "report: ${meta.id}" }
    result.trimmed.view { meta, f -> "trimmed: ${meta.id}" }
}
