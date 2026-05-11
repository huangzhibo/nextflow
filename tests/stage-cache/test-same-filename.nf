/*
 * Test: all samples produce files with the same name.
 * Verifies per-item subdirectory separation.
 */

process MAKE_REPORT {
    input:  tuple val(meta), path(fastq)
    output: tuple val(meta), path("report.txt")
    script: "echo 'report for ${meta.id}: '\$(cat ${fastq}) > report.txt"
}

workflow REPORT_STAGE {
    take: input
    main: MAKE_REPORT(input)
    emit: reports = MAKE_REPORT.out
}

workflow {
    ch = Channel.of(
        [[id: 'S1'], file('data/sample1.fq')],
        [[id: 'S2'], file('data/sample2.fq')]
    )
    result = REPORT_STAGE(ch)
    result.reports.view { meta, f -> "${meta.id}: ${f.text.trim()}" }
}
