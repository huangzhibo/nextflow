/*
 * Test: emit with both queue channel and value channel (static value).
 */

process COUNT_READS {
    input:  tuple val(meta), path(fastq)
    output: tuple val(meta), path("${meta.id}.count")
    script: "wc -l < ${fastq} > ${meta.id}.count"
}

workflow COUNT_STAGE {
    take: input
    main:
        COUNT_READS(input)
        total = params.expected_total
    emit:
        counts = COUNT_READS.out   // queue channel
        total                       // value channel (static)
}

workflow {
    ch = Channel.of(
        [[id: 'S1'], file('data/sample1.fq')],
        [[id: 'S2'], file('data/sample2.fq')]
    )
    result = COUNT_STAGE(ch)
    result.counts.view { meta, f -> "count: ${meta.id}" }
    result.total.view { v -> "total: ${v}" }
}
