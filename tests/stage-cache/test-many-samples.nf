/*
 * Test: 10 samples to verify performance and correctness at scale.
 */

process PROCESS_SAMPLE {
    input:  tuple val(meta), val(data)
    output: tuple val(meta), path("${meta.id}.result")
    script: "echo '${meta.id}: processed ${data}' > ${meta.id}.result"
}

process SUMMARIZE {
    input:  tuple val(meta), path(result); val(version)
    output: tuple val(meta), path("${meta.id}.summary")
    script: "echo 'summary(${version}): '\$(cat ${result}) > ${meta.id}.summary"
}

workflow PROCESS_STAGE {
    take: input
    main: PROCESS_SAMPLE(input)
    emit: result = PROCESS_SAMPLE.out
}

workflow SUMMARY_STAGE {
    take: input; version
    main: SUMMARIZE(input, version)
    emit: summary = SUMMARIZE.out
}

workflow {
    ch = Channel.of(
        [[id: 'S01'], 'data-01'],
        [[id: 'S02'], 'data-02'],
        [[id: 'S03'], 'data-03'],
        [[id: 'S04'], 'data-04'],
        [[id: 'S05'], 'data-05'],
        [[id: 'S06'], 'data-06'],
        [[id: 'S07'], 'data-07'],
        [[id: 'S08'], 'data-08'],
        [[id: 'S09'], 'data-09'],
        [[id: 'S10'], 'data-10']
    )
    processed = PROCESS_STAGE(ch)
    summarized = SUMMARY_STAGE(processed.result, params.summary_version)
    summarized.summary.view { meta, f -> "${meta.id}: ${f.text.trim()}" }
}
