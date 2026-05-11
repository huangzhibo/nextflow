/*
 * Test: fan-in stage that depends on multiple upstream stages.
 *
 *   PREPARE ──┐
 *             ├──→ MERGE_STAGE
 *   QC ───────┘
 *
 * Verifies that fan-in stage correctly tracks multiple upstream digests.
 */

process TRIM {
    input:  tuple val(meta), path(fastq)
    output: tuple val(meta), path("${meta.id}.trimmed.fq")
    script: "echo 'trimmed '\$(cat ${fastq}) > ${meta.id}.trimmed.fq"
}

process QC_CHECK {
    input:  tuple val(meta), path(fastq)
    output: tuple val(meta), path("${meta.id}.qc.txt")
    script: "echo 'qc passed for ${meta.id}' > ${meta.id}.qc.txt"
}

process COMBINE {
    input:  tuple val(meta), path(trimmed), path(qc)
    output: tuple val(meta), path("${meta.id}.combined.txt")
    script: "cat ${trimmed} ${qc} > ${meta.id}.combined.txt"
}

workflow PREPARE {
    take: input
    main: TRIM(input)
    emit: trimmed = TRIM.out
}

workflow QC {
    take: input
    main: QC_CHECK(input)
    emit: report = QC_CHECK.out
}

workflow MERGE_STAGE {
    take: trimmed; qc
    main:
        // join by meta (index 0): [meta, trimmed_fq] + [meta, qc_file] → [meta, trimmed_fq, qc_file]
        joined = trimmed.join(qc, by: 0)
        COMBINE(joined)
    emit: combined = COMBINE.out
}

workflow {
    ch = Channel.of(
        [[id: 'S1'], file('data/sample1.fq')],
        [[id: 'S2'], file('data/sample2.fq')]
    )
    prepared = PREPARE(ch)
    qc_result = QC(ch)
    merged = MERGE_STAGE(prepared.trimmed, qc_result.report)
    merged.combined.view { meta, f -> "${meta.id}: ${f.text.trim()}" }
}
