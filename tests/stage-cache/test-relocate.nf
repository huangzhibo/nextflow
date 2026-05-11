// Reused by two scenarios that share the same workflow shape but differ
// in how the source files are manipulated between runs (handled in run-tests.sh):
//
//   * source-deleted : files at $params.input_dir vanish between runs
//   * cross-cluster  : files relocated to a different absolute path between runs
//
// In both cases the take digest must remain stable because it excludes
// `path` from the hash — only file name + checksum participate.
params.input_dir = 'data'

process PREP {
    input:
    tuple val(meta), path(fastq)

    output:
    tuple val(meta), path("${meta.id}.prepped")

    script:
    """
    echo "prepped: \$(cat ${fastq})" > ${meta.id}.prepped
    """
}

process REPORT {
    input:
    tuple val(meta), path(prepped)

    output:
    tuple val(meta), path("${meta.id}.report")

    script:
    """
    echo "reported: \$(cat ${prepped})" > ${meta.id}.report
    """
}

workflow PREPARE {
    take:
    input

    main:
    PREP(input)

    emit:
    prepped = PREP.out
}

workflow REPORTING {
    take:
    prepped

    main:
    REPORT(prepped)

    emit:
    report = REPORT.out
}

workflow {
    ch = Channel.of(
        [[id: 'sample1'], file("${params.input_dir}/sample1.fq")],
        [[id: 'sample2'], file("${params.input_dir}/sample2.fq")]
    )

    prepared = PREPARE(ch)
    REPORTING(prepared.prepped)
}
