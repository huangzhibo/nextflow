process FASTP {
    input:
    tuple val(meta), path(fastq)

    output:
    tuple val(meta), path("${meta.id}.trimmed.fq")

    script:
    """
    echo "trimmed: \$(cat ${fastq})" > ${meta.id}.trimmed.fq
    """
}

process BWA_MEM {
    input:
    tuple val(meta), path(fastq)
    val(reference)

    output:
    tuple val(meta), path("${meta.id}.bam"), path("${meta.id}.bam.bai")

    script:
    """
    echo "aligned \$(cat ${fastq}) to ${reference}" > ${meta.id}.bam
    echo "index" > ${meta.id}.bam.bai
    """
}

workflow PREPARE {
    take:
    input

    main:
    FASTP(input)

    emit:
    fastq = FASTP.out
}

workflow ALIGN {
    take:
    fastq
    reference

    main:
    BWA_MEM(fastq, reference)

    emit:
    bam = BWA_MEM.out
}

workflow {
    ch = Channel.of(
        [[id: 'sample1'], file('data/sample1.fq')],
        [[id: 'sample2'], file('data/sample2.fq')]
    )

    prepared = PREPARE(ch)
    aligned  = ALIGN(prepared.fastq, params.reference)
    aligned.bam.view { meta, bam, bai -> "${meta.id}: ${bam}" }
}
