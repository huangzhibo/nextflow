/*
 * Test: nested named workflows (workflow A calls workflow B).
 * Verifies interceptor handles multi-level nesting correctly.
 */

process TRIM_READS {
    input:  tuple val(meta), path(fastq)
    output: tuple val(meta), path("${meta.id}.trimmed.fq")
    script: "echo 'trimmed '\$(cat ${fastq}) > ${meta.id}.trimmed.fq"
}

process ALIGN_READS {
    input:  tuple val(meta), path(fastq); val(ref)
    output: tuple val(meta), path("${meta.id}.bam")
    script: "echo 'aligned '\$(cat ${fastq})' to ${ref}' > ${meta.id}.bam"
}

process CALL_SNP {
    input:  tuple val(meta), path(bam)
    output: tuple val(meta), path("${meta.id}.vcf")
    script: "echo 'variants from '\$(cat ${bam}) > ${meta.id}.vcf"
}

// Inner named workflow
workflow TRIM_AND_ALIGN {
    take: input; reference
    main:
        TRIM_READS(input)
        ALIGN_READS(TRIM_READS.out, reference)
    emit:
        bam = ALIGN_READS.out
}

// Outer named workflow that calls inner
workflow FULL_ANALYSIS {
    take: input; reference
    main:
        TRIM_AND_ALIGN(input, reference)
        CALL_SNP(TRIM_AND_ALIGN.out.bam)
    emit:
        vcf = CALL_SNP.out
}

workflow {
    ch = Channel.of(
        [[id: 'S1'], file('data/sample1.fq')],
        [[id: 'S2'], file('data/sample2.fq')]
    )
    result = FULL_ANALYSIS(ch, params.reference)
    result.vcf.view { meta, f -> "${meta.id}: ${f.text.trim()}" }
}
