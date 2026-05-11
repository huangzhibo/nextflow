/*
 * Test: three-level chained stages A → B → C.
 * Verifies digest propagation through the chain.
 *
 * Scenario A: change param_c → A/B cached, C reruns
 * Scenario B: change param_b → A cached, B/C rerun
 */

process STEP_A {
    input:  tuple val(meta), path(input)
    output: tuple val(meta), path("${meta.id}.a.out")
    script: "echo 'A: '\$(cat ${input}) > ${meta.id}.a.out"
}

process STEP_B {
    input:  tuple val(meta), path(input); val(param_b)
    output: tuple val(meta), path("${meta.id}.b.out")
    script: "echo 'B(${param_b}): '\$(cat ${input}) > ${meta.id}.b.out"
}

process STEP_C {
    input:  tuple val(meta), path(input); val(param_c)
    output: tuple val(meta), path("${meta.id}.c.out")
    script: "echo 'C(${param_c}): '\$(cat ${input}) > ${meta.id}.c.out"
}

workflow STAGE_A {
    take: input
    main: STEP_A(input)
    emit: out = STEP_A.out
}

workflow STAGE_B {
    take: input; param_b
    main: STEP_B(input, param_b)
    emit: out = STEP_B.out
}

workflow STAGE_C {
    take: input; param_c
    main: STEP_C(input, param_c)
    emit: out = STEP_C.out
}

workflow {
    ch = Channel.of(
        [[id: 'S1'], file('data/sample1.fq')],
        [[id: 'S2'], file('data/sample2.fq')]
    )
    a = STAGE_A(ch)
    b = STAGE_B(a.out, params.param_b)
    c = STAGE_C(b.out, params.param_c)
    c.out.view { meta, f -> "${meta.id}: ${f.text.trim()}" }
}
