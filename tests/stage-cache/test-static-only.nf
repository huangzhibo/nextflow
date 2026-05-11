// Pure-static stage: the named workflow's take is a non-channel value
// (params.summary_version is a raw String). Exercises StageCache.runStageStatic.
//
// On warm runs, the workflow body itself is never invoked: the digest is
// computed up front, archive is resolved, and emit placeholders are bound
// directly to archived values. The inner process therefore reports
// completed=0 on hit, and cache-hit invocations are observable both as a
// "Reusing archived stage" log line and a row in cached-stages.tsv.

process EMIT_VERSION_TXT {
    input:
    val(version)

    output:
    path 'version.txt'

    script:
    """
    echo "version=${version}" > version.txt
    """
}

workflow VERSION_REPORT {
    take:
    version

    main:
    EMIT_VERSION_TXT(version)

    emit:
    txt = EMIT_VERSION_TXT.out
}

workflow {
    VERSION_REPORT(params.summary_version)
}
