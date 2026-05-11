// Pure-static stage: the named workflow's take is a non-channel value
// (params.summary_version is a raw String, not wrapped by ChannelOut.spread).
// Exercises the runStage(...) branch where `clonedChannels.isEmpty()` is true.
//
// Regression: this branch synchronously called decide() on the main thread,
// which deadlocked when archiveWithForward used blocking getVal() on a
// value-channel emit before the workflow body's process had fired.

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
