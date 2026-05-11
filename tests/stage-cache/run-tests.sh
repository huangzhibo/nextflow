#!/bin/bash
#
# Stage cache integration test suite.
#
# Each test runs in its own mktemp -d directory; passing tests are wiped,
# failing tests preserve their working directory for inspection (path
# printed to stdout).
#
# Usage:
#   ./run-tests.sh             # run all tests
#   ./run-tests.sh basic       # run a single test by name
#
# Override the Nextflow binary:
#   NXF=nextflow ./run-tests.sh        # use system-installed nextflow
#   NXF=../../launch.sh ./run-tests.sh # use fork dev build (default)

set -uo pipefail

# Force Nextflow's agent-mode log format so each run ends with a
# `[SUCCESS|FAILED] completed=N failed=N cached=N` line that the
# `assert_completed` grep can parse. Without this, Nextflow falls back to
# the ANSI progress bar (no `completed=` field) and every assertion sees
# an empty value.
export NXF_AGENT_MODE=1

TESTS_DIR=$(cd "$(dirname "$0")" && pwd)
NXF=${NXF:-${TESTS_DIR}/../../launch.sh}
DATA_DIR="${TESTS_DIR}/data"

PASS=0
FAIL=0
TEST_FAILED=0
TEST_DIR=""
LAST_OUTPUT=""

# ----------------------------------------------------------------------
# Test framework
# ----------------------------------------------------------------------

# Per-test sandbox: mktemp dir + symlink to shared data + cd into it.
setup_test_dir() {
    TEST_DIR=$(mktemp -d -t stage-cache.XXXXXX)
    LAST_OUTPUT="${TEST_DIR}/.nf-output"
    cd "$TEST_DIR"
    ln -s "$DATA_DIR" data
}

# Stage-cache-enabled config (default for most tests).
write_config() {
    cat > stage.config <<'EOF'
stage {
    archiveRoot      = '.nf-stage-archive'
    cachedStagesFile = 'cached-stages.tsv'
}
workDir = 'work'
params {
    reference       = 'GRCh38'
    dbsnp           = 'dbsnp154'
    param_b         = 'v1'
    param_c         = 'v1'
    expected_total  = 100
    summary_version = 'v1'
}
EOF
}

# Baseline config WITHOUT stage scope (verifies native-behavior fall-through).
write_noplugin_config() {
    cat > noplugin.config <<'EOF'
workDir = 'work'
params {
    reference       = 'GRCh38'
    dbsnp           = 'dbsnp154'
    param_b         = 'v1'
    param_c         = 'v1'
    expected_total  = 100
    summary_version = 'v1'
}
EOF
}

# Drop session + work between two runs; keep the archive.
between_runs() {
    rm -rf .nextflow .nextflow.log* work
}

# -- assertions --

assert_completed() {
    local expected=$1
    local actual
    actual=$(grep -oE 'completed=[0-9]+' "$LAST_OUTPUT" | tail -1 | cut -d= -f2)
    if [[ "$actual" != "$expected" ]]; then
        echo "  ASSERT FAILED: expected completed=${expected}, got completed=${actual}"
        TEST_FAILED=1
    fi
}

assert_cached_stages() {
    local expected=$1
    local actual=0
    [[ -f cached-stages.tsv ]] && actual=$(tail -n +2 cached-stages.tsv | wc -l | tr -d ' ')
    if [[ "$actual" != "$expected" ]]; then
        echo "  ASSERT FAILED: expected ${expected} cached stages, got ${actual}"
        TEST_FAILED=1
    fi
}

assert_file_exists() {
    if [[ ! -f "$1" ]]; then
        echo "  ASSERT FAILED: file not found: $1"
        TEST_FAILED=1
    fi
}

assert_log_contains() {
    if ! grep -qF "$1" "$LAST_OUTPUT"; then
        echo "  ASSERT FAILED: log does not contain: $1"
        TEST_FAILED=1
    fi
}

# Schema-shape check for an archived stage.json (v1).
assert_stage_json_v1() {
    local json=$1
    if [[ ! -f "$json" ]]; then
        echo "  ASSERT FAILED: stage.json not found at $json"
        TEST_FAILED=1
        return
    fi
    grep -q '"schema_version": "v1"' "$json" || {
        echo "  ASSERT FAILED: schema_version != v1 in $json"; TEST_FAILED=1; }
    grep -q '"take":'                  "$json" || {
        echo "  ASSERT FAILED: take field missing in $json"; TEST_FAILED=1; }
    grep -q '"emit":'                  "$json" || {
        echo "  ASSERT FAILED: emit field missing in $json"; TEST_FAILED=1; }
    if grep -q '"content_digest"' "$json"; then
        echo "  ASSERT FAILED: content_digest must not appear in v1 schema in $json"
        TEST_FAILED=1
    fi
}

# Runner: setup → eval test fn → report.
# Sandboxes live under `mktemp -d` (TMPDIR) and are reaped by the OS
# automatically. We always print the path so you can cd in and inspect
# the archive, stage.json, .nextflow.log, etc.
run_test() {
    local name=$1
    local fn=$2
    TEST_FAILED=0
    setup_test_dir
    echo ""
    echo "=== TEST: ${name} ==="
    eval "$fn"
    cd "$TESTS_DIR"
    if [[ $TEST_FAILED -eq 0 ]]; then
        echo "  PASS (${TEST_DIR})"
        ((PASS++))
    else
        echo "  FAIL (${TEST_DIR})"
        ((FAIL++))
    fi
}

# ----------------------------------------------------------------------
# Tests
# ----------------------------------------------------------------------

# Basic archive + restore (2 stages × 2 samples = 4 tasks).
test_basic() {
    write_config
    $NXF run "${TESTS_DIR}/test-basic.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 4

    # archive schema check (one per stage is enough)
    local prep_json align_json
    prep_json=$(find .nf-stage-archive/PREPARE -name stage.json | head -1)
    align_json=$(find .nf-stage-archive/ALIGN   -name stage.json | head -1)
    assert_stage_json_v1 "$prep_json"
    assert_stage_json_v1 "$align_json"

    between_runs
    $NXF run "${TESTS_DIR}/test-basic.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 0
    assert_cached_stages 2
}

# Multi-emit channels (queue + value mixed).
test_multi_emit() {
    write_config
    $NXF run "${TESTS_DIR}/test-multi-emit.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 4

    between_runs
    $NXF run "${TESTS_DIR}/test-multi-emit.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 0
    assert_cached_stages 1
}

# Value-channel-only emit.
test_value_channel() {
    write_config
    $NXF run "${TESTS_DIR}/test-value-channel.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 2

    between_runs
    $NXF run "${TESTS_DIR}/test-value-channel.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 0
}

# Single-file (non-tuple) emit.
test_single_file() {
    write_config
    $NXF run "${TESTS_DIR}/test-single-file.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 2

    between_runs
    $NXF run "${TESTS_DIR}/test-single-file.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 0
}

# Two samples producing identically-named files must not collide in archive.
test_same_filename() {
    write_config
    $NXF run "${TESTS_DIR}/test-same-filename.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 2

    between_runs
    $NXF run "${TESTS_DIR}/test-same-filename.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 0

    local f1 f2
    f1=$(find .nf-stage-archive -path '*/0/report.txt' 2>/dev/null)
    f2=$(find .nf-stage-archive -path '*/1/report.txt' 2>/dev/null)
    assert_file_exists "$f1"
    assert_file_exists "$f2"
    if diff -q "$f1" "$f2" > /dev/null 2>&1; then
        echo "  ASSERT FAILED: same-name files should have different content"
        TEST_FAILED=1
    fi
}

# 3-level chain, last-stage param flip → only last stage invalidates.
test_chain_last() {
    write_config
    $NXF run "${TESTS_DIR}/test-chain.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 6

    between_runs
    $NXF run "${TESTS_DIR}/test-chain.nf" -c stage.config --param_c v2 > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 2
    assert_cached_stages 2
}

# 3-level chain, middle-stage param flip → middle + last invalidate.
test_chain_middle() {
    write_config
    $NXF run "${TESTS_DIR}/test-chain.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 6

    between_runs
    $NXF run "${TESTS_DIR}/test-chain.nf" -c stage.config --param_b v2 > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 4
    assert_cached_stages 1
}

# Fan-in: 2 producers → 1 collator.
test_fan_in() {
    write_config
    $NXF run "${TESTS_DIR}/test-fan-in.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 6

    between_runs
    $NXF run "${TESTS_DIR}/test-fan-in.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 0
    assert_cached_stages 3
}

# No stage scope in config → must behave like native Nextflow.
test_no_plugin() {
    write_noplugin_config
    # -C (uppercase) ignores the launch-dir nextflow.config entirely.
    $NXF -C noplugin.config run "${TESTS_DIR}/test-basic.nf" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 4
}

# A process emits files outside the stage cache, then a named workflow
# receives them — hook must accept untracked-process inputs.
test_untracked_process() {
    write_config
    $NXF run "${TESTS_DIR}/test-untracked-process.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 3

    between_runs
    $NXF run "${TESTS_DIR}/test-untracked-process.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 1
    assert_cached_stages 1
}

# Workflow calls workflow (recursive interception).
test_nested_workflow() {
    write_config
    $NXF run "${TESTS_DIR}/test-nested-workflow.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 6

    between_runs
    $NXF run "${TESTS_DIR}/test-nested-workflow.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 0
}

# Scale check: 10 samples × 2 stages.
test_many_samples() {
    write_config
    $NXF run "${TESTS_DIR}/test-many-samples.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 20

    between_runs
    $NXF run "${TESTS_DIR}/test-many-samples.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 0
    assert_cached_stages 2
}

# Source-deletion recovery: input files vanish between runs; the 3-step
# fallback must recover via this stage's own prior archive scan.
test_source_deleted() {
    write_config
    mkdir sandbox
    cp data/sample1.fq data/sample2.fq sandbox/

    $NXF run "${TESTS_DIR}/test-relocate.nf" -c stage.config \
        --input_dir "${TEST_DIR}/sandbox" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 4

    between_runs
    rm -f sandbox/sample1.fq sandbox/sample2.fq

    $NXF run "${TESTS_DIR}/test-relocate.nf" -c stage.config \
        --input_dir "${TEST_DIR}/sandbox" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 0
    assert_cached_stages 2
}

# Cross-cluster portability: same content + name, different absolute path
# between runs must hit the same archive (path excluded from hash).
test_cross_cluster() {
    write_config
    mkdir sandbox_a sandbox_b
    cp data/sample1.fq data/sample2.fq sandbox_a/
    cp data/sample1.fq data/sample2.fq sandbox_b/

    $NXF run "${TESTS_DIR}/test-relocate.nf" -c stage.config \
        --input_dir "${TEST_DIR}/sandbox_a" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 4

    between_runs
    $NXF run "${TESTS_DIR}/test-relocate.nf" -c stage.config \
        --input_dir "${TEST_DIR}/sandbox_b" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 0
    assert_cached_stages 2
}

# Pure-static stage: regression for a deadlock where runStage's
# `clonedChannels.isEmpty()` branch synchronously called decide() on the
# main thread, blocking on value-channel getVal() before the workflow
# body's process could fire.
#
# Cache HITS for pure-static stages are recorded (TSV row, downstream
# placeholders bound to archived emit), but the workflow's own process
# still executes — the clone-trick only gates channel inputs, and a raw
# `take` value is wired directly to the process by Nextflow's auto-wrap.
# So the assertion here is "hit logged", not "process skipped".
test_static_only() {
    write_config
    # Phase 1: cold, runs the process and archives.
    $NXF run "${TESTS_DIR}/test-static-only.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 1
    assert_cached_stages 0

    between_runs
    # Phase 2: warm, same param — pure-static HIT skips the workflow body
    # entirely, so the process is never registered and completed must be 0.
    $NXF run "${TESTS_DIR}/test-static-only.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 0
    assert_log_contains "Reusing archived stage VERSION_REPORT"
    assert_cached_stages 1

    between_runs
    # Phase 3: different param, must miss and run again.
    $NXF run "${TESTS_DIR}/test-static-only.nf" -c stage.config --summary_version v9 \
        > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 1
    assert_cached_stages 0
}

# Readonly mode: stage.writable=false must (a) not create an archive on
# miss, (b) still serve hits when the archive exists.
test_readonly() {
    cat > readonly.config <<'EOF'
stage {
    archiveRoot = '.nf-stage-archive'
    writable    = false
}
workDir = 'work'
params {
    reference = 'GRCh38'
}
EOF
    # Phase 1: readonly + no existing archive → runs but does not write.
    $NXF run "${TESTS_DIR}/test-basic.nf" -c readonly.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 4
    if [[ -d .nf-stage-archive ]]; then
        echo "  ASSERT FAILED: archive must not be created when writable=false"
        TEST_FAILED=1
    fi

    between_runs

    # Phase 2: switch to writable to pre-populate the archive.
    write_config
    $NXF run "${TESTS_DIR}/test-basic.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 4

    between_runs

    # Phase 3: readonly + pre-populated archive → cache hits.
    $NXF run "${TESTS_DIR}/test-basic.nf" -c readonly.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 0
}

# 3-step fallback: source files missing AND no historical archive → throw.
# Pipeline must exit nonzero and log a "does not exist" message.
test_source_deleted_no_archive() {
    write_config
    mkdir empty_sandbox
    set +e
    $NXF run "${TESTS_DIR}/test-relocate.nf" -c stage.config \
        --input_dir "${TEST_DIR}/empty_sandbox" > "$LAST_OUTPUT" 2>&1
    local rc=$?
    set -e
    if [[ $rc -eq 0 ]]; then
        echo "  ASSERT FAILED: expected nonzero exit when source missing AND no archive"
        TEST_FAILED=1
    fi
    if ! grep -qE 'does not exist|has never been recorded' "$LAST_OUTPUT"; then
        echo "  ASSERT FAILED: expected 'does not exist' error message"
        TEST_FAILED=1
    fi
}

# Input file content changes between runs → digest changes → stage misses
# on the changed sample. Cold → warm-hit → modified-miss sequence.
test_file_content_change() {
    write_config
    # Replace the shared-data symlink with a local mutable copy.
    rm data
    mkdir data
    cp "${DATA_DIR}"/sample1.fq "${DATA_DIR}"/sample2.fq data/

    # Cold run.
    $NXF run "${TESTS_DIR}/test-basic.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 4

    between_runs
    # Warm run with unchanged content → full hit.
    $NXF run "${TESTS_DIR}/test-basic.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 0
    assert_cached_stages 2

    between_runs
    # Modify content → both stages invalidate (channel emission digest changes,
    # whole-stage granularity means all 4 tasks re-run).
    echo "modified" >> data/sample1.fq
    $NXF run "${TESTS_DIR}/test-basic.nf" -c stage.config > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 4
}

# ----------------------------------------------------------------------
# Entry
# ----------------------------------------------------------------------

declare -a ALL_TESTS=(
    "basic              test_basic"
    "multi-emit         test_multi_emit"
    "value-channel      test_value_channel"
    "single-file        test_single_file"
    "same-filename      test_same_filename"
    "chain-last         test_chain_last"
    "chain-middle       test_chain_middle"
    "fan-in             test_fan_in"
    "no-plugin          test_no_plugin"
    "untracked-process  test_untracked_process"
    "nested-workflow    test_nested_workflow"
    "many-samples       test_many_samples"
    "source-deleted     test_source_deleted"
    "cross-cluster      test_cross_cluster"
    "static-only        test_static_only"
    "readonly           test_readonly"
    "source-deleted-no-archive  test_source_deleted_no_archive"
    "file-content-change        test_file_content_change"
)

if [[ $# -eq 1 ]]; then
    pattern=$1
    matched=0
    for spec in "${ALL_TESTS[@]}"; do
        read -r name fn <<< "$spec"
        if [[ "$name" == "$pattern" ]]; then
            run_test "$name" "$fn"
            matched=1
            break
        fi
    done
    if [[ $matched -eq 0 ]]; then
        echo "Unknown test: $pattern"
        echo "Available tests:"
        for spec in "${ALL_TESTS[@]}"; do
            read -r name fn <<< "$spec"
            echo "  $name"
        done
        exit 1
    fi
else
    for spec in "${ALL_TESTS[@]}"; do
        read -r name fn <<< "$spec"
        run_test "$name" "$fn"
    done
fi

echo ""
echo "================================"
echo "Results: ${PASS} passed, ${FAIL} failed"
echo "================================"

[[ $FAIL -eq 0 ]]
