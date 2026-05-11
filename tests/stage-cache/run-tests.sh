#!/bin/bash
#
# Stage cache integration test suite (M2 scope: normal mode only).
# Run from the tests/stage-cache/ directory.
#
# Usage: ./run-tests.sh [test_name]
#   - no args:   run all tests
#   - test_name: run a single test (e.g. ./run-tests.sh basic)
#
# Override NXF env to point at a different Nextflow build:
#   NXF=nextflow ./run-tests.sh        # use system-installed nextflow
#   NXF=../../launch.sh ./run-tests.sh # use fork dev build (default)

set -uo pipefail

NXF=${NXF:-../../launch.sh}
PASS=0
FAIL=0
LAST_OUTPUT=$(mktemp)
TEST_FAILED=0
TEST_ID=""

cleanup() {
    rm -rf ".nextflow" "work-${TEST_ID}" ".nf-stage-archive-${TEST_ID}" \
           "cached-stages-${TEST_ID}.tsv" ".stage-test-${TEST_ID}.config" \
           ".stage-noplugin-${TEST_ID}.config" 2>/dev/null || true
}

assert_completed() {
    local expected=$1
    local actual
    actual=$(grep -oE 'completed=[0-9]+' "$LAST_OUTPUT" | tail -1 | cut -d= -f2)
    if [[ "$actual" != "$expected" ]]; then
        echo "  ASSERT FAILED: expected completed=${expected}, got completed=${actual}"
        TEST_FAILED=1
        return 1
    fi
}

assert_cached_stages() {
    local expected=$1
    local tsv="cached-stages-${TEST_ID}.tsv"
    local actual
    if [[ ! -f "$tsv" ]]; then
        actual=0
    else
        actual=$(tail -n +2 "$tsv" | wc -l | tr -d ' ')
    fi
    if [[ "$actual" != "$expected" ]]; then
        echo "  ASSERT FAILED: expected ${expected} cached stages, got ${actual}"
        TEST_FAILED=1
        return 1
    fi
}

assert_file_exists() {
    if [[ ! -f "$1" ]]; then
        echo "  ASSERT FAILED: file not found: $1"
        TEST_FAILED=1
        return 1
    fi
}

assert_log_contains() {
    if ! grep -qF "$1" "$LAST_OUTPUT"; then
        echo "  ASSERT FAILED: log does not contain: $1"
        TEST_FAILED=1
        return 1
    fi
}

run_test() {
    local name=$1
    local fn=$2
    TEST_ID="${name//[^a-zA-Z0-9]/-}-$$"
    TEST_FAILED=0
    echo ""
    echo "=== TEST: ${name} ==="
    eval "$fn"
    if [[ $TEST_FAILED -eq 0 ]]; then
        echo "  PASS"
        ((PASS++))
    else
        echo "  FAIL"
        ((FAIL++))
    fi
    cleanup
}

# Generate per-test nextflow.config with stage block + params
test_config() {
    cat > ".stage-test-${TEST_ID}.config" << EOF
stage {
    archiveRoot      = '.nf-stage-archive-${TEST_ID}'
    cachedStagesFile = 'cached-stages-${TEST_ID}.tsv'
}
workDir = 'work-${TEST_ID}'
params {
    reference = 'GRCh38'
    dbsnp = 'dbsnp154'
    param_b = 'v1'
    param_c = 'v1'
    expected_total = 100
    summary_version = 'v1'
}
EOF
    echo ".stage-test-${TEST_ID}.config"
}

# Generate per-test config WITHOUT stage scope (no-cache baseline)
noplugin_config() {
    cat > ".stage-noplugin-${TEST_ID}.config" << EOF
workDir = 'work-${TEST_ID}'
params {
    reference = 'GRCh38'
    dbsnp = 'dbsnp154'
    param_b = 'v1'
    param_c = 'v1'
    expected_total = 100
    summary_version = 'v1'
}
EOF
    echo ".stage-noplugin-${TEST_ID}.config"
}

between_runs() {
    # keep archive, drop session/work for next run
    rm -rf ".nextflow" "work-${TEST_ID}" 2>/dev/null || true
}

# ------------------------------------------------------------------
# Test 1: Basic archive and restore
# ------------------------------------------------------------------
test_basic() {
    local cfg; cfg=$(test_config)
    $NXF run test-basic.nf -c "$cfg" -work-dir "work-${TEST_ID}" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 4

    between_runs
    $NXF run test-basic.nf -c "$cfg" -work-dir "work-${TEST_ID}" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 0
    assert_cached_stages 2
    rm -f "$cfg"
}

# Note: dropped test-partial-rerun.nf — structurally identical to
# test-chain.nf (3-stage chain + 2 params); the partial-invalidation
# semantics are covered by chain-last + chain-middle.

# ------------------------------------------------------------------
# Test 2: Multi-emit channels
# ------------------------------------------------------------------
test_multi_emit() {
    local cfg; cfg=$(test_config)
    $NXF run test-multi-emit.nf -c "$cfg" -work-dir "work-${TEST_ID}" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 4

    between_runs
    $NXF run test-multi-emit.nf -c "$cfg" -work-dir "work-${TEST_ID}" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 0
    assert_cached_stages 1
    rm -f "$cfg"
}

# ------------------------------------------------------------------
# Test 5: Value channel emit
# ------------------------------------------------------------------
test_value_channel() {
    local cfg; cfg=$(test_config)
    $NXF run test-value-channel.nf -c "$cfg" -work-dir "work-${TEST_ID}" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 2

    between_runs
    $NXF run test-value-channel.nf -c "$cfg" -work-dir "work-${TEST_ID}" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 0
    rm -f "$cfg"
}

# ------------------------------------------------------------------
# Test 6: Single file emit (not tuple)
# ------------------------------------------------------------------
test_single_file() {
    local cfg; cfg=$(test_config)
    $NXF run test-single-file.nf -c "$cfg" -work-dir "work-${TEST_ID}" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 2

    between_runs
    $NXF run test-single-file.nf -c "$cfg" -work-dir "work-${TEST_ID}" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 0
    rm -f "$cfg"
}

# ------------------------------------------------------------------
# Test 7: Same filename across samples
# ------------------------------------------------------------------
test_same_filename() {
    local cfg; cfg=$(test_config)
    $NXF run test-same-filename.nf -c "$cfg" -work-dir "work-${TEST_ID}" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 2

    between_runs
    $NXF run test-same-filename.nf -c "$cfg" -work-dir "work-${TEST_ID}" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 0

    local f1 f2
    f1=$(find ".nf-stage-archive-${TEST_ID}" -path "*/0/report.txt" 2>/dev/null)
    f2=$(find ".nf-stage-archive-${TEST_ID}" -path "*/1/report.txt" 2>/dev/null)
    assert_file_exists "$f1"
    assert_file_exists "$f2"
    if diff -q "$f1" "$f2" > /dev/null 2>&1; then
        echo "  ASSERT FAILED: same-name files should have different content"
        TEST_FAILED=1
    fi
    rm -f "$cfg"
}

# ------------------------------------------------------------------
# Test 8: Three-level chain - change last param
# ------------------------------------------------------------------
test_chain_last() {
    local cfg; cfg=$(test_config)
    $NXF run test-chain.nf -c "$cfg" -work-dir "work-${TEST_ID}" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 6

    between_runs
    $NXF run test-chain.nf -c "$cfg" -work-dir "work-${TEST_ID}" --param_c v2 > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 2
    assert_cached_stages 2
    rm -f "$cfg"
}

# ------------------------------------------------------------------
# Test 9: Three-level chain - change middle param
# ------------------------------------------------------------------
test_chain_middle() {
    local cfg; cfg=$(test_config)
    $NXF run test-chain.nf -c "$cfg" -work-dir "work-${TEST_ID}" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 6

    between_runs
    $NXF run test-chain.nf -c "$cfg" -work-dir "work-${TEST_ID}" --param_b v2 > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 4
    assert_cached_stages 1
    rm -f "$cfg"
}

# ------------------------------------------------------------------
# Test 10: Fan-in stage
# ------------------------------------------------------------------
test_fan_in() {
    local cfg; cfg=$(test_config)
    $NXF run test-fan-in.nf -c "$cfg" -work-dir "work-${TEST_ID}" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 6

    between_runs
    $NXF run test-fan-in.nf -c "$cfg" -work-dir "work-${TEST_ID}" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 0
    assert_cached_stages 3
    rm -f "$cfg"
}

# ------------------------------------------------------------------
# Test 11: No-plugin compatibility (stage scope absent → behaves like native)
# ------------------------------------------------------------------
test_no_plugin() {
    local cfg; cfg=$(noplugin_config)
    # -C (uppercase) ignores nextflow.config in launch dir
    $NXF -C "$cfg" run test-basic.nf -work-dir "work-${TEST_ID}" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 4
    rm -f "$cfg"
}

# Note: dropped untracked-channel — the same pattern (Channel.of → named
# workflow) is already covered by basic.nf; our hook fires after spread
# so "untracked" channels need no special handling.

# ------------------------------------------------------------------
# Test 11: Untracked process output passed to named workflow
# ------------------------------------------------------------------
test_untracked_process() {
    local cfg; cfg=$(test_config)
    $NXF run test-untracked-process.nf -c "$cfg" -work-dir "work-${TEST_ID}" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 3

    between_runs
    $NXF run test-untracked-process.nf -c "$cfg" -work-dir "work-${TEST_ID}" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 1
    assert_cached_stages 1
    rm -f "$cfg"
}

# ------------------------------------------------------------------
# Test 14: Nested named workflows (workflow calls workflow)
# ------------------------------------------------------------------
test_nested_workflow() {
    local cfg; cfg=$(test_config)
    $NXF run test-nested-workflow.nf -c "$cfg" -work-dir "work-${TEST_ID}" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 6

    between_runs
    $NXF run test-nested-workflow.nf -c "$cfg" -work-dir "work-${TEST_ID}" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 0
    rm -f "$cfg"
}

# ------------------------------------------------------------------
# Test 15: Many samples (10 samples)
# ------------------------------------------------------------------
test_many_samples() {
    local cfg; cfg=$(test_config)
    $NXF run test-many-samples.nf -c "$cfg" -work-dir "work-${TEST_ID}" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 20

    between_runs
    $NXF run test-many-samples.nf -c "$cfg" -work-dir "work-${TEST_ID}" > "$LAST_OUTPUT" 2>&1 || true
    assert_completed 0
    assert_cached_stages 2
    rm -f "$cfg"
}

# Note: dropped many-samples-rerun — chain-last/chain-middle already
# verify partial-invalidation propagation; many-samples is kept as a
# scale-only test.

# NOTE: A "multi-emit ChannelOut passthrough" test was intentionally
# omitted. Nextflow's TypeCheckingVisitor rejects calls where the
# parsed argument count doesn't match the callee's declared parameter
# count, regardless of runtime ChannelOut.spread expansion. So
# `WORKFLOW(channelOutSize2)` to a 2-input workflow is a parse error,
# meaning the "size>1 broken in nf-stage" scenario can't actually be
# written by users and there's nothing for us to test here.

# ------------------------------------------------------------------
# Run tests
# ------------------------------------------------------------------
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
)

if [[ $# -eq 1 ]]; then
    # filter to single test by name prefix
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

rm -f "$LAST_OUTPUT"
[[ $FAIL -eq 0 ]]
