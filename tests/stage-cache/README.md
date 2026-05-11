# Stage cache integration tests

End-to-end test suite for the stage cache feature (see
[`adr/20260511-stage-cache.md`](../../adr/20260511-stage-cache.md)).

## Running

```bash
./run-tests.sh                # all 17 tests
./run-tests.sh basic          # one test by short name
NXF=nextflow ./run-tests.sh   # use system-installed nextflow
```

Default `NXF` is `../../launch.sh` (this fork's dev build). Run
`make compile` once at the repo root before invoking the suite.

Each test runs inside an isolated `mktemp -d` directory:

- **Pass** → temporary directory is removed.
- **Fail** → temporary directory is preserved and its path is printed,
  so you can inspect `stage.config`, `.nf-stage-archive/`,
  `cached-stages.tsv`, the Nextflow log, etc.

`tests/stage-cache/` itself contains only `.nf` test files, shared
`data/`, the runner, and this README. Nothing is written here by
`run-tests.sh`.

## Tests

| Test                 | What it verifies |
|----------------------|------------------|
| `basic`              | 2-stage pipeline (PREPARE → ALIGN), cold archive + warm reuse. |
| `multi-emit`         | A stage emitting multiple channels (queue + value mixed). |
| `value-channel`      | Value-channel-only emit round-trips correctly. |
| `single-file`        | Single-file (non-tuple) emit shape works. |
| `same-filename`      | Two samples producing identically-named files don't collide in the archive. |
| `chain-last`         | 3-level chain; flipping the last-stage param only invalidates the last stage. |
| `chain-middle`       | 3-level chain; flipping the middle-stage param invalidates middle + last. |
| `fan-in`             | Two producer stages feeding one collator stage. |
| `no-plugin`          | Without a `stage` scope, Nextflow behaves natively (hook is a no-op). |
| `untracked-process`  | A process output (not from a named workflow) flowing into a named workflow. |
| `nested-workflow`    | A named workflow calling another named workflow (recursive interception). |
| `many-samples`       | 10 samples × 2 stages — scale sanity check. |
| `source-deleted`     | Input files deleted between runs; 3-step fallback recovers via archive scan. |
| `cross-cluster`      | Same content + name at different absolute paths between runs still hits. |
| `readonly`           | `stage.writable=false` does not write on miss but still serves hits. |
| `source-deleted-no-archive` | Source missing AND no archive: 3-step fallback throws and pipeline exits nonzero. |
| `file-content-change`| Modifying an input file's content between runs invalidates the cache. |

## Adding a new test

1. Drop a `.nf` file in this directory. Reference test data as
   `file("data/${name}")` — the runner symlinks `data/` into every
   test's working directory.
2. Add a `test_<name>()` function in `run-tests.sh`. It runs in a
   pre-`cd`'d sandbox, with `$NXF`, `${TESTS_DIR}` and `${TEST_DIR}`
   already set, and helpers (`write_config`, `between_runs`,
   `assert_completed`, `assert_cached_stages`, etc.) available.
3. Register the test in the `ALL_TESTS` array at the bottom of
   `run-tests.sh` (short-name → function-name mapping).
4. Run `./run-tests.sh <short-name>` to verify.

A `.nf` file used by more than one test function should carry a
`Reused by: ...` header comment listing the function names.

## Data convention

`data/sample1.fq` and `data/sample2.fq` are tiny shared fixtures.
Tests that need different bytes or naming should generate them inside
their own sandbox (e.g. `test_source_deleted` copies the shared files
into `sandbox/` and then deletes them between runs). Don't add new
files to `data/` unless multiple tests need them.

## Spock unit tests

These end-to-end tests cover the integrated workflow path. The class-
level Spock tests for `StageTake`, `StageArchive`, `StageCache`, and
`StageConfig` live under
`modules/nextflow/src/test/groovy/nextflow/cache/stage/` and are run
via `./gradlew :nextflow:test --tests "nextflow.cache.stage.*"`.
