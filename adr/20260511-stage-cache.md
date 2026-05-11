# Stage cache: workflow-level content-addressed archive with source-deletion recovery

- Authors: huangzhibo
- Status: accepted
- Date: 2026-05-11
- Tags: cache, workflows, fork-only

## Summary

Add an opt-in **named-workflow-level** content-addressed cache that lets a pipeline reuse archived outputs of prior runs without consulting the work directory or the original source files. The cache is enabled by `stage.archiveRoot = ...` in `nextflow.config`; absent this scope it is a no-op and Nextflow behaves natively.

## Problem Statement

Bioinformatics pipelines in our environment routinely run for hours-to-days, produce intermediate artifacts of 10s-100s of GB per stage, and are re-executed for re-analysis weeks later. Two operational realities drive the design:

1. **Source-deletion is the primary scenario.** Original FASTQ and `work/` directories are systematically deleted ~N days after analysis completes. Re-analysis starts from an intermediate stage — there is no original input to hash and no work directory to resume from.

2. **Cross-cluster relocation is routine.** Archives are produced on one cluster and consumed on another with completely different absolute paths. Any hash that depends on `Path.toString()` breaks portability.

Nextflow's native `-resume` solves neither: it is task-level, requires the `work/` directory, and hashes file paths into the task fingerprint. Issue [#5589](https://github.com/nextflow-io/nextflow/issues/5589) tracks the upstream gap.

## Goals

- **Granularity**: cache at named-workflow level (e.g. `PREPARE { … }`), so an entire stage skips when its declared inputs match a prior run.
- **Source-deletion recovery**: a stage hashes successfully even when its declared input files have been deleted, as long as some prior archive of the same stage recorded them.
- **Cross-cluster portability**: the same input set produces the same archive digest regardless of the absolute path it lives at.
- **Schema stability**: one schema, written the same way regardless of which fallback path produced the checksums. No mode flags in stored archives.
- **Zero impact when disabled**: no `stage` scope ⇒ native Nextflow behavior, no hook overhead beyond a single `isEnabled()` check.

## Non-goals

- Replacing or interacting with Nextflow's native `-resume`. The two are orthogonal: a stage cache hit skips the whole stage; a stage cache miss runs the stage with native task-level resume semantics intact.
- Distributed cache, network-shared archive coordination, or eviction policies. The archive is a passive on-disk directory tree; operational policies (retention, sync, GC) are external.
- Process-level or task-level caching. That's `-resume`.
- Upstream contribution. This ships only in our `huangzhibo/nextflow` fork; rebased onto `upstream/master` periodically (PR [#7076](https://github.com/nextflow-io/nextflow/pull/7076) was withdrawn).

## Considered Options

### Where to put the interception point

1. **Plugin via `WorkflowInterceptor` ExtensionPoint** — proposed upstream as PR [#7076](https://github.com/nextflow-io/nextflow/pull/7076), then withdrawn.
2. **Fork patch in `WorkflowDef.run0()`** — chosen.

### What to hash

1. **Flat per-input digest** (`StageDigest` two-phase: static + channel emissions) — original M1 design.
2. **Canonical "take" structure shared with stage.json** — chosen.

### How to record file identity

1. **Path-string + content-checksum, both in hash**.
2. **Name + content-checksum in hash; path stored separately for recovery** — chosen.

### How to handle deleted source files

1. **strict / relax modes** with mode flag in archive.
2. **Single mode with 3-step fallback** (file read → registry → archive scan, then throw) — chosen.

## Decision

| Concern | Decision |
|---|---|
| Hook | `WorkflowDef.run0()` after `collectInputs` (post-`ChannelOut.spread`), gated on `StageCache.isEnabled()`. |
| Hash input | Canonical JSON of the `take` structure with file `path` fields stripped. |
| Hash output | First 16 hex chars of SHA-256 = archive directory name. **The directory name IS the digest.** |
| stage.json schema | v1, with `take` (symmetric to `emit`), no `content_digest`, no `integrity`, no mode flag. |
| File-checksum fallback | `Files.isReadable` → `knownChecksums` registry → `StageArchive.scanThisStageArchives` → throw. |
| Path canonicalization | `toAbsolutePath().normalize()` on both write and read. |
| Archive integrity | `stage.json` is the commit marker — written last after all file emissions. |
| Disabled state | No `stage` scope ⇒ `runStaged()` not entered, native Nextflow path is unchanged. |

## Rationale & discussion

### Why named-workflow granularity, not process

Task-level caching is what `-resume` already does. Process-level caching wouldn't help our scenario because the original input files are gone. Named workflows are the natural "redo unit" — users already structure pipelines as `PREPARE { } / ALIGN { } / CALL { }`, and they think in those units when saying "re-run from variant calling onward". A named workflow has a stable declared `take:` block (post-`ChannelOut.spread`) which gives us a deterministic identity boundary.

### Why fork-patch, not plugin

The plugin approach required mirroring Nextflow's `ChannelOut.spread` semantics inside the plugin to access workflow inputs. This worked for size-1 outputs but silently broke for size>1 `ChannelOut` passthrough — the parser rejects the call before the plugin ever runs. Even after the upstream `WorkflowInterceptor` PR was prepared, ~150 lines of pf4j SPI plumbing carried no information and made each Nextflow version bump a non-trivial port. Putting the code in `WorkflowDef.run0()` after `collectInputs` gives us spread-resolved inputs for free, removes the plumbing, and limits upstream-conflict surface to a single ~15-line hook insertion.

### Why the "take" structure (not a flat digest)

The original M1 design used `StageDigest` with two phases: hash static inputs immediately, hash channel emissions on completion, combine. This worked but had two limitations:

- The digest was opaque — when source files were deleted in a later run, there was no recorded mapping from "input file path X" to "checksum Y" we could look up.
- The digest input wasn't symmetric with the archived emit — debugging required mentally inverting two different serialization paths.

Storing the take as a structured field with the same shape as emit (`{ inputName: { type, items: [[element, ...]] } }`) makes both problems disappear: we walk `take.<input>.items[*].path` to recover checksums when source files are gone, and `take`/`emit` share `serializeElement`/`rebuildElement` machinery.

### Why hash strips file `path`

Two clusters can hold byte-identical input files at completely different absolute paths (`/cluster-a/data/S1.fq` vs `/cluster-b/runs/2026Q2/S1.fq`). If `path` participates in the hash, the second cluster mis-misses and re-runs. Stripping `path` from the canonicalized hash makes the digest depend only on file name + content checksum. The `path` field is still stored in `take` (for the source-deletion scan), but it is metadata, not identity.

The `name` field intentionally stays in the hash: it's part of the value Nextflow actually passes to downstream processes (the staged-in symlink uses the original filename), so two files with same content but different names are not interchangeable.

### Why the 3-step computeFileChecksum fallback

```
1. file readable  → SHA-256 of contents (authoritative)
2. knownChecksums → cached lookup
3. scan this stage's archives → fill registry, retry; throw if still absent
```

This is the minimal sequence that handles both normal execution (step 1) and source-deletion recovery (step 3) without introducing modes. The registry (step 2) is filled exclusively by the lazy scan in step 3; it never holds entries earlier than that, so there is no risk of a stale path winning over a real file. The scan walks `take.path → checksum` putIfAbsent, so it converges idempotently and runs at most once per stage per run (a missing-and-not-found path immediately throws and terminates the run).

Earlier drafts considered an additional `SENTINEL` value to distinguish "scanned but absent" from "never scanned", and a separate `scannedStages` set. Both were dropped: putIfAbsent + fail-fast-on-miss is sufficient because a path absent after the first scan can never appear in a later one.

### Why drop strict/relax modes

A previous design proposed `strict` mode (refuse to hash if file is gone) and `relax` mode (fall back to archived checksum). This required a mode flag in stored archives, which made the schema mode-dependent and made cross-mode replay ambiguous. Once the 3-step fallback is established, the "relax" behavior IS the only behavior — the natural failure mode (file gone AND no archive recorded it) is an exception, not a different schema variant. Single mode, single schema, no compatibility matrix.

### Why drop `content_digest` and `integrity` fields

- `content_digest`: redundant with the archive directory name. Storing the digest twice invites the two values to disagree on disk.
- `integrity`: redundant with the "stage.json is the commit marker" invariant. Any `stage.json` present implies all referenced files were written, because `writeArchive` writes files first and `stage.json` last under a `Files.exists` first-writer-wins guard.

### Per-iteration variable capture

Two async subscription loops (in `StageArchive.archiveWithForward` and an earlier iteration of `StageCache.subscribeAndCollect`) suffered from Groovy closures capturing the for-loop variable by reference, routing all `onNext` callbacks to the last iteration's bucket. Fixed by binding `final String capturedName = name` inside each iteration. Worth recording because it is the canonical async-loop trap in Groovy and will be re-introduced by any future code that adds a new per-channel subscription loop. Spock regression covers this in `StageArchiveTest.archiveWithForward correctly routes mixed value + queue channels`.

### What isn't done (and why it's OK)

- **knownChecksums never filled from cache-hit emit rebuild or archive-write completion.** These would optimize repeated reads of archived files but don't change correctness; step 1 covers the readable-file case. Add later if profiling shows archive-read cost dominates.
- **Multi-archive checksum conflict resolution.** Two prior archives recording different checksums for the same source path is fundamentally ambiguous; we log a warning and keep the first read, rather than introducing created_at ordering. If conflict matters in practice, add `Files.list` sorted by `created_at` desc before populating.
- **Per-stage opt-out (`stage.<name>.cache = false`).** Not needed yet. Trivial to add when a user hits a stage that should always re-execute.

## Implementation outline

```
modules/nextflow/src/main/groovy/nextflow/cache/stage/
├── StageConfig.groovy      # @ScopeName("stage") typed config
├── StageCache.groovy       # singleton orchestrator + clone-trick
├── StageArchive.groovy     # read/write archive layout + scan
└── StageTake.groovy        # canonical take + 3-step computeFileChecksum

modules/nextflow/src/main/groovy/nextflow/script/WorkflowDef.groovy
    # +15-line hook in run0()  →  StageCache.instance.runStage(...)
```

One file modified upstream (`WorkflowDef.groovy`), four new files in a dedicated package. Rebase conflict surface is small and bounded.

## Links

- Issue: [nextflow-io/nextflow#5589 — Workflow-level caching](https://github.com/nextflow-io/nextflow/issues/5589)
- Withdrawn upstream PR: [nextflow-io/nextflow#7076 — Add WorkflowInterceptor extension point](https://github.com/nextflow-io/nextflow/pull/7076)
- Fork commits: `62dbb60bb` (M1), `2fc9f8516` (M2), `0dad9f985` (M4)
