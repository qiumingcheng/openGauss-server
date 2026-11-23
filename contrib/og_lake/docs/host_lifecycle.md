# Guard cleanup lifecycle evidence and deferred patch

Read-only audit baseline: `feature/native-lake@e03a84ce0c` (Storage-context panic fix). The described late-cleanup and blocking-acquisition changes are integrated in the 0.2 source. They require the next full build and real tests; no runtime acceptance is claimed here.

## Native order verified in this tree

All source paths below are relative to the isolated openGauss worktree. Line numbers describe the pre-patch baseline.

| Path / lines | Observed native order | Required guard action |
|---|---|---|
| `src/gausskernel/storage/access/transam/xact.cpp:2940–3047` | COMMIT callback precedes RecordTransactionCommit; ProcArrayEndTransaction follows actual record; N0 LakeHostTransactionEnd was immediately afterward | N0 position proves commit visibility only, not completed resource/reader cleanup |
| same file `3080–3128` | BEFORE_LOCKS -> buffer/relation/partition cleanup -> AtEOXact_Inval(true) -> pending native storage/connection/library cleanup -> MultiXact -> LOCKS | Put LakeHostTransactionEnd(true) immediately before the LOCKS call |
| same file `4018–4036` | ABORT callback -> BEFORE_LOCKS -> native cache cleanup -> AtEOXact_Inval(false) -> pending storage/connection/library cleanup -> MultiXact -> LOCKS | Do not release in ABORT callback; put LakeHostTransactionEnd(false) immediately before LOCKS |
| same file `6421–6470` | child COMMIT callback precedes resource cleanup; native locks are adopted by parent | Reparent the guard owner without unlocking; no early loss of protection |
| same file `6675–6697` | child ABORT callback precedes BEFORE_LOCKS -> child relcache/inval -> AtSubAbort_smgr -> child LOCKS | New LakeHostSubTransactionAbort(subid) releases only this child's guards immediately before child LOCKS |
| `src/common/backend/utils/resowner/resowner.cpp:310–410` | BEFORE_LOCKS releases buffers and relation-related resources; top-level LOCKS calls ProcReleaseLocks, subabort LOCKS calls LockReleaseCurrentOwner | Use the boundary between those phases; do not emulate generic lock ownership with snapshot IDs |
| `src/gausskernel/storage/lmgr/proc.cpp:1449–1469` | top-level generic cleanup preserves USER_LOCKMETHOD session holds, on commit and abort | Explicit owned guard release remains necessary; maintenance claim must survive transaction endings |

The late helpers only inspect the nullable per-session state, release the registered guards, free their Storage-context nodes and increment counters. They do not allocate state on ordinary transactions, execute SQL, publish catalog metadata, call the SDK or dispatch a new generic callback event. Existing MOT, PLpgSQL and FDW callback behavior is unchanged.

The top-level abort hook remains inside the existing `TopTransactionResourceOwner != NULL` cleanup branch. Lake SQL cannot acquire a transaction guard before a top resource owner exists. The child abort hook likewise runs in the native initialized-subtransaction-owner branch. Parent guards are retained on child abort. PRE_PREPARE still refuses active diagnostic claims/guards; no 2PC support is introduced.

The patch also corrects the session-pointer allocation comment to match the already implemented writable Storage child context. It does not unseal a top context or change struct layout.

## Blocking acquisition cancellation race

`src/gausskernel/storage/lmgr/proc.cpp:1408–1428` documents that a failed/cancelled waiter may already have been granted its requested lock; `LockErrorCleanup()` can call `GrantAwaitedLock()`. If an unlinked LakeCatalogGuard is simply freed in the catch path, generic abort preserves the newly granted session lock and nobody can release it via the state list.

The blocking acquisition catch path now calls `LockErrorCleanup()` in that acquisition catch path, checks whether the exact internal key/mode was granted, releases it if present, then frees the not-yet-linked guard and rethrows. The idempotence lookup ran before acquisition, so this path has no earlier registered owner of that same key to release accidentally. It affects neither SQL's public advisory key spaces nor a parent's existing guard. Successful acquisition is linked before returning, and normal reader waiting uses the host lock manager and its cancellation/timeouts rather than polling.

## What these patches do not prove

- No physical deletion capability is enabled. Correct release ordering is necessary but does not establish an old-RR retirement horizon or RAC-wide reader safety.
- The current inline slice uses native TableAM buffers/relation references and executor memory. A future SDK resource must register a real cancellation/drain/close path which completes before the late hook; merely reaching BEFORE_LOCKS does not prove an arbitrary SDK handle is closed.
- Stream workers and asynchronous I/O must be joined/drained before their protection is released. A session-state empty check cannot replace counting actual live operations.
- A held cursor may outlive a transaction only if it has fully materialized all data while protected, or holds an explicitly supported independent protection lifetime. No such new protocol is supplied here.
- No post-commit helper should raise a recoverable error attempting I/O, allocate complex cleanup state or publish metadata after the transaction is irreversible. Resource ownership must make this late cleanup bounded and noncritical; failure-to-quiesce behavior needs explicit design/testing before SDK/physical-GC admission.
- Explicit diagnostic guard release while a future reader retains references needs separate active-reference checks. Diagnostic helpers remain internal and administrator-only; passing N0 does not authorize file GC.

## Required next-run checks

1. Static order assertion over the integrated source: true/false late hooks follow their BEFORE_LOCKS and invalidation calls and precede native LOCKS; generic COMMIT/ABORT callbacks contain no guard release; child abort late hook has the same order.
2. Rerun all N0 commit/rollback/savepoint/idempotence/disconnect/unlock_all/DISCARD assertions on the new binary. Existing N0 evidence cannot stand in for this changed lifecycle.
3. Real blocking test: holder takes exclusive guard; reader waits; statement timeout, cancellation and backend disconnect each leave no untracked session lock; release+cancel racing at grant must be repeated and checked by a third session taking exclusive guard.
4. N2 LIMIT/ERROR/cancel/rescan and child rollback tests must show no guard or native buffer/relation leak. Add a deterministic pre-release barrier test when the resource-drain seam exists, rather than claiming the current stress tests prove an uninstrumented instant.
5. SDK/Stream/held-cursor/async-I/O drain and RAC recovery gates remain blocked until the corresponding implementations and environments exist.
