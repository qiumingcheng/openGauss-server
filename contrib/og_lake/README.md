# Native lake prototype 0.10

The preceding 2e01c41b6b source candidate reads standard LIST<float32> columns as native
`vector(n)` with an explicit dimension. Catalog children retain their own IDs;
the optional SDK footer suffix must confirm the element ID before file reads.
Standard PostgreSQL inline carriers store nested values as text and are decoded
through native vector input. NULL vectors are permitted; NULL elements,
non-finite values and dimension mismatches fail rather than skip data.
Its vector CREATE, DML and flush remain refused. A subsequent isolated source
candidate adds native vector write-set accounting, standard VARCHAR publication,
parent/child CREATE definitions, bounded nested Arrow output, optional ABI4
candidate opening with the original ABI2 abort lifetime, and preserved logical
row IDs/birth versions during flush. Nested leaf statistics are conservative:
old element bounds are invalidated and unverified leaf counts are not published
as parent row counts. The preceding read candidate passed its official Debug full
build and six scoped native cases with 183 strictly reviewed assertions.
The write candidate has not yet been built or run through native acceptance.
Neither adds ANN or DiskANN access.

A new statistic probe on the read candidate found that floating file columns
incorrectly advertised `contains_nan=false` without measuring it. Their logical
values passed the scoped checks, but that does not validate such a statistic.
The write candidate changes the unmeasured flag to SQL NULL; the original
negative and a fresh strict statistic regression are required separately.

Revision fbacecd04a passed the official Debug full build (131 verified inputs)
and 28 scoped native cases with 795 strictly reviewed assertions: four real
scalar-file queries, nine compatibility/root cases, four physical builds and
eleven affected cases. This does not qualify Ustore indexes, DiskANN or fusion.
The following source candidate adds metadata-only current-file coverage status
and accurate host capability flags. File readiness requires runtime validation
and actual EXPLAIN ANALYZE proof; it does not promise a chosen query path.
Revision 1a0adab661 passed the subsequent official Debug incremental build (131
verified inputs) and 35 scoped cases with 981 strictly reviewed assertions:
seven Ustore cases170, four actual file queries136, nine compatibility/root
cases296, four physical builds124 and eleven affected cases255. A genuine
retained-reader transfer/build/two-heap-commit scenario first lost two old rows;
only replacing the session xmin shortcut was insufficient. Native
XidVisibleInSnapshot with the retained snapshot fixes both the horizon and
NULL-snapshot/FROZEN-cache shortcuts, including six advancing RC epochs.
The long nested fixture-ID failure remains preserved with its successful short
rerun. This is local integer correctness evidence, not full index acceptance.

The subsequent source candidate adds streaming leaf verification to explicit
file-index builds. Healthy unchanged sources keep their segment identity;
missing or corrupt derived artifacts are replaced with a new immutable segment
and the former coverage retires only in the same user transaction. New segments
also validate every finished leaf before READY. Verification
does not collect a result-sized posting vector. Cancellation, budget and
unsupported-provider errors propagate. This is synchronous administrative
repair, not automatic background scheduling or physical GC; native integration
and rollback qualification are still pending for this source candidate.
The preceding 1a0adab661 build reproduced its missing-artifact repair gap:
business reads remained complete, but an explicit build kept the broken READY
segment. The original negative and owned cleanup receipts are retained.

Lake business reads, writes, DDL and maintenance require host READ COMMITTED.
Other isolation levels fail with SQLSTATE 0A000, including reuse of a cached
lake plan. RC still retains the existing per-catalog lake read context for an
explicit transaction; active cursors, retained snapshots and SDK references
remain protected through their actual release boundaries.

Revision 34b9e2ea86 passed the scoped 0.5 permissions build and actual LOGIN tests: 12 dedicated cases (483 assertions), two complete reference comparisons (202 assertions), 15 compatibility cases (884 assertions), and the ordinary-user pool suite (808 child and 40 orchestration assertions, with measured cross-role worker reuse). Earlier UPDATE/DELETE, statistics and recovery evidence remains tied to its original source revisions. Revision f434ba4a11 passed its official full build, 131 host assertions and 30 catalog cases with 347 assertions, including actual DuckLake offline interread/continuation and controlled registration-lock conflicts. Test artifact-alias and case-ID packaging defects were repaired with the original attempts preserved. Revision d25228f262 passed its official full build and 17 scoped cases (686 assertions): host, ten statistics cases and six Ustore reader/analysis cases. Its private-transaction and SCROLL tests exposed native API restrictions and are being corrected without weakening those restrictions. Revision 93fb65c12a passed 26 scoped cases with 1414 native assertions and six full first-insert reference checkpoints with 602 child assertions. Those results remain tied to that revision. This is not complete N4 or commercial acceptance. This module is statically linked into gaussdb; SQL objects are installed per database. Production queries retain the openGauss executor; file I/O uses the optional isolated C ABI provider described below.

## Bounded native local-file reads

SQL0.8 can read locally registered immutable Parquet files through
`lake_file_catalog.cpp`, `lake_file_reader.cpp` and `lake_file_sdk.cpp`. The host
selects the fixed business snapshot and files from the standard SQL directory;
the provider reads only the selected file and returns Arrow C Data batches.
Native quals, joins, sorting and aggregation remain in the openGauss executor.

The initial gate admits up to 4096 local relative files with exact primitive
field IDs and bool/int4/int8/text columns, implicit `row_id_start + position`,
and optional inline rows in the same table. It validates every input footer
before the first business row. Visible standard inline file-position deletes,
delete files, field mappings, encryption,
partial/explicit hidden file versions, evolution, file-table DML, file ANALYZE
and Stream execution are rejected. Native projection currently reads all
business columns; the provider's narrower projection API is a separate ability.
Old installed SQL versions retain their file rejection gate.

The library is loaded from `$libdir/og_lake_sdk/libog_lake_sdk_arrow.so` and is
retained for process lifetime. Arrow/Parquet PIC static dependencies are sealed
inside its C ABI library. Query-context cleanup closes batches/readers/footers;
provider calls hold host interrupts and poll cancellation without database
ERROR inside the provider C++ stack. Returned text is copied into native tuple
memory. The source of the vendored Arrow C ABI is recorded under `vendor/arrow20`.

Revision 0c02240af8 passed the official committed build and 39 scoped native
file/compatibility cases (1480 assertions). Those cases did not cover inline
file-position deletes. Two subsequent genuine DuckLake UPDATE/DELETE fixtures
exposed stale native rows; this follow-up rejects that unsupported layout at
the fixed snapshot. The new rejection and affected regression require their
own build and evidence. This restriction is not an implementation of deletes.
The sealed provider separately passed real-file and I/O-fault qualification.
Fresh committed-source native verification is still required; none of these
results completes file DML, flush, private SDK, RAC or commercial acceptance.

## Transactional business CREATE

This 0.7 implementation extends product revision 93fb65c12af4a1bc99c07eb98faf3d83221d6cc9. The official full build and two cpp-only repair builds passed. Seven scoped business CREATE cases passed 353 assertions, including independent fixed-reference metadata comparison and continuation. The savepoint DROP/rollback test found an invalid dependency on rd_createSubid; the publisher now uses its previously validated LOCAL definition, trusted carrier OID and exact dependency. The ordinary state lookup remains forbidden in PRE_COMMIT. Evidence is maintained in the workspace native-lake validation directory; these cases do not complete the full DDL or commercial gate.

`CREATE FOREIGN TABLE biz.t (...) SERVER s OPTIONS (binding_mode 'create', lake_schema 'main')` requires installed SQL 0.7 or 0.8, a configured private owner, a committed writable catalog and an existing top-level lake schema. Ordinary callers need the host schema CREATE and server USAGE permissions (plus any native security-mode requirements). Initial types are nullable bool/int4/int8/text without defaults, explicit collations, inheritance or constraints. Administrator `binding_mode='attach'` remains unchanged. Catalog registration/owner handover cannot be mixed into the same transaction as this business CREATE.

The normal CREATE command makes an empty, private-owner carrier named from a stable UUID and registers its exact OID. Enabled event triggers are rejected before this internal DDL: the native event-trigger catalog ShareLock freezes the enabled set through transaction end. Business CREATE inside an extension script is rejected to keep durable lake carriers independent of extension membership. Effective inlining disabled permits an empty definition without a carrier and rejects data writes.

New tables have explicit LOCAL object identities and a bounded transaction journal. SELECT and subsequent INSERT/UPDATE/DELETE use only command-visible private versions until commit; the physical carrier is not scanned as a committed base. A successful snapshot claim assigns permanent IDs in an attempt-local mapping and publishes standard definitions, host bindings and final surviving rows atomically. No physical DDL runs at PRE_COMMIT. UUIDs, local tokens and the business snapshot survive a failed claim; permanent attempt mappings do not. CREATE-only has no statistics or row/file counter advance. CREATE followed by net-empty row operations still creates the table. CREATE followed by DROP has no lake publication unless other net changes remain.

A temporary carrier-to-host dependency implements uncommitted CREATE→DROP. Successful publication deletes only that exact internal dependency in the same retry subtransaction. Thereafter administrator host DROP/unbind and extension removal preserve standard metadata and carrier data. Ordinary creators may DROP their uncommitted local object; committed host unbinding remains administrator-only under the prior contract.

Runtime locators now use explicit 112-byte wire V2 with an object tag, scan private layout 3 and modify private layout 2. New binaries reject wire V1 and require a full build/restart; old installed SQL versions continue using the new runtime ABI while retaining their existing capability gates. Local object tokens never enter standard metadata, SQL options or cached plan private data. Savepoint rollback removes token-index entries and private rows; bounded inactive object descriptors remain until transaction end for ancestor portal safety.

## Existing first slice

- `CREATE EXTENSION og_lake` installs 0.10. The 0.1→0.2→0.3→0.4→0.5→0.6→0.7→0.8→0.9→0.10 SQL upgrade chain is transactional. Installation itself never creates or changes a standard DuckLake catalog.
- `og_lake.attach_catalog(server_name name, metadata_schema name, credential_ref text, read_only boolean)` attaches an existing same-database format 1.1 catalog. It requires a superuser; writable attachment requires installed SQL version 0.3 through 0.8. INSERT remains available to 0.3 installations; UPDATE/DELETE require the actual 0.4-or-later objects and internal locator marker. New registrations on runtime 0.6, including calls through older installed SQL, require a canonical absolute local root and NULL credentials. Existing bound read/write behavior is unchanged.
- `og_lake.create_catalog(server_name name, metadata_schema name, data_root text, credential_ref text)` requires installed 0.6, 0.7 or 0.8, an administrator and a configured private owner. It creates the 29 standard DuckLake 1.1 tables and seven initial records, server, host binding and OID registry in the caller transaction. The local root and schema/server must be unused; no directory or data file is created. Remote storage and credential-backed registration remain unavailable until their actual adapters are integrated.
- `CREATE FOREIGN TABLE ... SERVER ... OPTIONS (binding_mode 'attach', table_id '...')` creates persistent host UUID/relation/column bindings in the same DDL transaction. The explicit table ID is never inferred from a business name.
- Legacy installations and an unconfigured 0.5/0.6/0.7/0.8 database retain administrator-only lake access. Configured 0.5/0.6/0.7/0.8 uses the original host relation/column ACL and view `checkAsUser`; private metadata privileges are never granted to business users. Supported business types remain bool/int4/int8/text, without host CHECK/NOT NULL constraints.
- Metadata SQL uses explicit bound values, quoted identifiers, fixed pg_catalog name resolution and the executor's host snapshot. In a configured private-owner scope, opened relations, actual planning queries and final execution range tables are checked for namespace, exact registered identity and trusted physical objects. Required standard physical column types are checked before casts.
- Inline data is read by native TableAM and returned through ForeignScan's scan slot. Business quals remain in the native executor. ReScan retains the original lake snapshot and input binding; paths explicitly use dop=1 and Stream-worker execution is rejected.
- There may be at most one registered inline schema version. Its historical column IDs/names/order/types must exactly match the current definition; otherwise schema evolution is rejected. Metadata/inline row-compressed, partitioned, column-store, inherited-parent and nonpersistent storage forms are not enabled.
- Installed SQL before0.8 rejects any visible data file. SQL0.8 admits only the bounded local layout above; unsupported delete inputs reject the entire read. No partial inline fallback, logical-rowid/ctid substitution, flush, physical GC or SMP support is claimed.

## SQL0.9 local file version candidate

The new candidate retains the SQL0.8 rejection contract and requires installed
SQL0.9 for explicit logical row IDs, per-row birth snapshots and position
visibility. Standard inline deletion carriers and two/three-field Parquet
delete files are read at the fixed host/lake snapshot. Physical positions stay
separate from logical identities; all input footers and deletes are validated
before any business row. Deletion input is initially limited to 1048576
positions per binding, with at most4096 local data files. These bounds limit
input populations, not total process RSS.

Puffin, mapping, encryption, schema evolution, file DML/ANALYZE, native column
pruning, SMP, flush and remote durability remain unavailable. The provider C
ABI and existing catalog layout do not change. Header changes require an
official full rebuild. This candidate has not yet passed native acceptance;
the earlier0.8 safety fix af47c5b673 passed10 affected cases/323 assertions.

## SQL0.10 file-origin DML candidate

The new candidate requires installed SQL0.10, a configured private owner,
READ COMMITTED, supported primitive columns and a positive effective standard
inline limit. File-origin UPDATE/DELETE preserve logical identity and stage
only transaction-private after-images/tombstones. Native reads hide the old
file version before copying business Datums. Commit claims a standard snapshot,
rechecks source-file activity, writes standard file-position deletes and inline
final versions, and commits with ordinary host changes. The new data carrier,
if needed, and deterministic file-delete carrier are created only in the
claimed publication attempt. They have no host-table ownership dependency.
Attempt bindings are copied so rolled-back OIDs never enter the live journal.

Catalog/schema/table inline-limit precedence is checked at admission and
publication. This initial path rejects larger data/delete batches requiring
direct Parquet writes. No file is rewritten by DML. File ANALYZE, Puffin,
flush, physical GC, SMP and remote/RAC qualification remain open. This source
candidate has not yet been built or tested. SQL0.9 read qualification remains
tied to2c4f107321 (48cases/1621assertions plus separate identity diagnostics).

## Explicit pure-inline ANALYZE and planner estimates

Explicit single-node `ANALYZE` of a supported persistent pure-inline foreign table follows the native persistent-heap contract: run it as a top-level command outside an explicit transaction block. It uses the same TableAM visible-row reader as queries, fixes the command host/lake snapshot, retains at most the host-requested reservoir sample, and restores sample scan order for native correlation calculations. The query reader supports previous-command private writes, but the public ANALYZE entry cannot analyze a private write set inside BEGIN. The host computes column statistics on the business relation OID and updates its own `pg_statistic` and `pg_class`; no DuckLake snapshot or standard statistics are published by ANALYZE. The d252 full build and scoped statistics tests passed; corrected concurrency/cancellation tests and this candidate need separate acceptance.

Maintenance admission follows native owner/database-owner/VACUUM privileges, independently of SELECT. RLS/masking and unsupported storage layouts reject before partial statistics are written. Positive statistics targets are required. File sampling, partition/inheritance sampling, percentage targets, automatic scheduling and SMP analysis remain unavailable. Native empty-sample behavior can retain old column distributions; `pg_class` estimates use native inplace updates and are not promised to roll back byte-for-byte with an outer transaction.

The planner consumes the business relation's native MCV/NDV/histogram estimates and reports its fallback source in EXPLAIN. It separates full carrier/TOAST I/O and version filtering from result-row costs. This is an uncalibrated pure-inline baseline in host cost units, not a remote file cost model. Missing standard table statistics are a valid state before the first data write. Page/width fallback, especially with TOAST, is approximate; transaction-private rows may not be reflected in planning estimates. Cardinality estimates never decide row visibility.

## Ordinary-user access configuration

The administrator explicitly creates a restricted `NOLOGIN NOINHERIT` role with no administrative attributes and no memberships in either direction. Standard metadata and inline carrier ownership must be prepared explicitly before `og_lake.configure_internal_owner(name)`; the function does not create a cluster role, grant business access or commit internally. It registers the role OID once and transfers only the fixed private host control objects and namespace in the caller transaction. The policy table remains administrator-owned. A different owner cannot replace an existing registration silently.

Business users receive the required business schema, server and table/column privileges. Existing-object binding DDL and catalog attachment remain administrative; the 0.7 business CREATE path is the narrow ordinary-owner exception described above. Original executor permission bitmaps and view subjects are preserved; the hidden modification locator is not an implicit request for SELECT on every user column. Prepared execution and later statements recheck authorization. Previously authorized writes publish at PRE_COMMIT even if the caller changed role afterward, while new statements use their current host authorization.

Only fixed metadata SQL enters the short internal identity scope. Success and error paths restore the immediate saved user/security flags and scope before fallible SPI/GUC cleanup. Metadata and carriers must retain trusted ownership, private table/column ACLs, builtin types/indexes and the fixed control constraints. Their exact relation OIDs are recorded during administrator attachment/configuration; ordinary reads cannot adopt same-name replacements. User expressions run outside this identity scope. RLS, security-barrier and enabled masking combinations require additional support and are explicitly rejected by this slice.

The scoped 0.5 permissions evidence above does not prove every security combination or qualify 0.6. In 0.6, registration/configuration/unbinding use one catalog-binding relation mutex. Registry insertion uses RowExclusiveLock so ordinary registry readers can continue. First ownership handover still requires AccessExclusiveLock; failure to acquire all required control locks returns 55P03 before handover, for transaction/savepoint rollback and retry. This does not promise that arbitrary user DDL lock ordering is deadlock-free.

## Bounded INSERT implementation

The first write slice supports VALUES, INSERT SELECT and business-column RETURNING into an existing pure-inline carrier. A genuine never-inserted carrier may have no table/column statistics. After winning snapshot publication, the candidate proves that all retained carrier rows, file history and column statistics are empty, seeds the allocator in the same attempt, and publishes first-batch bounds with permanent IDs. Used carriers with missing statistics still reject with XX001; NULL known-state fields are never silently reset to zero. It requires UTF8, READ COMMITTED and one writable catalog per user transaction. Binding DDL and lake writes cannot share a transaction. User-defined triggers/rules/default expressions, non-nullable business columns, unsupported statistics options and unknown file-size statistics are rejected explicitly.

The first lake access fixes the catalog's business snapshot for the transaction; ordinary heap tables retain their host isolation semantics. Previous commands' private inserts are visible to lake scans. Current-command inserts cannot feed their own INSERT SELECT. Savepoint rollback removes child writes, restores the charged write-set budget and releases child metadata state after host scan cleanup. The initial private write-set limit is 64 MiB; spill is not implemented.

PRE_COMMIT publishes the existing write set in the user's transaction: stabilize metadata options, read fresh publication state, validate changesets, claim a standard snapshot, allocate permanent row IDs once per table, insert inline rows, update statistics and record standard changes. Failed snapshot claims roll back only their internal attempt and retry a bounded number of times; other errors abort the user transaction. No internal COMMIT makes data visible early. Permanent IDs stay below the fixed reference's `10^18` transaction-local range.

New lake work is rejected throughout PRE_COMMIT/PRE_PREPARE callback traversal, including first access from another callback. The internal superuser-only `test_precommit_insert` diagnostic performs a real SPI INSERT to test this guard. Its source is counted separately from product code. Host capability JSON describes the runtime binary and names the minimum installed SQL version; `inline_insert` is distinct from complete `writes` support.

## Native inline UPDATE/DELETE implementation

The new single-target row path uses an internal locator placeholder carried through the real ForeignScan slot, alongside the old business row for DELETE RETURNING. The final plan validator rejects any executable locator marker that was not replaced by a slot reference. The 112-byte locator encodes explicit fields, including the exact inline source/version or transaction-local version token; it never serializes a C struct or substitutes a heap ctid. File and partition locators remain rejected.

Transaction-private after-images and tombstones form immutable version chains. Each scan retains its command boundary; subsequent commands see the latest prior version. A statement suppresses repeated hits on the same full source version, while different logical rows sharing a business key remain distinct. Savepoint rollback restores parent chain heads and releases child versions and their charged budget.

Publication keeps existing logical row IDs, ends each committed source version once and writes only final surviving after-images. New identity allocation, added physical versions and ended origins use separate counts. INSERT followed by DELETE in the same transaction has no net lake publication; UPDATE followed by DELETE only ends the committed origin. Retained SQL0.10 and earlier keep the conservative same-table INSERT versus UPDATE/DELETE-attempt rule, including zero-match attempts when there is net lake publication. Attempts follow savepoint RELEASE/ROLLBACK. The SQL0.11 private-owner native RC candidate instead revalidates exact inline origins or file-position deletion markers after winning the standard snapshot PK, allowing independent inlined INSERT/DELETE changes while rejecting a changed source version. Schema, unknown structural changes and flush relocation still reject with40001. This finer native MVCC adaptation is distinct from the pinned DuckLake table-wide inline conflict rule and does not open external concurrent writers. The new row-merge candidate requires deterministic overlap/disjoint-row, history, rollback and timed mixed-write acceptance.

Revision 572e04851e has passing scoped tests for these contracts, including independent memory observations during repeated short-row savepoint rollback. These passes do not complete N4 or qualify later revisions automatically. The write-set budget does not cap all allocator/hash overhead. Modifying CTEs remain unsupported, including when a SELECT/INSERT parent contains a lake write.

Column bounds remain conservative after deletion. Unknown bounds may recover only when the supported new batch has complete bounds and the predecessor's visible inline data has no non-NULL value. The check includes source versions ended by this commit and excludes versions inserted by this commit; zero, false and empty text count as real values. This rule follows the fixed reference's NULL-only recovery and does not infer all-NULL data from `contains_null=true` alone.

## Host and lifecycle

Host status uses `session_id` and `logical_session_id` following the native pg_current_sessid rule: stable pool session ID for pool workers, native backend thread ID in dedicated mode. `thread_id` and `session_mode` expose the distinction explicitly.

Maintenance ownership is a database-scoped internal advisory session lock. Catalog guards use a separate internal key space, are cleaned on transaction/subtransaction completion, and are protected from SQL advisory unlock-all and DISCARD ALL. Normal readers wait through the host lock manager with cancellation/timeouts; diagnostic APIs still use nonblocking try. See [host lifecycle evidence](docs/host_lifecycle.md).

The extension deletion scope is set only while an existing dependency deletion set includes the og_lake extension itself. PG_TRY/PG_CATCH restores the previous scope. Only in this scope may callbacks skip already-deleted binding tables; ordinary foreign-table DROP requires its binding and removes it transactionally. Standard lake metadata/data is retained.

## Explicit remaining acceptance

The historical and seven scoped CREATE results do not complete this implementation. Current-source legacy inline/locator/permission/statistics regressions, concurrent CREATE claims, all four CREATE column types, special identifier cases and further lifecycle faults require their own evidence; unsupported storage layouts remain open. Earlier statistics, compatibility and lifecycle passes are tied to their exact source and binary. Complete N4 remains open. The complete lake G1 gate needs a real SDK for Parquet/mixed/position-delete layouts. Physical GC additionally needs live-reader/SDK/Stream drain and old-RR retirement-horizon validation; these locks alone do not prove it.

## Source attribution

Standard metadata and inline version rules reference MIT-licensed DuckLake `0aaa8e10d4adfe2ca37da77134e991d20106d0e7`: `src/metadata_manager/ducklake_metadata_manager_v1_1.cpp`, `src/storage/ducklake_metadata_manager.cpp` (`ReadInlinedData`, `GetInlinedTableColumnsSql`), and `src/include/storage/ducklake_metadata_manager.hpp`. The code here is an openGauss-native adaptation; it does not link those DuckDB physical operators. The matching reference environment and licenses are tracked in the workspace source lock.

The 29 DDL literals in `lake_catalog_ddl.inc` are mechanically extracted from that pin's explicit 1.1 virtual dispatch and retain its MIT license. They are not a 1.0-to-1.1 migration. Native initialization generates a real UUIDv4 and truthful openGauss producer string; reference compatibility must preserve these values during transport and be tested against the actual fixed reference engine.

Publication and inline statistics additionally follow that pin's `ducklake_transaction_state.cpp`, `ducklake_inline_data.cpp`, `ducklake_metadata_manager.cpp` and `common/index.hpp`. Host integration also references pg_ducklake `0daf5bbc1020dad4b531b842ad6f82e68287a845` native inline publication; its format-1.0/implicit-transaction restrictions are not treated as a format-1.1 transaction implementation. The adaptation uses native openGauss FDW callbacks, snapshots, resource owners and SPI.

`lake_create_publish.cpp` adapts the fixed reference definition, path and changeset rules; its upstream MIT notice is retained in `LICENSE.ducklake`. The transient native object journal and private owner DDL bridge are host implementation, not a new persistent DuckLake protocol.

N5 preparation adds native `lake_arrow_builder` buffers for boolean/int4/int8/text, including explicit NULLs and borrowed synchronous Arrow C Data writes. Raw TOAST lengths are checked before fetching an over-budget row. The file-provider ABI1 optionally appends serialized footer length behind capability/size checks; read paths retain the original footer prefix contract. Revision19be9678dc passed its official full build, nine affected native cases (360 assertions), and actual native Arrow component checks (39 each for heap/Ustore), counted separately. There is still no product flush entry, catalog publication, maintenance cleanup or remote/RAC qualification in this preparation component.

Transaction candidate preparation now adds an optional lifecycle ABI2 entry while preserving ABI1. Explicit abort discards even a finished unpublished candidate; an inode anchor prevents removal of an unrelated replacement. Session-owned handles are resolved after ordinary resource/lock release, adopted on savepoint release and aborted on savepoint rollback. Preparation is bounded to 16 candidates per transaction and is prohibited in pre-commit callbacks. Revision815fde00cc passed its official full, 37 real candidate lifecycle checks, 9 old-provider refusal checks and 15 affected native cases (565 assertions), counted separately. They provide no flush SQL entry or catalog publication, and do not qualify crash/orphan GC, remote durability or RAC.

Revision1e07bb4323 passed official full, nine scoped native flush cases (360 checks), all66 retained read/DML cases (2466 assertions), and the corrected self-contained post-inline-DELETE cancellation test (21 checks). SQL0.11 provides bounded inline historical-data flush. It prepares local immutable data plus ended-version position deletions with original logical row IDs and birth/end snapshots before publication. At pre-commit it claims a fresh catalog snapshot, rejects conflicting changes, and registers files/statistics together with deleting the exact transferred inline rows in the same host transaction. Ordinary inserts after the input cutoff remain inline. DML/CREATE/binding DDL and repeated flush cannot mix with this maintenance operation; user savepoints adopt or abort its pending operation. The implementation is administrator-only, READ COMMITTED, four primitive types, one data output and optional historical-delete output, at most262144 physical versions per call. Existing inline file-delete carriers remain in place; file-delete consolidation, larger multi-output flush, crash orphan GC, background scheduling, remote and RAC qualification remain open. This source is not a completed N5 or commercial release.

A subsequent real savepoint retry test reproduced failed-preparation memory retention on1e07: transaction-context capacity grew from630784 to1353952 bytes after four rejected oversized-row flushes. A new outer preparation guard now owns the entire operation context and deletes it on every ERROR, including errors before scan construction; candidate writers still resolve after native lock release. Revisionbec119 passed its official incremental build, the identical23-check memory test, nine bounded flush cases360, deeper cancellation21 and15 affected regressions565. Original negative evidence is retained in the public project tree. The source does not add file-delete consolidation, large multi-output flush, crash GC or commercial qualification.

The next source extends SQL0.11 flush to existing inline file-position deletions. It reuses the reader's exact Parquet deletion decoder, merges positions with their earliest original deletion snapshot, writes one replacement deletion file per affected data file, and atomically registers replacements and clears transferred inline marks. Retired deletion metadata is removed and its catalog-relative path is entered in standard ducklake_files_scheduled_for_deletion; old physical files are retained for readers and deferred GC. It emits standard deleted_from_table changes alongside inline_flush when data versions also move. Preparation is capped by16 candidate outputs and the existing deletion-position budget; this is not unbounded multi-file maintenance or physical GC. Revision81de passed official full with98 fixed native inputs, nineteen scoped flush components813 and fresh full read/DML regressions66/2466. Three local SIGKILL WAL recovery cases102 also passed, but each uncommitted crash leaves two unregistered candidates: safe orphan GC remains required. These are local bounded contracts, not completed N5/remote/RAC/commercial qualification.

The next source replaces whole-input budget refusal with a bounded identity prefix: native TableAM rows are ordered by logical(row_id,birth), using the host spillable sort under at most16MiB and the smaller session work_mem. Each call writes up to262144 versions and the publication DELETE is restricted to exactly that identity interval and business cutoff; remaining versions stay inline. One extra key detects a duplicated boundary identity. File-deletion consolidation similarly transfers at most the available16 output populations and leaves others untouched. Standard format and original IDs/snapshots stay unchanged. Inline publication also uses parameterized INSERT batches of at most512 rows and8192 parameters inside the same host transaction. This source requires official full plus actual large/prefix-boundary, NULL, rollback, history, concurrency and complete read/DML regression validation; no new runtime acceptance is claimed yet.

Revision242db passed official full,23 bounded batch/partial-flush components909, fresh full read/DML regressions66/2466, and3 actual local SIGKILL recovery cases102. Two scoped lifecycle-delay tests then reproduced premature catalog-guard release: the real SDK still owned candidates while top-level/savepoint abort had already dropped shared protection. They forwarded all Parquet operations and the actual abort to the immutable genuine provider; that provider was restored with an official restart afterward. The next CPP-only repair keeps session guards through late candidate close/abort, releases only child-owned guards after subabort cleanup, and leaves maintenance claims independent. Official incremental plus the same blocked-real-SDK tests and affected/full regressions are required. Physical GC, large single rows, background scheduling and remote/RAC qualification remain incomplete.


The next native RC flush candidate reconciles committed post-cutoff inline ends after winning the standard snapshot PK. It verifies the exact prepared identity interval, assigns physical positions by the same ordered(row_id,birth) keys as the completed Parquet writer, and records each newly ended position in the standard inlined-delete carrier using its original end snapshot. Those selected versions can then leave inline atomically; later-born versions and later file-origin deletion marks remain inline. Structural changes, competing flushes and old DML locators still conflict. No SDK I/O is added under publication, no new durable format or row-ID remapping is introduced. This is an intentional native adaptation beyond the pinned DuckLake conservative flush conflict policy. Real prepared-file/late-update, partial-delete plus late-marker history, mixed-source, cancellation and sustained-write qualification is required; source alone is not acceptance.

## Optional synchronous local file deletion

The provider additionally exposes optional maintenance ABI3. It anchors local
path components without following symbolic links, removes only a relative
regular Parquet object, synchronizes the containing directory, and supports
retry after an object is already absent. ABI1 reads/writes and ABI2 candidate
abort retain their existing layouts. The native loader accepts older providers
for their original operations and refuses physical cleanup without ABI3.

This component does not itself authorize deletion. Native catalog retirement,
safe MVCC watermark, retention/backup checks and catalog-exclusive protection
must all be satisfied by the maintenance caller. It does not enable automatic
GC, arbitrary orphan removal, remote deletion or RAC qualification.


## Native automatic flush implementation (not yet qualified)

The dedicated lake launcher and worker use the host postmaster lifecycle and
background PGPROC pool. The launcher rotates databases and relation cursors;
committed writes provide bounded scheduling hints, while the SQL directory
remains authoritative after restart. A failed database uses a30-second error
cooldown in the bounded launcher cache; new commit hints, including overflow
rediscovery, cannot clear an active error cooldown. Hints may still invalidate
the empty/unsupported-catalog discovery delay. This is instance scheduling
state, not a persistent or cross-instance task lease. Workers use READ COMMITTED, a session
maintenance claim, native TableAM pending-version sampling and the same
manual flush entry point, with independent bounded transactions.

`enable_lake_maintenance` defaults to off and requires restart. Scheduling
defaults are `lake_flush_naptime=1000ms`, `lake_flush_min_rows=100000`,
`lake_flush_max_delay=10000ms`, `lake_flush_worker_budget=30000ms`, and
`lake_flush_max_batches=8`. These are instance resource/scheduling defaults;
they do not change DuckLake table format or persistent row identities.
Automatic physical GC is not enabled. Full compilation and real background
startup, dispatch, retry, cancellation, shutdown and data tests are required
before the automatic flush path is qualified.

### Persistent option slice (SQL 0.12)

`og_lake.set_option('biz.t'::regclass, 'data_inlining_row_limit', '10000')`
requires the native table owner; the `name` overload takes a registered server
name and requires an administrator. Both update the standard
`ducklake_metadata` rows in the caller transaction, without creating a snapshot
or committing internally. Table overrides take precedence over schema/global
settings. The first slice accepts positive decimal int64 values only. Zero,
NULL/reset, schema scope, other keys and transaction-local tables are refused.
Direct file writes are still unavailable; this option is not a general hard
transaction-row limit for existing pure-inline carriers. It controls the
existing file-DML inline admission and the standard positive inlining policy.

Native option writers serialize on standard metadata; readers remain unlocked
by this writer lock. DML publication holds the existing metadata ShareLock and
revalidates the latest option before committing. Waited option writers also
read a fresh RC snapshot before deciding INSERT versus UPDATE. Lake writes,
CREATE and flush cannot mix with configuration in either order in one host
transaction; rollback/savepoints use the existing DDL journal. Catalog-wide
enablement conservatively requires live native primitive columns without
reserved inline names. The host registration lock is held to transaction end;
configuration transactions should be short. This is a functional development
slice, not completed DDL/configuration or commercial qualification.

### Transactional permanent-table RENAME slice (SQL 0.13)

`ALTER FOREIGN TABLE biz.orders RENAME TO orders_new` journals a permanent lake
table rename while the host holds its normal AccessExclusiveLock. At final
publication it ends the previous standard table definition, appends the new
name with the same table ID/UUID/path, records a schema version and updates
the existing host binding version. Row IDs, columns, carriers, file references
and data bytes remain unchanged. The existing standard snapshot PK arbitrates
publication with bounded retries; preparation does not take that PK. Native
reads and the fixed independent DuckLake must verify both inline/file data and
retained history before this slice is considered functionally accepted.

Up to64 rename events in one catalog can be journaled, with the final name per
relation published once; savepoint abort removes only child events and host
DDL rollback restores the corresponding host names. Final names are checked
using the reference ASCII fold across tables and views, including batch swaps.
Mixed lake DML/CREATE/configuration/binding changes/flush are rejected in either
order. Host-schema movement, column ALTER, transaction-local tables and the
compatibility `RENAME TABLE` direct catalog rewrite syntax stay unavailable.
Unknown concurrent structural/flush changes are conservatively rejected.
Business heap changes share the caller commit/rollback. No SDK I/O, physical
file rename or GC runs in rename publication. The old carrier schema version
remains valid because its columns are unchanged; this native adaptation needs
actual pinned-reference read/continuation tests rather than format assumptions.

### Derived index definition registry (SQL0.14 candidate)

`og_lake.create_lake_index('biz.t'::regclass,'ix'::name,
'{"version":1,"kind":"scalar","method":"btree","column":"id"}'::jsonb)`
registers a definition for an integer/bigint column. The host owner/admin is
required. Values are normalized to permanent column/type/collation identities,
then salted with the table UUID and SHA256-hashed. Unsupported algorithms,
keys, expressions, types and transaction-local tables fail before insertion.

The three private extension tables `index_definition`, `index_segment` and
`index_coverage` use host WAL/MVCC and do not change DuckLake metadata or claim
a lake snapshot. Register/drop cannot mix with lake DML/rename/CREATE/flush.
Ordinary heap writes commit/rollback with the definition. No builder or indexed
query path is implemented by this slice. `og_lake.lake_index_status(uuid)`
explicitly returns `usable=false`; ACTIVE denotes a definition, not a READY
search artifact. Normal queries continue their existing complete scan.

`og_lake.drop_lake_index(uuid)` retires coverage, moves BUILDING to FAILED and
READY to RETIRED, and marks the definition DROPPING. No files are deleted.
Partial unique indexes reserve names only for ACTIVE definitions and prevent
multiple active coverages for a source/generation. Composite FK protects the
coverage's segment/index/generation tuple. Host unbinding first retires its
definitions, then FK SET NULL detaches them while retaining stable table UUID
and artifact metadata for later protected cleanup. Non-admin status/drop
requires a still-bound host table that the caller owns.

New index tables retain the same private-owner trust policy. Upgrading a
configured0.13 database first validates empty new extension-owned tables,
transfers them to the registered owner, then installs the cross-domain FK and
validates the complete graph. Fresh owner handover moves all eight controls
together. Validators admit only the two exact builtin partial predicates,
fixed CHECK/PK/UNIQUE/FK layouts and their builtin RI actions. Internal upgrade
helpers require an administrator inside the extension script. This candidate
is source only until its official Debug full and actual tests pass.


SQL0.15 candidate (unverified): `build_lake_index(uuid)` builds an ordinary
nonunique host B-tree over the existing heap inline carrier using DefineIndex.
Its OID is transactionally recorded in the private definition; registration
alone still does not build an index. The planner compares simple integer/bigint
comparison paths in native cost units; Const/Param arguments are evaluated again
on rescan. Builtin smallint/integer/bigint RHS operands retain the operator's
right-hand type as ScanKey subtype, following native ExecIndexBuildScanKeys;
keys are never narrowed to the column type. Integer IN/equality-ANY Const/Param
arrays reuse native SK_SEARCHARRAY; the original predicate still rechecks
files and private rows. Native B-tree handles duplicate/NULL/empty elements;
ALL and other array operators retain complete scan fallback. IS NULL and
IS NOT NULL use native SK_SEARCHNULL/SK_SEARCHNOTNULL, with no comparison
function. The private plan layout is version5 to carry the null mode; no
persistent file/catalog or SDK format changes. Cost includes estimated array
probes. Numeric/coercion expressions still use full scanning. Original quals, the fixed lake/host snapshot, begin/end checks,
transaction-private after-images and complete file scanning remain mandatory.
Cached paths recheck current definition/physical OID and the native HOT build
horizon, falling back before output when no longer applicable. The preceding
d817 snapshot passed eight scoped Debug modes238 and actual/private-overlay21;
8385 cross-integer source passed its nine scoped Debug cases352. The subsequent
array and NULL-key sources still require their own official build and tests. File indexes, Ustore indexed access,
DiskANN and fusion remain unfinished; no performance or commercial claim.
Physical inline B-trees are retained on definition retirement for later protected
cleanup; they are not shared-store file-index segments or DuckLake data files.


SQL0.16 root/component baseline3ce5 passed official Debug full and20 scoped cases547; the next file-builder candidate is native unverified: the catalog-administrator
`set_option(server_name::name,'index_root',path)` stores an independently
provisioned canonical directory in the nullable ninth catalog_binding column.
Data and index roots cannot overlap in either registration direction. Retained
index segments pin a configured root; setting it does not relocate files,
allocate a business snapshot/row ID, create directories or commit internally.
Configured0.15 upgrades add the column without changing private ownership.
Other persistent options and legacy SQL contracts retain their previous gates.

The portable scalar format component accepts already sorted complete physical
rows from the host, following Lance's two-level lookup/data organization through
our Parquet C ABI. Data pages hold integer keys and (file ID, original position);
lookup pages hold bounds, NULL populations and page digests. Canonical SHA256
binds lookup, leaves, key type and host context. Candidate collection is bounded
and completes validation before returning positions. Current component tests
include genuine provider files, a separate DuckDB SQL oracle, cross-process
reopen, corruption, ordered altered postings, cancellation and posting budgets.
The next synchronous file builder projects every physical key, including ended
and future lake versions, through native spillable tuplesort. It prepares at
most eight immutable two-file segments within the existing sixteen-candidate
transaction limit, publishes guarded READY plus full source coverage in the
same user transaction, and never changes business snapshots/row IDs. A repeat
call skips unchanged READY sources; unbuilt files remain ordinary scan inputs.
The first implementation holds the existing registration/definition locks
through I/O, serializing administrative DDL, but acquires inline carrier build
locks only afterwards. Background short-transaction build scheduling remains
unfinished. No implicit commits are introduced.

Optional ABI1 footer/batch suffixes provide opaque immutable-object versions;
limits and the function table remain unchanged. Old providers remain readable,
but cannot build pruning coverage. The local version binds the opened object's
stat identity, not a content checksum; each raw key batch and the final source
footer must match it. The coverage source_checksum column explicitly stores
object-version-v1:<token>, while the source manifest hash additionally binds
path, size, physical row population and schema mapping. The schema mapping uses
stable field IDs/types, not names. Candidates remain session-owned through late
abort/close and share the existing catalog protection. index_root must already
be committed; changing roots requires empty old/new directories so rollback or
crash-created orphan directories cannot be silently forgotten.

The preceding5644 build source passed its official Debug incremental after the
4368 full-link TableAM fix,131 input checks and24 scoped cases659, including
physical build/publication/abort/version/permissions and affected regressions.
That evidence does not qualify the subsequent query source below. Ustore indexed
access, DiskANN and commercial acceptance remain unfinished. host_status conservatively
keeps lake_index_file_build=false until the new native build path is qualified.

File scalar query candidate (not yet compiled or qualified): the planner retains
complete scanning and estimates only metadata-matched covered files in native
cost units. Uncovered file I/O stays in the baseline; lookup/leaf and conservative
random row-group fetch work is charged explicitly. enable_indexscan=off keeps
complete scanning. Cost and remote calibration remain future performance work.
At execution the adapter rechecks definition/table/field mapping, source metadata,
opaque object version, artifact root/context and content digests. It collects
bounded complete postings before any business output, then reads only intersecting
source row groups and validates each original physical position. Index artifact
errors or budget failure leave that source fully scanned; cancellation propagates.
Existing host/lake snapshot checks, all deletion markers, transaction-private
versions and every original SQL qual remain mandatory. File and inline access
are independent: absent/unusable native inline indexes retain TableAM scanning.
Rescan rebuilds parameter candidates and releases prior C++ results through an
owned query memory context. EXPLAIN distinguishes verified coverage from complete
file fallback; planned paths alone are not execution evidence. Official Debug full
and actual candidate/fallback/visibility/rescan/lifetime cases remain required.

Ustore inline-index candidate (separate uncompiled follow-up): native DefineIndex
selects ubtree for an actual Ustore carrier, keeps fixed pg_catalog integer
operator classes/family, and records the existing physical OID in the same user
transaction. Host control indexes still require B-tree; reviewed Ustore data
relations require builtin UB-tree. No native tree algorithm is copied or replaced.
The reader follows nodeIndexscan.cpp's IndexGetnextSlot Ustore path, owning and
clearing the Ustore slot before scan/context teardown; heap keeps its existing
borrowed-tuple path. Generic integer/null/array ScanKeys and retained lake
visibility still apply. The conservative CSN horizon remains: native plancat's
own-current-XID RC shortcut does not prove an older retained lake host snapshot.
EXPLAIN labels UB-tree separately. This follow-up changes CPP only and requires
its own official build, real Ustore plans/data/undo/rescans/rollback/lifetime and
affected tests; it is not included in the preceding builder/query evidence.


## Isolated floating/list codec prerequisite

The candidate provider admits IEEE float32/float64 and standard one-level
LIST<FLOAT32> behind explicit capabilities. Logical list field IDs are read
from their parent Parquet groups, not the FLOAT element leaves. Optional ABI4
writer_open takes separately supplied catalog element IDs/nullability; ABI1
writer_open refuses list columns. ABI1/2/3 layouts, field-array stride, limits
and function tables do not change. This does not itself admit native lake SQL
float/vector types, validate vector dimensions, or provide DiskANN.

Nested C Data descriptors are cloned through the bounded root/list/element
levels; caller buffers and release callbacks remain borrowed only during
append. Empty lists, NULL lists, NULL elements, element offsets, lossless IEEE
bits, physical row positions and independently selected row groups are retained.
The host vector adapter must separately enforce vector(n), finite elements and
operator rules. Standard DuckLake PostgreSQL nested inline storage uses text;
it must not be replaced with an incompatible PostgreSQL real[] carrier.

Isolated real-provider tests cover genuine DuckLake list files, three codecs,
independent DuckDB schema/value decoding, incorrect child-ID/collision rejection,
malformed offsets, unsupported child types, cancellation, forbidden NULL elements,
and reader-close/batch-release order. Old-header scalar reader/writer consumers
remain separate compatibility tests. Native installation and SQL qualification
are deliberately separate from these component results.


The following native candidate centralizes six scalar mappings in lake_types.h,
adding float4/float8 to ordinary CREATE, typed inline values, Parquet decoding
and bounded Arrow flush batches. Float file access requires the provider's
explicit capability. Catalog float min/max are conservatively invalidated on
writes, including existing external bounds, until their comparison/NaN policy
is qualified; native typed ANALYZE remains the statistics path. This source
candidate requires a fresh official Debug full build and native SQL/flush,
transaction, statistics and affected regression evidence before admission.
It does not yet admit vector or lists into native business tables.


A subsequent isolated provider candidate adds LIST parent/element IDs and physical
child nullability in a capability/struct-size protected footer suffix. The field
array stride and ABI1/2/3/4 function tables remain unchanged; absent IDs are -1.
Native object-version guards use the end of their own optional member, rather
than the size of the entire evolving footer, preserving older SDK prefixes.
Component checks exercise real DuckLake IDs, required/nullable and absent element
IDs, old-prefix clients, old scalar writers/readers and exact IEEE payloads.
These results do not qualify the native guard change, admit vector business
columns, or install this candidate into the running instance.
