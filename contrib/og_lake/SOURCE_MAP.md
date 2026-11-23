> Current integration note: the CREATE module below is already integrated by
> revision 2cd0a21aad. Its retained integration rules are historical context,
> not instructions to overwrite the active module. Native0.8 file access adds
> lake_file_catalog.cpp (standard directory selection), lake_file_reader.cpp
> (native materialization) and lake_file_sdk.cpp (C ABI loading/ownership).
> See README.md and sdk/README.md for the current gate and dependency boundary.

# 业务 CREATE：标准目录发布辅助候选

状态：仅 `/tmp` 源码候选，未正式应用、未编译、未执行 SQL。对应产品基线 `93fb65c12af4a1bc99c07eb98faf3d83221d6cc9`。`lake_create_publish.h/.cpp` 是与 native 作者已协商的窄接口，不是第二套事务日志或新持久协议。

## 1. 真实源码映射

固定 DuckLake：`0aaa8e10d4adfe2ca37da77134e991d20106d0e7`，源码 `/tmp/ducklake_rowid_review_20261104`；DuckDB `d54cbbe3df17e8e5ce68312f483b4155836112aa`，源码 `.reference/opengauss-native-lake/ducklake/duckdb`。上游 MIT 许可原文保存在 `LICENSE.ducklake`。以下为核对依据，不声称直接复制上游函数即可移植宿主事务。

| 协议点 | 固定源码 | 本候选落点 |
|---|---|---|
| 创建使用局部对象，flat 列 ID 从1开始 | `ducklake_schema_entry.cpp:113–137` | descriptor 独立于永久ID；检查column_id/output_attnum为1..N |
| 永久 table ID 消费next_catalog_id，父schema在commit映射 | `ducklake_transaction.cpp:1359–1378` | `LakePlanCreateAttempt` 仅在尝试上下文生成独立映射；当前片父schema必须已提交，不分配新schemaID |
| 模式有变化只增加一次schema_version | `ducklake_transaction_state.cpp:2795–2799` | K>0时V+1，K=0不增加 |
| 新表目录8字段及其新列 | `ducklake_metadata_manager.cpp:3531–3553` | 参数化 `ducklake_table` INSERT，随后每列参数化INSERT |
| 列13字段与无默认值编码 | 同文件 `3324–3368`；`ducklake_table_entry.cpp:991–1005` | initial_default=NULL；default_value文本`NULL`；literal/duckdb；nullable=true；parent=NULL |
| inline注册保存实际物理名，不要求解析名得到ID | `ducklake_metadata_manager.cpp:3474–3484` | actual UUID carrier名通过参数写入；OID与注册信任由宿主验证 |
| CREATE会按选项创建inline，禁用时可无carrier | 同文件 `3555–3604`；`transaction_state.cpp:1925–1934、2361` | 正常DDL阶段由caller建物理carrier；本模块仅在实际OID存在时登记，不忽略禁用选项 |
| 各新表schema_versions | `ducklake_transaction_state.cpp:2475–2494`；manager `6123–6133` | 写 `(commit_snapshot, commit_schema_version, permanent_table_id)` |
| 新表stats只在首次有数据时产生 | manager `5404`；state `2392` | 本模块不写table/column stats，交既有首次INSERT路径；纯CREATE不制造0行stats |
| quoted schema.table changeset/各组次序 | state `421–486`；schema_entry `64–69` | `LakeAppendCreatedTableChange` 始终加双引号，内部双引号翻倍；调用方先created组再insert组再delete组 |
| 变化集不能按逗号裸拆 | `ducklake_transaction_changes.cpp:81–124`；util `39–87`；DuckDB `string_util.cpp:226` | `LakeNextPublicationChange` 解析带引号的完整identifier，逗号/点/冒号位于引号内不作分隔 |
| 名称ASCII大小写不敏感 | DuckDB `string_util.cpp:495–512` | `LakeIdentifierEquals` 只折叠ASCII A..Z；不采用宿主locale lower() |
| 名称生成目录路径：合规ASCII字母数字/_/-且<=255，否则UUID | catalog `379–401`；util头 `MAX_FILE_NAME_LENGTH=255` | `LakeCreateRelativePath`；首片本地POSIX `/`分隔，明确不冒充对象存储SDK实现 |
| CREATE→DROP移除局部new_tables，不登记持久drop | transaction `2644–2671` | caller只向helper传存活pending对象；本模块没有DROP编码，也不创建取消对象的ID |
| 每次失败尝试重建commit_state/映射 | state `2783–2858` | attempt数组/copy/SPI数据均在internal-subtransaction尝试上下文，绝不改journal或业务binding |

## 2. 已冻结接口与职责

`LakeCreateDefinition` 是 authoritative pending journal 的只读投影：binding指针、已有schema_id/uuid/name、表名、相对path、实际inline_name。binding身份保持LOCAL；helper从不将其身份当永久table_id。现有LakeInlineColumn字段沿用不变。

`LakeCreateAttempt` 单独携带永久table_id、发布snapshot及schema_version。`LakePlanCreateAttempt` 按caller提供的稳定顺序生成，`next_catalog_id + K`只计算不写目录、不取行锁。最终永久ID不得达到固定参考的局部保留区 `10^18`；独占高水位可以等于该界。UUID/name/carrier不随重试变化，永久ID可以随head变化重分配。

`LakeValidateCreateDefinition` 使用发布尝试的新合法宿主snapshot核对父schema的ID/UUID/名字/顶层属性，以及此schema下当前可见table/view名。初次CREATE查重的42P07由caller在业务命令阶段执行；本函数只用于发布重验，遇并发同名/父变化报40001。合法但当前切片不支持的changeset保守拒绝，不自动refresh业务快照。

`LakeWriteCreateDefinition` 只写标准4类记录，不生成snapshot、不写changeset行、不创建宿主对象、不提交。调用前必须已成功争得本次snapshot；发生错误由主publisher回滚整个internal subtransaction，最终失败必须传播到用户事务。

`LakeNextPublicationChange` 支持created_table（当前顶层schema）、inlined_insert、inlined_delete的语法。**能解析不等于允许合并**：保留SQL0.10及更早接口的整表UPDATE/DELETE尝试冲突规则。当前SQL0.11私有owner、原生封闭目录且RC的普通DML发布可以接受later inlined_insert／inlined_delete：赢得snapshot主键后，既有精确inline源版本UPDATE RETURNING数量或file_id／物理位置删除标记检查仍是强制验证，同一源版本竞争返回40001。flush发布不启用该放行，仍拒绝读后UPDATE／DELETE及结构变化。此处是利用原生SQL MVCC作更细粒度验证的适配，区别于固定DuckLake参考在transaction_state.cpp:228–324的整表inlined规则；不开放外部并发写入。对合法created_table需以完整schema/table名字同pending表作ASCII比较，同名40001，不同名可继续；已有表写事务可忽略无关created_table但仍验证自身绑定/列/UUID。未知组、nested-schema名字保守40001；已知语法破损报数据损坏。

## 3. publisher 集成顺序（具体约束）

1. 从主journal收集存活CREATE对象及有净行变更的既存表。丢弃已取消/保存点撤销对象。无存活CREATE且无净行时按原流程返回，不生成空snapshot。
2. 沿已有metadata配置锁→fresh publication snapshot→读取head `S,V,C,F`；固定业务snapshot不动。检查later changes，schema/name与所有既存表绑定。Schema/table/column名全部来自已准入的descriptor，SQL仍参数化。
3. K个新表调用 `LakePlanCreateAttempt` 得到 `[C,C+K)` 与`V+(K>0)`。父schema沿已提交ID，不消耗C；列1..N也不消耗C。`next_file_id=F+净最终inline新版本非空的表数`；纯CREATE与纯DELETE不增加。
4. 以最终V/C/F参数执行现有 `INSERT snapshot ... ON CONFLICT ... RETURNING`。没赢：回滚该尝试，销毁全部mapping/copy，按既有上限重试。UUID/carrier/local locator/journal都不重写；不得把失败映射写回业务binding。
5. 赢得snapshot后，对每个新表调用 `LakeWriteCreateDefinition`；接着由caller参数化写host table/relation/column binding。同事务其他会话仍看不到任何半套定义。
6. **自写CID边界不可省**：原 `snapshot` 在新目录写入前固定。显式snapshot+readonly SPI精确沿用其旧curcid，直接调用原首次INSERT empty proof会看不到刚插入的inline registry。所有定义/host binding写完后调用 `LakeCreatePushOwnWriteSnapshot`：CCI、`PushCopiedSnapshot`、`UpdateActiveSnapshotCommandId`；保持这个base active直至所有发布SPI结束，保持xmin/xmax/CSN不变，不调用`GetTransactionSnapshot`换集合，不修改注册业务snapshot。
7. 构造attempt内浅copy LakeTxnTable/Binding，将copy identity设PERMANENT/T及最终schemaV；标准definition已经可由第6步copy读取。对有净行的新表/既存表调用原 `LakePublishTable`，复用93fb首次空表统计初始化和已审统计合并。新表所有identity必须是本事务新身份，不允许committed_origin。纯CREATE没有净行则不调用行publisher，也不seed stats。
8. 构造created_table组后再按永久ID有序追加inlined_insert/inlined_delete，写一次snapshot_changes；成功路径先`PopActiveSnapshot`释放第6步base，再释放内部子事务，仍由用户外层事务最终COMMIT。任意错误回滚定义、绑定、统计、行与snapshot；不存在内部独立COMMIT。

CID依据：OG `spi.cpp:2766–2788、2898–2900`；`snapmgr.cpp:904–978`的Push/Pop规则要求copied快照必须有持续active或registered引用，否则首个readonly SPI结束即可释放。当前helper使用base active引用，SubAbort可清理该层快照；已有 `LakeValidateUninitializedInlineTable` 注册查询使用readonly SPI。该helper只推进自己的命令可见性，不能被误用来更新业务读S。

## 4. 最小验证交错与真实差分

必须由固定参考正常DDL/DML产生对照，不手改snapshot/ID/统计：

- 空CREATE：snapshot+1、schemaV+1、C+1、F不动，标准定义/carrier/schema_versions可读，stats仍无行；真实参考transport保持native所有ID/UUID/name不变，继续INSERT成功。
- CREATE+INSERT、两次UPDATE、INSERT→DELETE：只有最终新版本计入；保留CREATE且净空时仍只有DDL snapshot，无stats/file/row取号；CREATE→DROP且无其他变更完全无snapshot。
- 两新表同事务与既存表DML：schemaV只+1、C只+K、columnID每表1..N、changes组顺序、F按实际非空表计；引用每一处都使用attempt分配T。
- 同名跨宿主schema、ASCII大小写、中文、含`"`/`,`/`.`/`:`/单引号的名字；changeset编码后固定参考实际解析/冲突判定，不仅字符串相等。
- A准备CREATE、B普通INSERT抢snapshot并提交→A重试且旧local locator仍有效；普通INSERT不推进C，所以不得要求重试一定分配不同T；B回滚→A可获原slot。不得把管理DDL锁串行误称所有snapshot竞争已验。
- 同名后提交40001、不同名可合并；父schema删除/重命名拒绝；未知变化组仍拒绝而不截断。
- 取消/保存点回滚在正常carrier创建、claim后标准定义部分写入、hostbindings后、初始stats与行中途：全目录及普通heap原子，identity恢复，未泄漏局部ID/失败映射。
- 正式N4/首次EMPTY_TABLE INSERT/普通LOGIN/列ACL/view/SMP与2PC拒绝全回归。当前只提供候选源码，不据本文件声称这些通过。

## 5. 构建/许可/集成边界

只新增 `lake_create_publish.cpp/.h`；需由native作者把CPP加入既有Make/CMake，并把header接到其新pending identity接口。本次不修改其文件、不运行编译/SQL/source-lock。头文件变化须随业务CREATE整体官方full，不能只因本模块新增CPP便增量绕过ABI重编。

标准表schema与物理关系/OID信任继续走现有LakeMetadataRelation/Owner检查；本模块要求已配置私有owner，没有给普通用户元数据GRANT。普通DDL权限、形态拒绝、动态carrier可信登记、event-trigger防线与全局DDL锁序由正常CREATE模块实现。这里的窄职责不构成省略这些安全门的授权。

SQL0.10 candidate: native file-origin DML reuses the existing immutable transaction journal, standard inlined_delete changes and data carrier. Primitive private-carrier DDL is shared with business CREATE; file-position writes and first-carrier registration occur in an owned-CID publication snapshot after its claim. The journal binding remains immutable across attempts. No new durable table/file format is introduced. Build and native acceptance remain pending.

Native write preparation: `lake_arrow_builder.{h,cpp}` converts already-resolved native Datums into Arrow C Data buffers. It uses the existing C provider writer; it neither runs DuckDB nor persists a new lake format. Writer metadata uses the optional ABI1 footer-length suffix. This is source preparation for flush/direct-file DML, not completed atomic flush; standalone heap/Ustore and optional-footer evidence is maintained under the public project evidence tree, with19be native full and affected runtime acceptance passed (scope in public evidence).

Candidate lifecycle: `lake_file_sdk` loads the optional ABI2 contract and owns bounded session-memory candidate records; `lake_host` adopts subtransactions and resolves records through explicit post-lock hooks in `xact.cpp`. Published candidates close on commit; all other outcomes explicitly abort. No candidate is prepared under a snapshot claim. Revision815fde has scoped native lifecycle and affected read/DML acceptance; it is not atomic flush.

Bounded flush source: `lake_flush` reads native TableAM historical versions and writes explicit-ID data plus position/snapshot deletions; `lake_flush_path` prepares durable local directories without symbolic-link aliases. `lake_txn` records one maintenance operation and routes savepoint ownership; the appended `lake_publish` flush path reuses the existing final snapshot claim and writes standard metadata plus inline DELETE atomically. SQL0.11 replaces only the flush entry and preserves earlier SQL version gates. Revision1e07 has scoped native full/runtime evidence; the exact scope and unfinished capabilities are in README and public documents.

Failed preparation ownership: `LakePrepareInlineFlush` now wraps the entire internal construction in one context guard. It disposes failed contexts before rethrowing; native TableAM resources close in the inner handler and session-owned candidates stay queued for post-lock cleanup. The change repairs the actually reproduced savepoint retry retention, and is pending its own runtime acceptance.

File-delete flush source: `LakeFileVisitDeletes` extracts the exact existing reader validation into a shared native visitor; `lake_flush_file_deletes` merges old Parquet markers and standard inline(file_id,row_id,begin_snapshot) records before the snapshot claim. `LakeWriteFlushedFileDeletes` registers merged files, retires old metadata into the standard scheduled-deletion table and removes only transferred inline marks in the host transaction. It never physically removes an old deletion file. Unadopted sibling preparation failures reclaim the whole operation context while session-owned writer cleanup remains post-lock. This source is pending native qualification.

Native late-end flush candidate: pinned DuckLake transaction_state.cpp rejects same-table inline deletion versus inline flush. Native SQL0.11/private-owner/RC instead uses the existing standard partial data/delete files and inlined(file_id,position,begin_snapshot) representation to reconcile ends committed after preparation. The snapshot PK excludes other cooperating publishers during the source interval count/end check, ordered physical-position derivation, late-marker insertion and exact prefix deletion. The input payload/ID/birth immutability contract is unchanged; source relocation/structural changes still reject. This is new native adaptation, not a verbatim upstream feature; real current/history/reference/cancel and sustained-progress qualification is pending.

## Native persistent option adaptation (SQL 0.12)

- `lake_options.cpp` adapts pinned DuckLake
  `src/functions/ducklake_set_option.cpp` (scope/value admission) and
  `src/storage/ducklake_metadata_manager.cpp: SetConfigOption` (standard
  key/value/scope/scope_id rows), plus options-only/no-snapshot semantics in
  `ducklake_transaction_state.cpp`. It does not embed a DuckDB executor.
- Native adaptations: host relation owner/private metadata identity, bound SPI
  values, savepoint-aware exclusion of mixed lake DDL/DML, registration and
  metadata locks, fresh post-wait RC snapshot, closed positive inline-limit
  option set. Full reference reset/scopes/keys are not claimed.

## Permanent table RENAME adaptation (SQL 0.13)

- Reference: DuckLake0aaa8e10 `ducklake_transaction_state.cpp`
  `LocalChangeType::RENAMED`, renamed-table conflict checks, schema tracking
  and grouped `created_table` changes; `ducklake_metadata_manager.cpp`
  `DropTables(ids,true)` ends only the previous table definition.
- Native: `lake_rename.cpp` plus the transaction journal and existing late
  snapshot claim in `lake_publish.cpp`; host `RenameRelationInternal` locks
  and performs its own pg_class/type rename and cache invalidation. Existing
  carrier names and registrations are kept because the physical column schema
  does not change. No reference execution engine, new persistent row identity
  or alternative table format is introduced. Other ALTER forms stay closed.

## Derived index registry foundation (SQL0.14 candidate)

- This is a host extension, not a pg_ducklake secondary-index implementation:
  pinned DuckLake has no equivalent business search indexes. It implements the
  approved design6.14 independent definitions/segments/coverage foundation,
  following the immutable-segment and uncovered-input model referenced there.
- Algorithm source for subsequent scalar/DiskANN work is already frozen by
  native baseline ba1e4ad2f99295b1b0f65e3880a05cb75a1121d4, including
  `src/gausskernel/storage/access/datavec/diskann*.cpp` and corresponding
  `src/include/access/datavec` headers under the repository license. This
  registry does not claim extraction of the heap-TID/page-dependent backend.
- `lake_index.cpp` uses native host metadata SPI and owner checks;
  `lake_acl.cpp` extends the exact private control graph and trusted builtin
  constraint/partial-index validator. `lake_index_catalog.sql.inc` is copied
  verbatim into both0.14 scripts; no executable SQL include is used at runtime.
- No index artifact ABI, READY backend, scalar search, DiskANN search, ANN
  filtering, index acceleration or commercial qualification is delivered by
  definition registration. Source alone is not functional acceptance.


Inline B-tree candidate0.15 (unverified): native `DefineIndex` in
`src/gausskernel/optimizer/commands/indexcmds.cpp` builds the physical index;
`access/hbindex_am.h` IndexAM scans its ordinary carrier. No B-tree algorithm
is reimplemented. Native plancat.cpp's indcheckxmin/CSN horizon is rechecked
against the actual retained business snapshot. The new adapter performs only
stable-column/OID binding, closed integer comparison matching, cost/path wiring,
resource lifetime and lake visibility/fallback integration. This does not port
native heap TIDs into a Parquet locator or provide the file index backend.

Cross-integer ScanKey adaptation follows native nodeIndexscan.cpp's
ExecIndexBuildScanKeys: get_op_opfamily_properties supplies the RHS subtype.
The planner and executor both validate builtin INTEGER_BTREE_FAM operator
operand types; no cast/narrowing of int8 boundaries to int4 is introduced.
This source change is not qualified until its own official build and tests.

Integer equality-ANY/IN uses native nodeIndexscan.cpp ScalarArrayOpExpr handling
and nbtutils.cpp's SK_SEARCHARRAY preprocessing. B-tree itself handles NULL and
empty arrays, duplicate element elimination and repeated primitive scans. The
adapter admits only builtin integer equality and Const/Param RHS arrays, retains
original quals/files/private inputs, and estimates repeated seeks with native
estimate_array_length. No array expansion or B-tree algorithm is reimplemented.
This additional source still needs its own official build and actual tests.

NULL/NOTNULL keys follow native nodeIndexscan.cpp NullTest handling exactly:
SK_ISNULL plus SK_SEARCHNULL/SK_SEARCHNOTNULL and InvalidStrategy/InvalidOid,
with no operator function. Only plain primitive key Vars are admitted. Plan
private version5 carries this mode explicitly; files/private rows and original
quals remain complete. This additional source has not been built or qualified.

SQL0.16 index_root is a host-only addition to og_lake_internal.catalog_binding,
not an added DuckLake metadata option. It uses the existing registration lock,
fresh post-wait RC reads, policy owner checks and native transaction rollback.
Its independent path domain follows the accepted shared derived-index design.

Portable scalar file organization references Lance's BTree specification:
https://lance.org/format/index/scalar/btree/ (checked 2026-11-09).
The native adapter uses existing Parquet C ABI writers/readers and receives
native-tuplesort output; it does not implement a mutable B-tree or a second
catalog. Parquet field layouts, explicit-endian hash domains and context binding
are defined in lake_scalar_format.cpp/.h. Source is new native adaptation,
not a direct copy of Lance's Rust/file format or a claim of Lance compatibility.
Native catalog, coverage and query integration remain separate unfinished work.

The next lake_file_scalar.cpp host builder composes existing native tuplesort,
SPI transaction semantics, session-owned SDK candidates and catalog guards with
the portable format. Complete raw physical key projection lives in
lake_file_reader.cpp. No frozen/current generation view rewrite or logical
row-ID reassignment is introduced. The optional provider version identity uses
its already checked local RangeFile stat tuple, copied into owned footer/batch
suffixes; it is not a DuckLake metadata extension or a content hash. Native
builder qualification and query integration remain pending for this candidate.

The subsequent scalar query source uses the same tested portable component and
fixed catalog identities, not SPI business-row scans. Native ScanKey subtype/null/
array semantics supply integer predicates; source row-group projection remains in
the existing Parquet C ABI and business visibility in lake_file_reader.cpp.
File-side costs translate catalog byte/group estimates using native seq/random
page and CPU units, explicitly retaining uncovered work. This is an uncalibrated
local baseline, not a remote latency or commercial performance claim. The new
source requires its own full build and actual indexed query qualification.

The Ustore inline-index follow-up directly reuses indexcmds.cpp DefineIndex,
pg_opclass's builtin ubtree int4/int8 classes and INTEGER_UBTREE_FAM_OID, native
ubtree code, and nodeIndexscan.cpp IndexNext's IndexGetnextSlot ownership path.
Metadata control AM restrictions remain unchanged. Lake snapshot checks remain
conservative for retained older business snapshots. No Ustore tree implementation
is added; exact native behavior still requires separate runtime qualification.

Status diagnostics reuse the current-input coverage estimate from
lake_file_scalar.cpp. READY here is catalog coverage, not trust in artifact
bytes: execution rechecks source versions and index contents before pruning.
Revision fbacecd04a's full Debug and 28-case/795-assertion scoped evidence covers
the preceding scalar file query integration, not the new Ustore/diagnostic code.

Administrative repair composes the existing scalar reader's page checks with
the original session-owned candidate/transactional coverage publisher. The
same page visitor checks queries and all-leaf verification; the latter retains
only one leaf and the bounded directory, without collecting candidate positions.
No new business snapshot, row ID, catalog format or in-place artifact mutation
is introduced. The new native builder integration remains unverified.

Retained index birth visibility reuses snapmgr.cpp XidVisibleInSnapshot with
the actual snapshot. transam.cpp FetchCSNFromCache and its NULL-snapshot
RecentXmin/CLOG path can otherwise return FROZEN without proving old visibility.
Revision 1a0adab661's official131-input Debug build and35-case/981-assertion
scoped review cover this correction and the Ustore/diagnostic adapters. They
do not cover the subsequent streaming repair candidate.


Floating scalar adapter candidate (unbuilt): lake_scalar_key.h follows the locked
DuckDB common/radix.hpp EncodeDouble signed-order transform and uses native
float_ops (B-tree1970 / UB-tree6970). FLOAT4 values widen exactly before encoding;
SQL FLOAT4/FLOAT8 cross comparisons retain native operators. All NaNs are equal
and above positive infinity; both zero signs share a key. Derived page storage
remains int64 in format1, with floating algorithm_version2 and logical type bound
by the existing definition/source context; integer algorithm1 stays unchanged.
No new DuckLake data format, Parquet SDK ABI, comparison function or table AM is
introduced. Actual native and independently decoded total-order tests are required
before this candidate is qualified.

## Native scalar-mask file ANN candidate (2026-11-09; unbuilt)

The existing `DiskAnnSearchGraph` traversal in the pinned openGauss source is reused unchanged. A scalar-rejected vertex uses its existing removed-result semantics: outgoing edges still expand. The lake adapter checks each posting against a native `Bitmapset` of immutable source-file positions, built by projecting exactly one numeric source column and invoking the validated builtin comparison or NULL test; this is a query-local mask, not a new persistent bitmap index, PQ implementation, SDK ABI, or alternative graph algorithm. Inline/private and visibility-affected files remain complete native/source scans. Insufficient distinct matching candidates or navigation budget exhaustion selects complete per-file fallback before output. Plan-private version7 carries one separately typed scalar argument, re-evaluated on every rescan. More complex filters keep existing exact paths. The current490 negative run shows `id>=1` leaves file ANN coverage0; it establishes a missing path, not wrong query results. New native header signatures require official Debug full and actual SQL/bridge/recovery regression before qualification.
