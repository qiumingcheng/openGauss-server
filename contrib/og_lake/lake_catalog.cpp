/* Fixed DuckLake 1.1 metadata adapter and transactional local catalog creation. */
#include "postgres.h"
#include "lake_column_layout.h"
#include "lake_column_default.h"
#include "lake_file_sdk.h"
#include "lake_nullability.h"
#include "lake_autoanalyze.h"
#include "lake_inline_sources.h"
#include "lake_native_partition.h"
#include "catalog/pg_extension.h"
#include "commands/extension.h"
#include "access/genam.h"
#include "catalog/indexing.h"
#include "knl/knl_variable.h"
#include "lake_txn_private.h"
#include "lake_index_private.h"
#include "lake_create_publish.h"
#include "lake_version.h"
#include "lake_row_lock.h"
#include "lake_drop.h"
#include "lake_types.h"
#include "access/datavec/vector.h"
#include "access/heapam.h"
#include "access/og_lake.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_class.h"
#include "commands/schemacmds.h"
#include "commands/tablecmds.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "nodes/makefuncs.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include <dirent.h>
#include <errno.h>

PG_FUNCTION_INFO_V1(og_lake_attach_catalog);
PG_FUNCTION_INFO_V1(og_lake_create_catalog);

#include "lake_catalog_ddl.inc"
#include "lake_column_default.h"

static char* LakeInstalledVersion(void)
{
    char* cached = NULL;
    if (LakeFastVersionLookup(&cached))
        return cached;
    /* This openGauss tree has no pg_extension syscache. Follow its own
     * extension.cpp lookup and read the varlena field through TableAM. */
    Relation relation=heap_open(ExtensionRelationId,AccessShareLock);
    ScanKeyData key;
    ScanKeyInit(&key,Anum_pg_extension_extname,BTEqualStrategyNumber,F_NAMEEQ,CStringGetDatum("og_lake"));
    SysScanDesc scan=systable_beginscan(relation,ExtensionNameIndexId,true,NULL,1,&key);
    HeapTuple tuple=systable_getnext(scan);
    if(!HeapTupleIsValid(tuple))
        ereport(ERROR,(errcode(ERRCODE_UNDEFINED_OBJECT),errmsg("native lake extension is not installed")));
    bool isnull=false;
    Datum value=tableam_tops_tuple_getattr(tuple,Anum_pg_extension_extversion,RelationGetDescr(relation),&isnull);
    char* version=isnull?NULL:TextDatumGetCString(value);
    systable_endscan(scan);
    heap_close(relation,AccessShareLock);
    LakeFastVersionStore(version);
    return version;
}

/* A cumulative feature requires every published contract from its own
 * release through the installed release. Disabled/aliased build labels must
 * break the chain. Earlier non-cumulative contracts retain their own rules. */
static constexpr int LAKE_FIRST_CUMULATIVE_MINOR = 32;
static constexpr bool LakeCanonicalSqlLabel(const char* label, int minor)
{
    return label != NULL && label[0] == '0' && label[1] == '.' &&
        label[2] == '0' + minor / 10 && label[3] == '0' + minor % 10 && label[4] == '\0';
}

static bool LakeCumulativeContract(const char* version, int firstMinor)
{
    static constexpr bool declared[] = {
        LakeCanonicalSqlLabel(OG_LAKE_TIME_PARTITION_SQL_VERSION, 32),
        LakeCanonicalSqlLabel(OG_LAKE_TIME_PARTITION_PRUNE_SQL_VERSION, 33),
        LakeCanonicalSqlLabel(OG_LAKE_PARTITION_EVOLUTION_SQL_VERSION, 34),
        LakeCanonicalSqlLabel(OG_LAKE_CORE_FOLLOWUPS_SQL_VERSION, 35),
        LakeCanonicalSqlLabel(OG_LAKE_LOGICAL_DROP_SQL_VERSION, 36),
        LakeCanonicalSqlLabel(OG_LAKE_ADD_COLUMN_SQL_VERSION, 37),
        LakeCanonicalSqlLabel(OG_LAKE_JOURNAL_SPILL_SQL_VERSION, 38),
        LakeCanonicalSqlLabel(OG_LAKE_LITERAL_DEFAULT_SQL_VERSION, 39),
        LakeCanonicalSqlLabel(OG_LAKE_NOT_NULL_SQL_VERSION, 40),
        LakeCanonicalSqlLabel(OG_LAKE_ALTER_DEFAULT_SQL_VERSION, 41),
        LakeCanonicalSqlLabel("0.42", 42),
        LakeCanonicalSqlLabel("0.43", 43),
        LakeCanonicalSqlLabel(OG_LAKE_BINARY_NOT_NULL_SQL_VERSION, 44),
        LakeCanonicalSqlLabel(OG_LAKE_BINARY_DEFAULT_SQL_VERSION, 45),
        LakeCanonicalSqlLabel(OG_LAKE_INT2_SCALAR_SQL_VERSION, 46),
        LakeCanonicalSqlLabel(OG_LAKE_INT2_ROW_SMP_SQL_VERSION, 47),
        LakeCanonicalSqlLabel(OG_LAKE_PARTITION_REWRITE_SQL_VERSION, 48),
        LakeCanonicalSqlLabel(OG_LAKE_CREATE_LITERAL_DEFAULT_SQL_VERSION, 49),
        LakeCanonicalSqlLabel(OG_LAKE_SNAPSHOT_EXPIRY_SQL_VERSION, 50),
        LakeCanonicalSqlLabel(OG_LAKE_MULTI_FILE_REWRITE_SQL_VERSION, 51),
        LakeCanonicalSqlLabel(OG_LAKE_UUID_BUSINESS_SQL_VERSION, 52),
        LakeCanonicalSqlLabel(OG_LAKE_JSON_BUSINESS_SQL_VERSION, 53),
        LakeCanonicalSqlLabel(OG_LAKE_NUMERIC_BUSINESS_SQL_VERSION, 54),
        LakeCanonicalSqlLabel(OG_LAKE_SNAPSHOT_METADATA_SQL_VERSION, 55),
        LakeCanonicalSqlLabel(OG_LAKE_LAST_COMMITTED_SQL_VERSION, 56),
        LakeCanonicalSqlLabel(OG_LAKE_TIME_TRAVEL_SQL_VERSION, 57),
        LakeCanonicalSqlLabel(OG_LAKE_TABLE_CHANGES_SQL_VERSION, 58),
        LakeCanonicalSqlLabel(OG_LAKE_BIND_SNAPSHOT_SQL_VERSION, 59),
        LakeCanonicalSqlLabel(OG_LAKE_MERGE_ADJACENT_SQL_VERSION, 60),
        LakeCanonicalSqlLabel(OG_LAKE_EXTERNAL_FILE_SQL_VERSION, 61),
        LakeCanonicalSqlLabel(OG_LAKE_COPY_IO_SQL_VERSION, 62),
        LakeCanonicalSqlLabel(OG_LAKE_PARTITION_COMBO_SQL_VERSION, 63),
        LakeCanonicalSqlLabel(OG_LAKE_IX_SMP_EXTENDED_SQL_VERSION, 64),
        LakeCanonicalSqlLabel(OG_LAKE_STATS_EXTENDED_SQL_VERSION, 65),
        LakeCanonicalSqlLabel(OG_LAKE_EXTENDED_DEFAULTS_SQL_VERSION, 66),
        LakeCanonicalSqlLabel(OG_LAKE_IX_OBS_SQL_VERSION, 67),
        LakeCanonicalSqlLabel(OG_LAKE_SNAPSHOT_PIN_SQL_VERSION, 68),
        LakeCanonicalSqlLabel(OG_LAKE_PHYSICAL_CLEANUP_SQL_VERSION, 69),
        LakeCanonicalSqlLabel(OG_LAKE_ALTER_TYPE_OPS_SQL_VERSION, 70),
        LakeCanonicalSqlLabel(OG_LAKE_JSON_FORMAT_SQL_VERSION, 71),
        LakeCanonicalSqlLabel(OG_LAKE_VECTOR_SMP_SQL_VERSION, 72),
        LakeCanonicalSqlLabel(OG_LAKE_TRUNCATE_PARTITION_SQL_VERSION, 73),
        LakeCanonicalSqlLabel(OG_LAKE_NATIVE_PARTITION_EXPANDED_SQL_VERSION, 74),
    };
    if (version == NULL || strlen(version) != 4 || version[0] != '0' || version[1] != '.' ||
        version[2] < '0' || version[2] > '9' || version[3] < '0' || version[3] > '9')
        return false;
    int installed = (version[2] - '0') * 10 + version[3] - '0';
    if (firstMinor < LAKE_FIRST_CUMULATIVE_MINOR || installed < firstMinor ||
        installed >= LAKE_FIRST_CUMULATIVE_MINOR + (int)lengthof(declared))
        return false;
    for (int minor = firstMinor; minor <= installed; ++minor)
        if (!declared[minor - LAKE_FIRST_CUMULATIVE_MINOR])
            return false;
    return true;
}

static bool LakeInstalledContractEnabled(int firstMinor)
{
    char* version = LakeInstalledVersion();
    bool enabled = LakeCumulativeContract(version, firstMinor);
    if (version != NULL)
        pfree(version);
    return enabled;
}

/* Root-coordinated installed SQL contracts, independent of the binary's
 * newest default. Keep retained23 READ/INSERT even before upgrade or rollback.
 * Empty/aliased future labels never grant new rights to23 or to each other. */
/* SQL35 combines three reviewed ordinary-table follow-ups. Exact installed
 * capability is independent of binary default, and does not enable SQL27/28. */
/* SQL51 only widens an explicitly selected, already admitted rewrite group.
 * Old installed SQL keeps its original two-file bound under this binary. */
/* SQL74 (PGL-0 / IX5-N7): Expanded native pg_partition (RANGE/LIST/HASH, multi-leaf up to 64,
 * extended partition key & business column types) plus high-dim Vamana+PQ improvements. */
bool LakeNativePartitionExpandedVersionEnabled(void)
{
    if(!OidIsValid(get_extension_oid("og_lake",true)))return false;
    return LakeInstalledContractEnabled(74);
}
/* SQL73 (PGL-3): truncate(N) partition transform across INT2/INT4/INT8/TEXT/BYTEA. */
bool LakeTruncatePartitionVersionEnabled(void)
{
    if(!OidIsValid(get_extension_oid("og_lake",true)))return false;
    return LakeInstalledContractEnabled(73);
}
/* SQL72 (N7-VECTOR): SMP + Vectorized VecForeignScan parallel execution,
 * vectorized extended scalar/partition/temporal scans, and per-batch cleanup. */
bool LakeVectorSmpVersionEnabled(void)
{
    if(!OidIsValid(get_extension_oid("og_lake",true)))return false;
    return LakeInstalledContractEnabled(72);
}
/* SQL71 (PGL-2 / PGL-6): local JSON / JSONL COPY FROM/TO, og_lake_file_fdw,
 * external schema inference, and file info. */
bool LakeJsonFormatVersionEnabled(void)
{
    if(!OidIsValid(get_extension_oid("og_lake",true)))return false;
    return LakeInstalledContractEnabled(71);
}
/* SQL70 (N6 widening ALTER COLUMN TYPE evolution + OPS1 backup manifest,
 * catalog/index integrity verify/repair, and retention observability). */
bool LakeAlterTypeOpsVersionEnabled(void)
{
    if(!OidIsValid(get_extension_oid("og_lake",true)))return false;
    return LakeInstalledContractEnabled(70);
}
/* SQL69 (N9b local physical GC / cleanup_old_files / delete_orphaned_files
 * protected by exclusive catalog guard, active snapshot pins, and OldestXmin). */
bool LakePhysicalCleanupVersionEnabled(void)
{
    if(!OidIsValid(get_extension_oid("og_lake",true)))return false;
    return LakeInstalledContractEnabled(69);
}
/* SQL68 (PGL-5 single-node active reader snapshot pin & maintenance resilience). */
bool LakeSnapshotPinVersionEnabled(void)
{
    if(!OidIsValid(get_extension_oid("og_lake",true)))return false;
    return LakeInstalledContractEnabled(68);
}
/* SQL67 (IX5 remaining gaps + PGL-4 local observability/cache/EXPLAIN closure). */
bool LakeIxObsVersionEnabled(void)
{
    if(!OidIsValid(get_extension_oid("og_lake",true)))return false;
    return LakeInstalledContractEnabled(67);
}
/* SQL66 (N6 / PGL-1): Unlocks extended scalar/UUID/JSON/NUMERIC(p,s) literal defaults,
 * ADD COLUMN ... NOT NULL DEFAULT <literal>, and cross-session relcache invalidation. */
bool LakeExtendedDefaultsVersionEnabled(void)
{
    if(!OidIsValid(get_extension_oid("og_lake",true)))return false;
    return LakeInstalledContractEnabled(66);
}
/* SQL65 (N7-STATS) unlocks exact NUMERIC(p,s) canonical min/max statistics
 * across ducklake_table_column_stats and ducklake_file_column_stats, plus
 * exact file-level bounds across direct INSERT, flush, and rewrite/merge. */
bool LakeStatsExtendedVersionEnabled(void)
{
    if(!OidIsValid(get_extension_oid("og_lake",true)))return false;
    return LakeInstalledContractEnabled(65);
}
/* SQL64 (IX1~IX5 & N7-SMP) unlocks extended payload columns across scalar/ANN
 * indexes and row SMP, plus PQ/Boolean/multi-file/DOP=4 parallel ANN SMP. */
bool LakeIxSmpExtendedVersionEnabled(void)
{
    if(!OidIsValid(get_extension_oid("og_lake",true)))return false;
    return LakeInstalledContractEnabled(64);
}
/* SQL63 (PGL-3) expands DuckLake file partitions to UUID/NUMERIC/DATE/temporal/BYTEA
 * keys and unlocks extended payload types, indexes, COPY, and row SMP combinations,
 * while keeping standard truncate(...) explicitly rejected with 0A000. */
bool LakePartitionComboVersionEnabled(void)
{
    if(!OidIsValid(get_extension_oid("og_lake",true)))return false;
    return LakeInstalledContractEnabled(63);
}
/* SQL62 adds controlled local Parquet/CSV COPY FROM/TO over openat2-confined paths
 * and exclusive candidate export with transaction/savepoint rollback cleanup. */
bool LakeCopyIoVersionEnabled(void)
{
    if(!OidIsValid(get_extension_oid("og_lake",true)))return false;
    return LakeInstalledContractEnabled(62);
}
/* SQL61 adds og_lake_file_fdw read-only external Parquet/CSV foreign tables
 * confined by kernel openat2 anchor-fd roots and public schema inference. */
bool LakeExternalFileVersionEnabled(void)
{
    if(!OidIsValid(get_extension_oid("og_lake",true)))return false;
    return LakeInstalledContractEnabled(61);
}
/* SQL60 adds og_lake.merge_adjacent_files, target_file_size/auto_compact options,
 * automatic background small-file merge, and cross-evolution rewrite_data_files. */
bool LakeMergeAdjacentVersionEnabled(void)
{
    return LakeInstalledContractEnabled(60);
}
/* SQL59 adds transaction-scoped read-only snapshot binding og_lake.bind_snapshot,
 * generational og_lake.rebuild_lake_index, and privilege-filtered public management views. */
bool LakeBindSnapshotVersionEnabled(void)
{
    return LakeInstalledContractEnabled(59);
}
/* SQL58 adds serial read-only public CDC change-feed functions
 * table_insertions / table_deletions / table_changes over (regclass, bigint, bigint)
 * and (regclass, timestamptz, timestamptz). */
bool LakeTableChangesVersionEnabled(void)
{
    return LakeInstalledContractEnabled(58);
}
/* SQL57 adds serial read-only public time_travel(regclass, bigint/timestamptz)
 * with explicit AS historical column verification and journal-free read views. */
bool LakeTimeTravelVersionEnabled(void)
{
    return LakeInstalledContractEnabled(57);
}
/* SQL56 adds u_sess logical-session last_committed_snapshot across all10 business
 * and DDL publishers. Older installed SQL retains its exact pre-0.56 gates. */
bool LakeLastCommittedVersionEnabled(void)
{
    return LakeInstalledContractEnabled(56);
}
/* SQL55 exposes only read-only public snapshot metadata. Older installed SQL
 * retains its own exact feature gates under the cumulative binary. */
bool LakeSnapshotMetadataVersionEnabled(void)
{
    return LakeInstalledContractEnabled(55);
}
bool LakeNumericBusinessVersionEnabled(void)
{
    return LakeInstalledContractEnabled(54);
}
bool LakeJsonBusinessVersionEnabled(void)
{
    return LakeInstalledContractEnabled(53);
}
bool LakeUuidBusinessVersionEnabled(void)
{
    return LakeInstalledContractEnabled(52);
}
bool LakeMultiFileRewriteVersionEnabled(void)
{
    return LakeInstalledContractEnabled(51);
}
int LakeRewriteSourceLimit(void)
{ return LakeMultiFileRewriteVersionEnabled()?OG_LAKE_REWRITE_MAX_SOURCES:2; }

/* SQL50 adds only bounded metadata-only logical snapshot expiry. */
bool LakeSnapshotExpiryVersionEnabled(void)
{
    return LakeInstalledContractEnabled(50);
}

/* SQL49 adds only authenticated protected-CREATE typed literal defaults. */
bool LakeCreateLiteralDefaultVersionEnabled(void)
{
    return LakeInstalledContractEnabled(49);
}

/* SQL48 adds only same historical-spec/complete-key file rewrite. */
bool LakePartitionRewriteVersionEnabled(void)
{
    return LakeInstalledContractEnabled(48);
}

/* SQL47 adds plain INT2 row SMP only. This exact leader predicate
 * never runs in a worker and grants no rights to future/aliased labels. */
bool LakeInt2RowSmpVersionEnabled(void)
{
    return LakeInstalledContractEnabled(47);
}

/* Exact installed SQL46/47 retains serial same-type INT2 scalar indexes.
 * Read the current extension tuple for every boundary; no version cache. */
bool LakeInt2ScalarIndexVersionEnabled(void)
{
    return LakeInstalledContractEnabled(46);
}

/* Exact installed SQL45/46 adds BYTEA literal defaults. Later or
 * similarly spelled labels grant no rights. Physical SDK ABI is unchanged. */
bool LakeBinaryDefaultVersionEnabled(void)
{
    return LakeInstalledContractEnabled(45);
}

/* SQL45 inherits SQL44 logical constraints without changing retained
 * physical nullability or enabling ADD NOT NULL. */
bool LakeBinaryNotNullVersionEnabled(void)
{
    return LakeInstalledContractEnabled(44);
}

bool LakeBinaryBusinessVersionEnabled(void)
{ return LakeInstalledContractEnabled(43); }

bool LakeDropColumnVersionEnabled(void)
{ return LakeInstalledContractEnabled(42); }


bool LakeAlterDefaultVersionEnabled(void)
{ return LakeInstalledContractEnabled(41); }


bool LakeNotNullVersionEnabled(void)
{ return LakeInstalledContractEnabled(40); }


bool LakeLiteralDefaultVersionEnabled(void)
{ return LakeInstalledContractEnabled(39); }





/* SQL34 adds versioned inline sources for ordinary native CREATE tables.
 * This exact label inherits SQL33 proofs and exactly SQL26 parent rights. */

/* SQL33 adds only optional time-file query proofs. It inherits SQL32 data
 * rules and exactly the published SQL26 native-parent rights, not SQL27/28. */

/* File time transforms add no native-parent rights or producer provenance. */

uint32 LakeNativePartitionContractCapabilities(const char* version)
{
    if(!version||!version[0]||!OG_LAKE_NATIVE_PARTITION_SQL_VERSION[0]||
       strcmp(OG_LAKE_NATIVE_PARTITION_SQL_VERSION,"0.23")!=0||
       strcmp(OG_LAKE_NATIVE_PARTITION_WRITE_SQL_VERSION,OG_LAKE_NATIVE_PARTITION_SQL_VERSION)!=0)return 0;
    const uint32 base=LAKE_NATIVE_PARENT_READ|LAKE_NATIVE_PARENT_INSERT;
    if(strcmp(version,OG_LAKE_NATIVE_PARTITION_SQL_VERSION)==0)return base;
    const char* mutation=OG_LAKE_NATIVE_PARTITION_MUTATION_SQL_VERSION;
    const char* analyze=OG_LAKE_NATIVE_PARTITION_ANALYZE_SQL_VERSION;
    bool validMutation=mutation[0]&&strcmp(mutation,OG_LAKE_NATIVE_PARTITION_SQL_VERSION)!=0&&
        strcmp(OG_LAKE_NATIVE_PARTITION_COPY_SQL_VERSION,mutation)==0;
    if(!validMutation)return 0;
    const uint32 write=base|LAKE_NATIVE_PARENT_MUTATION|LAKE_NATIVE_PARENT_COPY;
    if(strcmp(version,mutation)==0)return write;
    bool validAnalyze=analyze[0]&&strcmp(analyze,OG_LAKE_NATIVE_PARTITION_SQL_VERSION)!=0&&strcmp(analyze,mutation)!=0;
    if(!validAnalyze)return 0;
    const uint32 stats=write|LAKE_NATIVE_PARENT_ANALYZE;
    if(strcmp(version,analyze)==0)return stats;
    const char* maintenance=OG_LAKE_NATIVE_PARTITION_ROW_HEAD_SQL_VERSION;
    bool validMaintenance=maintenance[0]&&strcmp(maintenance,OG_LAKE_NATIVE_PARTITION_SQL_VERSION)!=0&&
        strcmp(maintenance,mutation)!=0&&strcmp(maintenance,analyze)!=0;
    if(!validMaintenance)return 0;
    if(strcmp(version,maintenance)==0)return stats|LAKE_NATIVE_PARENT_ROW_HEAD_MAINTENANCE;
    /* File-only SQL29 keeps precisely the published SQL26 parent contract. */
    if(strcmp(maintenance,"0.26")==0&&strcmp(OG_LAKE_FILE_PARTITION_KEYS_SQL_VERSION,"0.29")==0&&
       strcmp(version,OG_LAKE_FILE_PARTITION_KEYS_SQL_VERSION)==0)
        return stats|LAKE_NATIVE_PARENT_ROW_HEAD_MAINTENANCE;
    /* SQL30 adds only ordinary file bucket rules. No SQL27/28 parent rights. */
    if(strcmp(maintenance,"0.26")==0&&strcmp(OG_LAKE_FILE_PARTITION_KEYS_SQL_VERSION,"0.29")==0&&
       strcmp(OG_LAKE_BUCKET_PARTITION_SQL_VERSION,"0.30")==0&&
       strcmp(version,OG_LAKE_BUCKET_PARTITION_SQL_VERSION)==0)
        return stats|LAKE_NATIVE_PARENT_ROW_HEAD_MAINTENANCE;
    /* Temporal business fields add no native-parent rights until SQL74 PGL-0. */
    if(strcmp(maintenance,"0.26")==0&&strcmp(OG_LAKE_FILE_PARTITION_KEYS_SQL_VERSION,"0.29")==0&&
       strcmp(OG_LAKE_BUCKET_PARTITION_SQL_VERSION,"0.30")==0&&
       strcmp(OG_LAKE_TEMPORAL_BUSINESS_SQL_VERSION,"0.31")==0&&
       (strcmp(version,OG_LAKE_TEMPORAL_BUSINESS_SQL_VERSION)==0||LakeCumulativeContract(version, 32))){
        uint32 caps=stats|LAKE_NATIVE_PARENT_ROW_HEAD_MAINTENANCE;
        if(LakeCumulativeContract(version, 74))caps|=LAKE_NATIVE_PARENT_EXPANDED;
        return caps;
    }
    return 0;
}

bool LakeNativeFutureContractEnabled(const char* version)
{
    return (LakeNativePartitionContractCapabilities(version)&LAKE_NATIVE_PARENT_MUTATION)!=0;
}

static uint32 LakeNativeInstalledParentCapabilities(void)
{
    if(!OidIsValid(get_extension_oid("og_lake",true)))return 0;
    char* version=LakeInstalledVersion();uint32 caps=LakeNativePartitionContractCapabilities(version);
    if(version)pfree(version);return caps;
}

/* Retained SQL contracts keep the row ForeignScan path. This gate grants no
 * expression/type capability: the FDW and native vector planner check those. */
/* SQL0.19 is prepared by the combined upgrade owner. Old SQL keeps its
 * existing explicit direct-file refusal and 64 MiB journal contract. */
bool LakeIdentityPartitionKeysVersionEnabled(void)
{
    if(!OidIsValid(get_extension_oid("og_lake",true)))return false;
    char* version=LakeInstalledVersion();
    bool enabled=version!=NULL&&strcmp(OG_LAKE_FILE_PARTITION_KEYS_SQL_VERSION,"0.29")==0&&
        (strcmp(version,OG_LAKE_FILE_PARTITION_KEYS_SQL_VERSION)==0||
         (strcmp(OG_LAKE_BUCKET_PARTITION_SQL_VERSION,"0.30")==0&&
          (strcmp(version,OG_LAKE_BUCKET_PARTITION_SQL_VERSION)==0||
           (strcmp(OG_LAKE_TEMPORAL_BUSINESS_SQL_VERSION,"0.31")==0&&
            (strcmp(version,OG_LAKE_TEMPORAL_BUSINESS_SQL_VERSION)==0||LakeCumulativeContract(version, 32))))));
    if(version)pfree(version);
    return enabled;
}

bool LakeBucketPartitionVersionEnabled(void)
{
    if(!OidIsValid(get_extension_oid("og_lake",true)))return false;
    char* version=LakeInstalledVersion();
    bool enabled=version!=NULL&&strcmp(OG_LAKE_FILE_PARTITION_KEYS_SQL_VERSION,"0.29")==0&&
        strcmp(OG_LAKE_BUCKET_PARTITION_SQL_VERSION,"0.30")==0&&
        (strcmp(version,OG_LAKE_BUCKET_PARTITION_SQL_VERSION)==0||
         (strcmp(OG_LAKE_TEMPORAL_BUSINESS_SQL_VERSION,"0.31")==0&&
          (strcmp(version,OG_LAKE_TEMPORAL_BUSINESS_SQL_VERSION)==0||LakeCumulativeContract(version, 32))));
    if(version)pfree(version);
    return enabled;
}

bool LakeTimePartitionVersionEnabled(void)
{
    if(!OidIsValid(get_extension_oid("og_lake",true)))return false;
    char* version=LakeInstalledVersion();
    bool enabled=LakeCumulativeContract(version, 32)&&
        strcmp(OG_LAKE_TEMPORAL_BUSINESS_SQL_VERSION,"0.31")==0&&
        strcmp(OG_LAKE_BUCKET_PARTITION_SQL_VERSION,"0.30")==0&&
        strcmp(OG_LAKE_FILE_PARTITION_KEYS_SQL_VERSION,"0.29")==0;
    if(version)pfree(version);
    return enabled;
}

/* File-query proofs require installed SQL33 or its exact SQL34/35 successors.
 * Retained SQL32 keeps
 * its data transformations but does not acquire this query-proof capability. */
bool LakeTimePartitionPruneVersionEnabled(void)
{
    if(!OidIsValid(get_extension_oid("og_lake",true)))return false;
    char* version=LakeInstalledVersion();
    bool enabled=LakeCumulativeContract(version, 33)&&LakeCumulativeContract(version, 32)&&
        strcmp(OG_LAKE_TEMPORAL_BUSINESS_SQL_VERSION,"0.31")==0&&
        strcmp(OG_LAKE_BUCKET_PARTITION_SQL_VERSION,"0.30")==0&&
        strcmp(OG_LAKE_FILE_PARTITION_KEYS_SQL_VERSION,"0.29")==0;
    if(version)pfree(version);
    return enabled;
}

/* Ordinary source evolution in SQL34/35 never expands ATTACH/native-parent or arbitrary
 * column-shape admission. Old SQL33 and below retain NoCarrier SET/RESET. */
bool LakePartitionEvolutionVersionEnabled(void)
{
    if(!OidIsValid(get_extension_oid("og_lake",true)))return false;
    char* version=LakeInstalledVersion();
    bool enabled=LakeCumulativeContract(version, 34)&&
        LakeCumulativeContract(version, 33)&&LakeCumulativeContract(version, 32)&&
        strcmp(OG_LAKE_TEMPORAL_BUSINESS_SQL_VERSION,"0.31")==0&&
        strcmp(OG_LAKE_BUCKET_PARTITION_SQL_VERSION,"0.30")==0&&
        strcmp(OG_LAKE_FILE_PARTITION_KEYS_SQL_VERSION,"0.29")==0;
    if(version)pfree(version);
    return enabled;
}

/* Three ordinary-table follow-ups share one exact installed capability.
 * All inherited format/type gates remain checked; old SQL labels stay old. */
bool LakeCoreFollowupsVersionEnabled(void)
{
    if(!OidIsValid(get_extension_oid("og_lake",true)))return false;
    char* version=LakeInstalledVersion();
    bool enabled=LakeCumulativeContract(version, 35)&&LakeCumulativeContract(version, 34)&&
        LakeCumulativeContract(version, 33)&&LakeCumulativeContract(version, 32)&&
        strcmp(OG_LAKE_TEMPORAL_BUSINESS_SQL_VERSION,"0.31")==0&&
        strcmp(OG_LAKE_BUCKET_PARTITION_SQL_VERSION,"0.30")==0&&
        strcmp(OG_LAKE_FILE_PARTITION_KEYS_SQL_VERSION,"0.29")==0;
    if(version)pfree(version);
    return enabled;
}

bool LakeLogicalDropVersionEnabled(void)
{
    if(!OidIsValid(get_extension_oid("og_lake",true)))return false;
    char* version=LakeInstalledVersion();
    bool enabled=LakeCumulativeContract(version, 36)&&LakeCumulativeContract(version, 35)&&
        LakeCumulativeContract(version, 34)&&LakeCumulativeContract(version, 33)&&LakeCumulativeContract(version, 32)&&
        strcmp(OG_LAKE_TEMPORAL_BUSINESS_SQL_VERSION,"0.31")==0&&
        strcmp(OG_LAKE_BUCKET_PARTITION_SQL_VERSION,"0.30")==0&&
        strcmp(OG_LAKE_FILE_PARTITION_KEYS_SQL_VERSION,"0.29")==0;
    if(version)pfree(version);
    return enabled;
}

bool LakeTemporalBusinessVersionEnabled(void)
{
    if(!OidIsValid(get_extension_oid("og_lake",true)))return false;
    char* version=LakeInstalledVersion();
    bool enabled=version!=NULL&&strcmp(OG_LAKE_TEMPORAL_BUSINESS_SQL_VERSION,"0.31")==0&&
        (strcmp(version,OG_LAKE_TEMPORAL_BUSINESS_SQL_VERSION)==0||LakeCumulativeContract(version, 32));
    if(version)pfree(version);
    return enabled;
}

bool LakeIdentityPartitionVersionEnabled(void)
{
    char* version=LakeInstalledVersion();
    const char* target=OG_LAKE_IDENTITY_PARTITION_SQL_VERSION;
    bool enabled=version!=NULL&&target[0]!='\0'&&(strcmp(version,target)==0||(strcmp(version,"0.23")==0||LakeNativeFutureContractEnabled(version)));
    if(version!=NULL)pfree(version);
    return enabled;
}

bool LakeDirectInsertVersionEnabled(void)
{
    char* version=LakeInstalledVersion();
    bool enabled=version!=NULL&&(strcmp(version,"0.19")==0||(strcmp(version,"0.20")==0||(strcmp(version,"0.21")==0||(strcmp(version,"0.22")==0||(strcmp(version,"0.23")==0||LakeNativeFutureContractEnabled(version))))));
    if(version!=NULL)pfree(version);
    return enabled;
}

bool LakeVectorBatchVersionEnabled(void)
{
    char* version=LakeInstalledVersion();
    bool enabled=version!=NULL&&(strcmp(version,"0.18")==0||(strcmp(version,"0.19")==0||(strcmp(version,"0.20")==0||(strcmp(version,"0.21")==0||(strcmp(version,"0.22")==0||(strcmp(version,"0.23")==0||LakeNativeFutureContractEnabled(version)))))));
    if(version!=NULL)pfree(version);
    return enabled;
}

void LakeRequireCopyVersion(void)
{
    /* Extend this closed list only with reviewed future SQL versions. A new
     * runtime must retain historical SQL0.7/0.16/0.17 COPY rejection. */
    char* version=LakeInstalledVersion();
    bool enabled=version!=NULL&&(strcmp(version,"0.18")==0||(strcmp(version,"0.19")==0||(strcmp(version,"0.20")==0||(strcmp(version,"0.21")==0||(strcmp(version,"0.22")==0||(strcmp(version,"0.23")==0||LakeNativeFutureContractEnabled(version)))))));
    if(version!=NULL)pfree(version);
    if(!enabled)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("COPY FROM into native lake tables requires extension 0.18")));
}

void LakeRequireWriteVersion(CmdType operation)
{
    char* version=LakeInstalledVersion();
    bool writable=version!=NULL && ((strcmp(version,"0.10")==0||strcmp(version,"0.11")==0||strcmp(version,"0.12")==0||strcmp(version,"0.13")==0||(strcmp(version,"0.14")==0||(strcmp(version,"0.15")==0||(strcmp(version,"0.16")==0||(strcmp(version,"0.17")==0||(strcmp(version,"0.18")==0||(strcmp(version,"0.19")==0||(strcmp(version,"0.20")==0||(strcmp(version,"0.21")==0||(strcmp(version,"0.22")==0||(strcmp(version,"0.23")==0||LakeNativeFutureContractEnabled(version)))))))))))) || strcmp(version,"0.9")==0 || strcmp(version,"0.8")==0 || strcmp(version,"0.7")==0 || strcmp(version,"0.6")==0 || strcmp(version,"0.5")==0 || strcmp(version,"0.4")==0 ||
        (operation==CMD_INSERT && strcmp(version,"0.3")==0));
    if(!writable)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake INSERT requires extension 0.3 or later; UPDATE/DELETE require extension 0.4 or later")));
}

static Snapshot LakeCatalogSnapshot(void)
{
    return ActiveSnapshotSet() ? GetActiveSnapshot() : GetTransactionSnapshot();
}

static bool LakeIsServer(Oid serverOid)
{
    return LakeTrustedForeignServer(serverOid);
}

struct LakeTableOptions { bool create; int64 table_id; const char* schema; };
static LakeTableOptions LakeParseTableOptions(List* options)
{
    const char* mode=NULL; const char* id=NULL; const char* schema=NULL;
    ListCell* cell=NULL;
    foreach(cell,options) {
        DefElem* option=(DefElem*)lfirst(cell);
        if(strcmp(option->defname,"binding_mode")==0 && mode==NULL) mode=defGetString(option);
        else if(strcmp(option->defname,"table_id")==0 && id==NULL) id=defGetString(option);
        else if(strcmp(option->defname,"lake_schema")==0 && schema==NULL) schema=defGetString(option);
        else ereport(ERROR,(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),errmsg("unknown or repeated native lake table option \"%s\"",option->defname)));
    }
    if(mode==NULL || (strcmp(mode,"create")==0 ? schema==NULL : id==NULL))
        ereport(ERROR,(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),errmsg("native lake requires binding_mode and table_id (attach) or lake_schema (create)")));
    LakeTableOptions result; result.create=strcmp(mode,"create")==0;result.table_id=0;result.schema=schema;
    if(result.create) {
        if(id!=NULL || schema[0]=='\0')
            ereport(ERROR,(errcode(ERRCODE_FDW_INVALID_OPTION_DATA),errmsg("native lake create requires a nonempty lake_schema and no table_id")));
    } else {
        if(strcmp(mode,"attach")!=0 || schema!=NULL)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake supports only create/lake_schema or attach/table_id options")));
        if(id[0]=='\0') ereport(ERROR,(errcode(ERRCODE_FDW_INVALID_OPTION_DATA),errmsg("table_id cannot be empty")));
        for(const char* c=id;*c!='\0';++c) if(*c<'0' || *c>'9')
            ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("table_id must be a nonnegative integer")));
        result.table_id=DatumGetInt64(DirectFunctionCall1(int8in,CStringGetDatum(id)));
    }
    return result;
}

bool LakeNativeProtectedCreate(Oid relationOid)
{
    return LakeParseTableOptions(GetForeignTable(relationOid)->options).create;
}

void LakeNativeValidateOptions(List* options, Oid catalog)
{
    if(catalog==ForeignTableRelationId) {
        LakeTableOptions parsed=LakeParseTableOptions(options);
        if(!parsed.create) LakeInlineRequireAdministrator();
        else LakeRequireBusinessCreateVersion();
    } else if(options!=NIL) {
        ereport(ERROR,(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),errmsg("native lake does not accept options on this object")));
    }
}

void LakeRequireBusinessCreateVersion(void)
{
    char* version=LakeInstalledVersion();
    if(version==NULL || (strcmp(version,"0.7")!=0&&strcmp(version,"0.8")!=0&&strcmp(version,"0.9")!=0&&(strcmp(version,"0.10")!=0&&strcmp(version,"0.11")!=0&&strcmp(version,"0.12")!=0&&strcmp(version,"0.13")!=0&&(strcmp(version,"0.14")!=0&&(strcmp(version,"0.15")!=0&&(strcmp(version,"0.16")!=0&&(strcmp(version,"0.17")!=0&&(strcmp(version,"0.18")!=0&&(strcmp(version,"0.19")!=0&&(strcmp(version,"0.20")!=0&&(strcmp(version,"0.21")!=0&&(strcmp(version,"0.22")!=0&&(strcmp(version,"0.23")!=0&&!LakeNativeFutureContractEnabled(version))))))))))))))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake business CREATE requires extension 0.7")));
}

void LakeRequireFileReadVersion(void)
{
    char* version=LakeInstalledVersion();
    if(version==NULL||(strcmp(version,"0.8")!=0&&strcmp(version,"0.9")!=0&&(strcmp(version,"0.10")!=0&&strcmp(version,"0.11")!=0&&strcmp(version,"0.12")!=0&&strcmp(version,"0.13")!=0&&(strcmp(version,"0.14")!=0&&(strcmp(version,"0.15")!=0&&(strcmp(version,"0.16")!=0&&(strcmp(version,"0.17")!=0&&(strcmp(version,"0.18")!=0&&(strcmp(version,"0.19")!=0&&(strcmp(version,"0.20")!=0&&(strcmp(version,"0.21")!=0&&(strcmp(version,"0.22")!=0&&(strcmp(version,"0.23")!=0&&!LakeNativeFutureContractEnabled(version))))))))))))))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake file reads require extension 0.8")));
}

bool LakeFileVersionedReadsEnabled(void)
{
    char* version=LakeInstalledVersion();
    return version!=NULL && (strcmp(version,"0.9")==0||(strcmp(version,"0.10")==0||strcmp(version,"0.11")==0||strcmp(version,"0.12")==0||strcmp(version,"0.13")==0||(strcmp(version,"0.14")==0||(strcmp(version,"0.15")==0||(strcmp(version,"0.16")==0||(strcmp(version,"0.17")==0||(strcmp(version,"0.18")==0||(strcmp(version,"0.19")==0||(strcmp(version,"0.20")==0||(strcmp(version,"0.21")==0||(strcmp(version,"0.22")==0||(strcmp(version,"0.23")==0||LakeNativeFutureContractEnabled(version)))))))))))));
}

bool LakeFileModificationsEnabled(void)
{
    char* version=LakeInstalledVersion();
    return version!=NULL && (strcmp(version,"0.10")==0||strcmp(version,"0.11")==0||strcmp(version,"0.12")==0||strcmp(version,"0.13")==0||(strcmp(version,"0.14")==0||(strcmp(version,"0.15")==0||(strcmp(version,"0.16")==0||(strcmp(version,"0.17")==0||(strcmp(version,"0.18")==0||(strcmp(version,"0.19")==0||(strcmp(version,"0.20")==0||(strcmp(version,"0.21")==0||(strcmp(version,"0.22")==0||(strcmp(version,"0.23")==0||LakeNativeFutureContractEnabled(version))))))))))));
}

static void LakeCheckFormat(Oid schemaOid, Snapshot snapshot)
{
    char* metadata = LakeMetadataRelation(schemaOid, "ducklake_metadata");
    LakeMetaRows* rows = LakeMetadataQuery(psprintf(
        "SELECT value::text FROM %s WHERE key='version' AND scope IS NULL", metadata),
        0, NULL, NULL, NULL, snapshot, true, SPI_OK_SELECT, schemaOid);
    LakeMetadataRequireCount(rows, 1, "format version");
    if (strcmp(LakeMetadataText(rows, 0, 1), "1.1") != 0)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("native lake requires DuckLake format 1.1")));
}

/* Shared by every version of the attach C entry point and native creation.
 * Only administrative catalog registration is serialized here. */
static void LakeLockCatalogRegistrationInternal(bool conditional)
{
    Oid ns=get_namespace_oid("og_lake_internal",false);
    Oid oid=get_relname_relid("catalog_binding",ns);
    Oid extension=get_extension_oid("og_lake",false);
    if(!OidIsValid(oid) || getExtensionOfObject(RelationRelationId,oid)!=extension ||
       getExtensionOfObject(NamespaceRelationId,ns)!=extension)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("native lake catalog binding is missing or untrusted")));
    /* No identity switch occurs before this administrative lock. Prove the
     * extension dependency natively first; validate the full policy after the
     * lock so registry reads cannot reverse the common registration order. */
    if(conditional){
        bool locked = false;
        for (int attempt = 0; attempt < 50; ++attempt) {
            if (ConditionalLockRelationOid(oid, ShareRowExclusiveLock)) {
                locked = true;
                break;
            }
            CHECK_FOR_INTERRUPTS();
            pg_usleep(10000L);
        }
        if (!locked)
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                errmsg("native lake ADD COLUMN conflicts with catalog registration")));
    }
    Relation bindings=heap_open(oid,conditional?NoLock:ShareRowExclusiveLock);
    if(bindings->rd_rel->relkind!=RELKIND_RELATION)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("native lake catalog binding must be an ordinary table")));
    heap_close(bindings,NoLock); /* Outer transaction owns the lock. */
    (void)LakeMetadataOwner();
}

void LakeLockCatalogRegistration(void) { LakeLockCatalogRegistrationInternal(false); }
void LakeTryLockCatalogRegistration(void) { LakeLockCatalogRegistrationInternal(true); }

bool LakeColumnEvolutionVersionEnabled(void)
{
    if(!OidIsValid(get_extension_oid("og_lake",true)))return false;
    char* version=LakeInstalledVersion();
    bool enabled=LakeCumulativeContract(version, 37)&&LakeCumulativeContract(version, 36)&&
        LakeCumulativeContract(version, 35)&&LakeCumulativeContract(version, 34)&&LakeCumulativeContract(version, 32)&&
        strcmp(OG_LAKE_TEMPORAL_BUSINESS_SQL_VERSION,"0.31")==0&&
        strcmp(OG_LAKE_BUCKET_PARTITION_SQL_VERSION,"0.30")==0&&
        strcmp(OG_LAKE_FILE_PARTITION_KEYS_SQL_VERSION,"0.29")==0;
    if(version)pfree(version);
    return enabled;
}

bool LakeJournalPayloadSpillVersionEnabled(void)
{
    if(!OidIsValid(get_extension_oid("og_lake",true)))return false;
    char* version=LakeInstalledVersion();
    bool enabled=LakeCumulativeContract(version, 38)&&LakeCumulativeContract(version, 37)&&
        LakeCumulativeContract(version, 36)&&LakeCumulativeContract(version, 35)&&LakeCumulativeContract(version, 34);
    if(version)pfree(version);
    return enabled;
}

static void LakeCheckCatalogNames(const char* serverName, const char* schemaName, bool creating)
{
    if(*serverName=='\0' || *schemaName=='\0')
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("lake server and metadata schema names cannot be empty")));
    if(IsReservedName(schemaName) || isTempNamespaceName(schemaName) ||
       strcmp(schemaName,"information_schema")==0 || strcmp(schemaName,"og_lake")==0 ||
       strcmp(schemaName,"og_lake_internal")==0)
        ereport(ERROR,(errcode(ERRCODE_INVALID_SCHEMA_NAME),errmsg("reserved lake metadata schema")));
    if(GetForeignServerByName(serverName,true)!=NULL)
        ereport(ERROR,(errcode(ERRCODE_DUPLICATE_OBJECT),errmsg("lake foreign server already exists")));
    if(creating && OidIsValid(get_namespace_oid(schemaName,true)))
        ereport(ERROR,(errcode(ERRCODE_DUPLICATE_SCHEMA),errmsg("lake metadata schema already exists")));
}

/* The local-first capability never claims to reserve filesystem objects.
 * Root spelling is canonical only within this host's path namespace. */
static char* LakeNormalizeLocalRoot(const char* input, bool creating)
{
    if(input==NULL || *input=='\0')
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("lake data root cannot be empty")));
    if(*input!='/')
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("new native lake registrations require an absolute local data root"),
            errdetail("Remote storage and relative-root registration require an additional storage adapter.")));
    Size size=strlen(input);
    if(size>=MAXPGPATH-1)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("lake data root is too long")));
    char path[MAXPGPATH]; Size used=1; path[0]='/';
    const char* at=input;
    while(*at!='\0') {
        while(*at=='/') ++at;
        if(*at=='\0') break;
        const char* end=at;
        while(*end!='\0' && *end!='/') ++end;
        Size length=(Size)(end-at);
        if((length==1 && at[0]=='.') || (length==2 && at[0]=='.' && at[1]=='.'))
            ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("lake data root cannot contain dot path components")));
        if(used>1) path[used++]='/';
        errno_t rc=memcpy_s(path+used,sizeof(path)-used,at,length); securec_check(rc,"","");
        used+=length; at=end;
    }
    path[used]='\0';
    if(used==1)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("filesystem root is not a lake data root")));
    struct stat status;
    int found=lstat(path,&status);
    if(found==0) {
        if(creating)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("new lake data root must not already exist")));
        if(S_ISLNK(status.st_mode) || !S_ISDIR(status.st_mode))
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("lake data root must be a directory without symbolic links")));
    } else if(errno!=ENOENT) {
        ereport(ERROR,(errcode_for_file_access(),errmsg("could not inspect lake data root: %m")));
    }
    /* realpath must inspect an existing target or its existing parent. This
     * rejects intermediate symlink spellings; a missing parent is not ENOENT
     * evidence that the root is safe to register. */
    char existing[MAXPGPATH];
    errno_t rc=strcpy_s(existing,sizeof(existing),path); securec_check(rc,"","");
    if(found!=0) {
        char* slash=strrchr(existing,'/');
        if(slash==existing) slash[1]='\0'; else *slash='\0';
    }
    /* POSIX allocated result avoids overflow when symlink expansion exceeds
     * MAXPGPATH, even though such aliases are rejected below. */
    char* resolved=realpath(existing,NULL);
    if(resolved==NULL)
        ereport(ERROR,(errcode_for_file_access(),errmsg("could not resolve lake data root parent: %m")));
    bool samePath=strcmp(existing,resolved)==0;
    free(resolved);
    if(!samePath)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("lake data root cannot use symbolic-link path aliases")));
    if(stat(existing,&status)!=0)
        ereport(ERROR,(errcode_for_file_access(),errmsg("could not inspect lake data root parent: %m")));
    if(!S_ISDIR(status.st_mode))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("lake data root parent is not a directory")));
    return psprintf("%s/",path);
}

static void LakeCheckRootAvailable(const char* canonicalRoot)
{
    /* Must be called only after registration lock acquisition. SecondarySnapshot
     * is newer than a statement snapshot that was taken before waiting. */
    Snapshot snapshot=RegisterSnapshot(GetLatestSnapshot());
    PG_TRY();
    {
        bool indexRoots=LakeHasIndexRootVersion();
        LakeMetaRows* roots=LakeMetadataQuery(indexRoots?
            "SELECT data_root::text,index_root::text FROM og_lake_internal.catalog_binding":
            "SELECT data_root::text FROM og_lake_internal.catalog_binding",0,NULL,NULL,NULL,
            snapshot,true,SPI_OK_SELECT);
        Size incomingLength=strlen(canonicalRoot);
        for(uint32 i=0;i<roots->count;++i) {
            for(int column=1;column<=(indexRoots?2:1);++column) {
                bool absent=false;Datum value=LakeMetadataValue(roots,i,column,TEXTOID,true,&absent);
                if(absent)continue;
                char* existing=LakeNormalizeLocalRoot(TextDatumGetCString(value),false);
                Size common=Min(incomingLength,strlen(existing));
                if(strncmp(canonicalRoot,existing,common)==0)
                    ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("lake data root overlaps a registered data or derived-index root"),
                        errdetail("Equal roots and ancestor/descendant roots cannot be registered to different stores.")));
            }
        }
        UnregisterSnapshot(snapshot);
    }
    PG_CATCH();
    {
        UnregisterSnapshot(snapshot);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

static void LakeRequireEmptyIndexRoot(const char* root)
{
    DIR* directory=opendir(root);
    if(directory==NULL)ereport(ERROR,(errcode_for_file_access(),errmsg("cannot inspect index_root: %m")));
    bool empty=true;struct dirent* entry;errno=0;
    while((entry=readdir(directory))!=NULL){
        if(strcmp(entry->d_name,".")!=0&&strcmp(entry->d_name,"..")!=0){empty=false;break;}
    }
    int saved=errno,closed=closedir(directory);
    if(saved!=0||closed!=0){if(saved)errno=saved;ereport(ERROR,(errcode_for_file_access(),errmsg("cannot finish inspecting index_root: %m")));}
    if(!empty)ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        errmsg("index_root must remain pinned while it contains artifacts or orphan directories"),
        errhint("Use a dedicated empty index directory. Relocation requires protected artifact cleanup.")));
}

/* Host-owned derived artifacts never enter DuckLake's standard data root.
 * Root selection is administrative metadata, not a GUC or a business snapshot. */
void LakeSetCatalogIndexRoot(const char* serverName,const char* input)
{
    LakeInlineRequireAdministrator();
    if(!LakeHasIndexRootVersion())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("index_root requires extension0.16")));
    if(u_sess->attr.attr_common.XactReadOnly)
        ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("cannot set index_root in a read-only transaction")));
    LakeTxnRejectBindingDdl();LakeLockCatalogRegistration();
    ForeignServer* server=GetForeignServerByName(serverName,false);
    Snapshot fresh=RegisterSnapshot(GetLatestSnapshot());
    PG_TRY();
    {
        LakeInlineBinding* binding=LakeLoadCatalogHeader(server->serverid,fresh);
        if(binding->read_only)
            ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("cannot configure a read-only lake index store")));
        char* root=LakeNormalizeLocalRoot(input,false);
        if(strlen(root)>MAXPGPATH-128)
            ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("index_root leaves insufficient space for artifact paths")));
        struct stat status;
        if(stat(root,&status)!=0)
            ereport(ERROR,(errcode_for_file_access(),errmsg("index_root must be an existing provisioned directory: %m")));
        if(!S_ISDIR(status.st_mode))
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("index_root is not a directory")));
        Oid type=OIDOID;Datum serverArg=ObjectIdGetDatum(server->serverid);
        LakeMetaRows* previous=LakeMetadataQuery("SELECT index_root::text FROM og_lake_internal.catalog_binding WHERE server_oid=$1",
            1,&type,&serverArg,NULL,fresh,true,SPI_OK_SELECT);
        LakeMetadataRequireCount(previous,1,"index store catalog");
        bool absent=false;Datum value=LakeMetadataValue(previous,0,1,TEXTOID,true,&absent);
        if(absent || strcmp(TextDatumGetCString(value),root)!=0) {
            LakeRequireEmptyIndexRoot(root);
            if(!absent)LakeRequireEmptyIndexRoot(TextDatumGetCString(value));
            LakeMetaRows* pinned=LakeMetadataQuery("SELECT EXISTS(SELECT 1 FROM og_lake_internal.index_segment s "
                "JOIN og_lake_internal.index_definition d ON d.index_uuid=s.index_uuid WHERE d.server_oid=$1)",
                1,&type,&serverArg,NULL,fresh,true,SPI_OK_SELECT);
            LakeMetadataRequireCount(pinned,1,"derived-index root retention");
            if(DatumGetBool(LakeMetadataValue(pinned,0,1,BOOLOID)))
                ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("index_root is pinned by retained index segments"),
                    errdetail("Index relocation requires protected artifact maintenance; changing the root does not move files.")));
            LakeMetaRows* roots=LakeMetadataQuery("SELECT server_oid,data_root::text,index_root::text FROM og_lake_internal.catalog_binding",
                0,NULL,NULL,NULL,fresh,true,SPI_OK_SELECT);
            for(uint32 i=0;i<roots->count;++i) {
                Oid existingServer=DatumGetObjectId(LakeMetadataValue(roots,i,1,OIDOID));
                for(int column=2;column<=3;++column) {
                    if(column==3 && existingServer==server->serverid)continue;
                    bool missing=false;Datum existing=LakeMetadataValue(roots,i,column,TEXTOID,true,&missing);
                    if(missing)continue;
                    char* canonical=LakeNormalizeLocalRoot(TextDatumGetCString(existing),false);
                    if(strncmp(root,canonical,Min(strlen(root),strlen(canonical)))==0)
                        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                            errmsg("index_root overlaps a registered data or index store")));
                }
            }
            Oid types[2]={OIDOID,TEXTOID};Datum args[2]={serverArg,CStringGetTextDatum(root)};
            LakeMetaRows* changed=LakeMetadataQuery("UPDATE og_lake_internal.catalog_binding SET index_root=$2 WHERE server_oid=$1",
                2,types,args,NULL,fresh,false,SPI_OK_UPDATE);
            LakeMetadataRequireCount(changed,1,"derived-index root option writes");
            if(LakeExtendedDefaultsVersionEnabled())
                LakeInvalidateCatalogRelations(server->serverid,fresh);
        }
        UnregisterSnapshot(fresh);
    }
    PG_CATCH();{UnregisterSnapshot(fresh);PG_RE_THROW();}PG_END_TRY();
}

void LakeInvalidateRelationSafe(Oid relationOid)
{
    if(!LakeExtendedDefaultsVersionEnabled()||!OidIsValid(relationOid))return;
    if(SearchSysCacheExists1(RELOID,ObjectIdGetDatum(relationOid)))
        CacheInvalidateRelcacheByRelid(relationOid);
}

void LakeInvalidateCatalogRelations(Oid serverOid, Snapshot snapshot)
{
    if(!LakeExtendedDefaultsVersionEnabled()||!OidIsValid(serverOid))return;
    Oid types[1]={OIDOID};Datum args[1]={ObjectIdGetDatum(serverOid)};
    LakeMetaRows* rows=LakeMetadataQuery(
        "SELECT DISTINCT r.relation_oid FROM og_lake_internal.relation_binding r "
        "JOIN og_lake_internal.table_binding t USING(binding_id) "
        "WHERE t.server_oid=$1 AND t.state='active'",
        1,types,args,NULL,snapshot,true,SPI_OK_SELECT);
    for(uint32 i=0;i<rows->count;++i){
        bool absent=false;
        Datum rel=LakeMetadataValue(rows,i,1,OIDOID,true,&absent);
        if(!absent)LakeInvalidateRelationSafe(DatumGetObjectId(rel));
    }
}

/* Utility bootstrap runs as the already-authorized administrator, never inside
 * a private metadata scope. No dynamic expression or user data enters this SQL. */
static void LakeCreateUtility(const char* sql)
{
    if(LakeGetMetadataScope()!=NULL)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("lake catalog creation cannot run inside a metadata identity scope")));
    MemoryContext caller=CurrentMemoryContext;
    volatile bool connected=false;
    volatile bool pathPushed=false;
    PG_TRY();
    {
        OverrideSearchPath path;
        errno_t rc=memset_s(&path,sizeof(path),0,sizeof(path)); securec_check(rc,"","");
        path.schemas=list_make1_oid(PG_CATALOG_NAMESPACE);
        PushOverrideSearchPath(&path); pathPushed=true;
        if(SPI_connect()!=SPI_OK_CONNECT)
            ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("could not connect catalog DDL SPI")));
        connected=true;
        int result=SPI_execute(sql,false,0);
        if(result!=SPI_OK_UTILITY)
            ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("unexpected catalog DDL result %d",result)));
        int finish=SPI_finish(); connected=false;
        if(finish!=SPI_OK_FINISH)
            ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("could not finish catalog DDL SPI")));
        PopOverrideSearchPath(); pathPushed=false;
        MemoryContextSwitchTo(caller);
        CommandCounterIncrement();
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);
        if(connected) (void)SPI_finish();
        if(pathPushed) PopOverrideSearchPath();
        PG_RE_THROW();
    }
    PG_END_TRY();
}

/* Copies a completely verified table description into query-owned memory.
 * All metadata reads use the caller's host snapshot, including the snapshot MAX.
 */
static LakeInlineBinding* LakeLoadCatalogHeaderImpl(Oid serverOid, Snapshot snapshot)
{
    LakeRequireAccessPolicy();
    if (!LakeIsServer(serverOid))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("server is not an og_lake_fdw server")));
    LakeAcquireCatalogGuard(serverOid);
    Oid oidType[1] = {OIDOID};
    Datum oidArg[1] = {ObjectIdGetDatum(serverOid)};
    LakeMetaRows* catalog = LakeMetadataQuery(
        "SELECT metadata_schema_oid, read_only, format_version::text, data_root, catalog_id, binding_generation "
        "FROM og_lake_internal.catalog_binding WHERE server_oid=$1",
        1, oidType, oidArg, NULL, snapshot, true, SPI_OK_SELECT);
    LakeMetadataRequireCount(catalog, 1, "catalog binding");
    if (strcmp(LakeMetadataText(catalog, 0, 3), "1.1") != 0)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("only DuckLake 1.1 bindings are enabled")));
    LakeInlineBinding* binding = (LakeInlineBinding*)palloc0(sizeof(LakeInlineBinding));
    binding->read_only = DatumGetBool(LakeMetadataValue(catalog, 0, 2, BOOLOID));
    binding->server_oid = serverOid;
    binding->metadata_schema_oid = DatumGetObjectId(LakeMetadataValue(catalog, 0, 1, OIDOID));
    binding->metadata_schema = get_namespace_name(binding->metadata_schema_oid);
    binding->data_root = LakeMetadataText(catalog,0,4);
    pg_uuid_t* catalogUuid = DatumGetUUIDP(LakeMetadataValue(catalog, 0, 5, UUIDOID));
    errno_t rc = memcpy_s(binding->catalog_id, UUID_LEN, catalogUuid->data, UUID_LEN);
    securec_check(rc, "", "");
    binding->binding_generation = DatumGetInt64(LakeMetadataValue(catalog, 0, 6, INT8OID));
    if (binding->binding_generation <= 0)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid native lake catalog binding generation")));
    binding->has_catalog_identity = true;
    LakeCheckFormat(binding->metadata_schema_oid, snapshot);
    char* snapshots = LakeMetadataRelation(binding->metadata_schema_oid, "ducklake_snapshot");
    LakeMetaRows* latest = LakeMetadataQuery(psprintf(
        "SELECT snapshot_id, schema_version FROM %s ORDER BY snapshot_id DESC LIMIT 1", snapshots),
        0, NULL, NULL, NULL, snapshot, true, SPI_OK_SELECT, binding->metadata_schema_oid);
    LakeMetadataRequireCount(latest, 1, "visible snapshot");
    binding->snapshot_id = DatumGetInt64(LakeMetadataValue(latest, 0, 1, INT8OID));
    binding->schema_version = DatumGetInt64(LakeMetadataValue(latest, 0, 2, INT8OID));
    if (binding->snapshot_id < 0 || binding->schema_version < 0)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid native lake snapshot identity")));
    return binding;
}

LakeInlineBinding* LakeLoadCatalogHeader(Oid serverOid, Snapshot snapshot)
{
    int fastScope = LakeEnterFastValidationScope();
    LakeInlineBinding* volatile binding = NULL;
    PG_TRY();
    {
        binding = LakeLoadCatalogHeaderImpl(serverOid, snapshot);
    }
    PG_CATCH();
    {
        LakeLeaveFastValidationScope(fastScope);
        PG_RE_THROW();
    }
    PG_END_TRY();
    LakeLeaveFastValidationScope(fastScope);
    return binding;
}

/* Standard nested entries are catalog children, not extra business columns.
 * Only LIST<float32> is admitted; malformed or richer trees fail explicitly. */
static LakeInlineColumn* LakeReadColumnTree(LakeMetaRows* rows,int* rootCount)
{
    if(rows->count==0||rows->count>2*MaxHeapAttributeNumber)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("unsupported native lake column tree size")));
    int count=0;
    int64* ids=(int64*)palloc(sizeof(int64)*rows->count);
    int64* parents=(int64*)palloc(sizeof(int64)*rows->count);
    bool* roots=(bool*)palloc(sizeof(bool)*rows->count);
    bool* used=(bool*)palloc0(sizeof(bool)*rows->count);
    for(uint32 i=0;i<rows->count;++i){
        ids[i]=DatumGetInt64(LakeMetadataValue(rows,i,1,INT8OID));
        Datum parent=LakeMetadataValue(rows,i,4,INT8OID,true,&roots[i]);
        parents[i]=roots[i]?-1:DatumGetInt64(parent);
        if(ids[i]<0||(!roots[i]&&parents[i]<0))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake column tree identity")));
        for(uint32 j=0;j<i;++j)if(ids[j]==ids[i])
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate native lake column tree identity")));
        if(roots[i])++count;
    }
    if(count<1||count>MaxHeapAttributeNumber)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("unsupported native lake business column count")));
    LakeInlineColumn* columns=(LakeInlineColumn*)palloc0(sizeof(LakeInlineColumn)*count);
    int position=0;
    for(uint32 i=0;i<rows->count;++i){
        if(!roots[i])continue;
        LakeInlineColumn* column=&columns[position];used[i]=true;
        column->column_id=ids[i];column->name=LakeMetadataText(rows,i,2);
        column->output_attnum=++position;column->typmod=-1;column->element_column_id=-1;
        if(column->name[0]=='\0'||strlen(column->name)>=NAMEDATALEN||strncmp(column->name,"_ducklake_",10)==0)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid or reserved lake column identity")));
        for(int j=0;j<position-1;++j)if(strcmp(columns[j].name,column->name)==0)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate native lake business column name")));
        const char* logical=LakeMetadataText(rows,i,3);int child=-1;
        for(uint32 j=0;j<rows->count;++j)if(!roots[j]&&parents[j]==ids[i]){
            if(child!=-1)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake LIST requires exactly one element column")));
            child=(int)j;
        }
        if(strcmp(logical,"list")==0){
            if(child<0||strcmp(LakeMetadataText(rows,child,3),"float32")!=0)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake vector columns require LIST<float32>")));
            column->type=VECTOROID;column->element_column_id=ids[child];used[child]=true;
        }else{
            if(child!=-1)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake scalar column has nested children")));
            column->type=LakeInlineType(logical);
            if(column->type==NUMERICOID)column->typmod=LakeNumericDuckTypmod(logical);
            LakeNativeTypeRequireBusinessVersion(column->type);
        }
    }
    for(uint32 i=0;i<rows->count;++i)if(!used[i])
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake column tree has an unbound or unsupported descendant")));
    pfree(ids);pfree(parents);pfree(roots);pfree(used);*rootCount=count;return columns;
}

/* Partition-only schema changes rotate the current writer while preserving
 * every older registered carrier. All descriptors belong to this binding's
 * memory context and are resolved under the caller's single host snapshot. */
static int64 LakeInlineSourceOrigin(LakeInlineBinding* binding,int64 version,Snapshot snapshot)
{
    Oid types[2]={INT8OID,INT8OID};
    Datum args[2]={Int64GetDatum(LakePermanentTableId(binding)),Int64GetDatum(version)};
    LakeMetaRows* row=LakeMetadataQuery(psprintf(
        "SELECT begin_snapshot FROM %s WHERE table_id=$1 AND schema_version=$2",
        LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_schema_versions")),
        2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    /* Unlike an unbound reference table, native CREATE records every source
     * schema origin explicitly. Missing history must never guess a partition. */
    LakeMetadataRequireCount(row,1,"native CREATE inline schema origin");
    int64 origin=DatumGetInt64(LakeMetadataValue(row,0,1,INT8OID));
    if(origin<0||origin>binding->snapshot_id)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid inline schema origin")));
    return origin;
}

/* Read original introduction from ALL retained field versions. RENAME ends and
 * re-adds a row with the same ID; its current begin_snapshot is not introduction.
 * Native CREATE provenance is established by the protected binding_mode path. */
bool LakeVerifyColumnTypeChain(Oid ns, int64 tableId, int64 columnId, int64 maxSnapshot,
    Oid expectedFinalType, int32 expectedFinalTypmod, Oid* earliestTypeOut, int32* earliestTypmodOut,
    bool* promotedOut, Snapshot snapshot)
{
    Oid types[3] = {INT8OID, INT8OID, INT8OID};
    Datum args[3] = {Int64GetDatum(tableId), Int64GetDatum(columnId), Int64GetDatum(maxSnapshot)};
    const char* rel = LakeMetadataRelation(ns, "ducklake_column");
    LakeMetaRows* rows = LakeMetadataQuery(psprintf(
        "SELECT column_type::text, begin_snapshot, end_snapshot, parent_column IS NULL "
        "FROM %s WHERE table_id=$1 AND column_id=$2 AND begin_snapshot<=$3 "
        "ORDER BY begin_snapshot", rel),
        3, types, args, NULL, snapshot, true, SPI_OK_SELECT, ns);
    if (rows->count == 0)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("native column history chain is empty")));
    Oid prevType = InvalidOid;
    int32 prevTypmod = -1;
    int64 prevEnd = -1;
    bool promoted = false;
    for (uint32 r = 0; r < rows->count; ++r) {
        if (!DatumGetBool(LakeMetadataValue(rows, r, 4, BOOLOID)))
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("native scalar field history has nested parent")));
        const char* tStr = LakeMetadataText(rows, r, 1);
        int64 begin = DatumGetInt64(LakeMetadataValue(rows, r, 2, INT8OID));
        bool endNull = false;
        Datum endDat = LakeMetadataValue(rows, r, 3, INT8OID, true, &endNull);
        int64 end = endNull ? -1 : DatumGetInt64(endDat);
        if (begin < 0 || (!endNull && end <= begin))
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid snapshot bounds in native column history")));
        if (r > 0 && prevEnd != begin)
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("non-contiguous native column version chain")));
        if (r + 1 < rows->count && (endNull || end > maxSnapshot))
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("intermediate native column version lacks end_snapshot")));
        Oid curType = LakeInlineType(tStr);
        int32 curTypmod = curType == NUMERICOID ? LakeNumericDuckTypmod(tStr) : -1;
        if (r == 0) {
            if (earliestTypeOut) *earliestTypeOut = curType;
            if (earliestTypmodOut) *earliestTypmodOut = curTypmod;
        } else {
            if (!LakeIsTypeCompatibleOrPromoted(prevType, prevTypmod, curType, curTypmod))
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("native stable field history performed an unsupported or narrowing type change")));
            if (curType != prevType || curTypmod != prevTypmod)
                promoted = true;
        }
        prevType = curType;
        prevTypmod = curTypmod;
        prevEnd = end;
    }
    if (prevType != expectedFinalType || prevTypmod != expectedFinalTypmod)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("native column history terminal type differs from target field definition")));
    if (promotedOut) *promotedOut = promoted;
    return true;
}

static void LakeReadColumnIntroductions(LakeInlineBinding* binding,Snapshot snapshot)
{
    Oid tid=INT8OID;Datum tableArg=Int64GetDatum(LakePermanentTableId(binding));
    LakeMetaRows* origin=LakeMetadataQuery(psprintf("SELECT min(begin_snapshot) FROM %s WHERE table_id=$1",
        LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_table")),1,&tid,&tableArg,NULL,
        snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    LakeMetadataRequireCount(origin,1,"native table introduction");
    int64 tableStart=DatumGetInt64(LakeMetadataValue(origin,0,1,INT8OID));
    if(tableStart<0||tableStart>binding->snapshot_id)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native table introduction")));
    LakeReserveColumnMapBytes(binding,sizeof(LakeInlineColumn)*binding->column_count+
        (Size)binding->column_count*2*NAMEDATALEN);
    MemoryContext owner=CurrentMemoryContext;
    MemoryContext scratch=AllocSetContextCreate(owner,"Native batched field introductions",ALLOCSET_DEFAULT_SIZES);
    PG_TRY();{
        MemoryContextSwitchTo(scratch);
        Datum* ids=(Datum*)palloc(sizeof(Datum)*binding->column_count);
        for(int c=0;c<binding->column_count;++c)ids[c]=Int64GetDatum(binding->columns[c].column_id);
        Oid types[3]={INT8OID,INT8ARRAYOID,INT8OID};
        Datum args[3]={tableArg,PointerGetDatum(construct_array(ids,binding->column_count,INT8OID,8,true,'d')),
            Int64GetDatum(binding->snapshot_id)};
        /* One same-snapshot history pass for the exact live IDs. RENAME and
         * ended versions remain in the aggregates; children/retired IDs are
         * neither silently admitted nor used as a substitute for a live ID. */
        const char* nullable=binding->not_null_constraints?"nulls_allowed IS NOT NULL":"nulls_allowed IS TRUE";
        LakeMetaRows* history=LakeMetadataQuery(psprintf(
            "SELECT column_id,min(begin_snapshot),bool_and(parent_column IS NULL AND %s "
            "AND (initial_default IS NULL OR initial_default='NULL')),min(column_type::text),max(column_type::text),"
            "(bool_and(COALESCE(parent_column IS NULL AND %s AND column_type IS NOT NULL "
            "AND octet_length(column_type)<=64 "
            "AND initial_default IS NULL AND (CASE WHEN octet_length(default_value)=4 THEN "
            "pg_catalog.textsend(default_value::text)=pg_catalog.textsend('NULL'::text) ELSE default_value IS NULL END) "
            "AND (CASE WHEN octet_length(default_value_type)=7 THEN "
            "pg_catalog.textsend(default_value_type::text)=pg_catalog.textsend('literal'::text) ELSE false END) "
            "AND (CASE WHEN octet_length(default_value_dialect)=6 THEN "
            "pg_catalog.textsend(default_value_dialect::text)=pg_catalog.textsend('duckdb'::text) ELSE false END) "
            "AND begin_snapshot>=0 AND (end_snapshot IS NULL OR end_snapshot>begin_snapshot),false)) "
            "AND count(*)=count(DISTINCT begin_snapshot) "
            "AND count(DISTINCT CASE WHEN octet_length(column_type)<=64 THEN pg_catalog.textsend(column_type::text) END)=1 "
            "AND count(CASE WHEN end_snapshot IS NULL OR end_snapshot>$3 THEN 1 END)=1 "
            "AND %s) FROM %s WHERE table_id=$1 AND column_id=ANY($2) AND begin_snapshot<=$3 "
            "GROUP BY column_id ORDER BY column_id",
            nullable,nullable,binding->alter_defaults?"true":
            "(count(default_value)=0 OR bool_and(COALESCE(CASE WHEN octet_length(default_value)=4 THEN "
            "pg_catalog.textsend(default_value::text)=pg_catalog.textsend('NULL'::text) ELSE false END,false)))",
            LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_column")),
            3,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
        LakeMetadataRequireCount(history,binding->column_count,"native field introductions");
        for(int c=0;c<binding->column_count;++c){
            CHECK_FOR_INTERRUPTS();
            LakeInlineColumn* field=&binding->columns[c];
            uint32 low=0,high=history->count;
            while(low<high){
                uint32 middle=low+(high-low)/2;
                if(DatumGetInt64(LakeMetadataValue(history,middle,1,INT8OID))<field->column_id)low=middle+1;
                else high=middle;
            }
            if(low==history->count||DatumGetInt64(LakeMetadataValue(history,low,1,INT8OID))!=field->column_id)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native field introduction history is missing")));
            field->introduced_snapshot=DatumGetInt64(LakeMetadataValue(history,low,2,INT8OID));
            if(field->introduced_snapshot<tableStart||field->introduced_snapshot>binding->snapshot_id)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native stable field history changed identity or type")));
            if(strcmp(LakeMetadataText(history,low,4),LakeMetadataText(history,low,5))!=0){
                if(!binding->alter_type_evolution)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native stable field history changed identity or type")));
                bool promoted=false;
                (void)LakeVerifyColumnTypeChain(binding->metadata_schema_oid,LakePermanentTableId(binding),
                    field->column_id,binding->snapshot_id,field->type,field->typmod,NULL,NULL,&promoted,snapshot);
                if(promoted)binding->has_altered_types=true;
            }
            field->initial_null_proven=field->element_column_id<0&&field->introduced_snapshot>tableStart&&
                DatumGetBool(LakeMetadataValue(history,low,3,BOOLOID));
            if(binding->literal_defaults){
                bool proofNull=false;
                Datum proof=LakeMetadataValue(history,low,6,BOOLOID,true,&proofNull);
                MemoryContextSwitchTo(owner);
                if(field->element_column_id<0&&!proofNull&&DatumGetBool(proof)){
                    /* A sufficient proof of precisely the original NULL-only
                     * result. All other histories use the full original reader,
                     * including its errors, byte budgets and scratch lifetime. */
                    LakeReserveColumnMapBytes(binding,0);
                    field->initial_literal=NULL;field->current_literal=NULL;
                    field->initial_value=(Datum)0;field->defaults_validated=true;
                    field->initial_null_proven=field->introduced_snapshot>tableStart;
                    field->initial_value_proven=false;
                }else LakeReadColumnDefault(binding,field,tableStart,snapshot);
                MemoryContextSwitchTo(scratch);
            }
            if(field->introduced_snapshot>tableStart)binding->has_added_columns=true;
        }
        MemoryContextSwitchTo(owner);MemoryContextDelete(scratch);
    }PG_CATCH();{
        MemoryContextSwitchTo(owner);MemoryContextDelete(scratch);PG_RE_THROW();
    }PG_END_TRY();
}

/* Retirements are final top-level identities, not ended RENAME/default
 * versions. A single protected map row remains for every original host slot. */
static void LakeReadRetiredColumns(LakeInlineBinding* binding,Snapshot snapshot,bool withDefaults=true)
{
    if(!binding->drop_columns)return;
    MemoryContext owner=CurrentMemoryContext;
    MemoryContext scratch=AllocSetContextCreate(owner,"Native retired field history",ALLOCSET_DEFAULT_SIZES);
    PG_TRY();{
        MemoryContextSwitchTo(scratch);
        Oid types[3]={INT8OID,INT8OID,INT8OID};
        Datum args[3]={Int64GetDatum(LakePermanentTableId(binding)),Int64GetDatum(binding->snapshot_id),0};
        const char* rel=LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_column");
        LakeMetaRows* ids=LakeMetadataQuery(psprintf(
            "SELECT column_id FROM %s WHERE table_id=$1 AND begin_snapshot<=$2 AND parent_column IS NULL "
            "GROUP BY column_id HAVING bool_and(end_snapshot IS NOT NULL AND end_snapshot<=$2) "
            "ORDER BY column_id LIMIT %d",rel,MaxHeapAttributeNumber+1),
            2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
        if(ids->count>MaxHeapAttributeNumber)
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native retired field count exceeds the host bound")));
        binding->retired_count=(int)ids->count;binding->has_dropped_columns=ids->count!=0;
        LakeReserveColumnMapBytes(binding,(Size)ids->count*(sizeof(LakeRetiredColumn)+2*NAMEDATALEN));
        binding->retired_columns=ids->count?(LakeRetiredColumn*)MemoryContextAllocZero(owner,sizeof(LakeRetiredColumn)*ids->count):NULL;
        LakeMetaRows* table=LakeMetadataQuery(psprintf("SELECT min(begin_snapshot) FROM %s WHERE table_id=$1",
            LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_table")),1,types,args,NULL,
            snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
        LakeMetadataRequireCount(table,1,"retired native table origin");
        int64 tableStart=DatumGetInt64(LakeMetadataValue(table,0,1,INT8OID));
        for(uint32 i=0;i<ids->count;++i){
            int64 id=DatumGetInt64(LakeMetadataValue(ids,i,1,INT8OID));args[2]=Int64GetDatum(id);
            int64 reserved=Min(Min(LAKE_FILE_ROW_ID,LAKE_FILE_SNAPSHOT_ID),Min(LAKE_DELETE_PATH_ID,LAKE_DELETE_POSITION_ID));
            if(id<0||id>=reserved)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("retired native field collides with hidden field namespace")));
            LakeMetaRows* children=LakeMetadataQuery(psprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE table_id=$1 AND parent_column=$3)",rel),
                3,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
            LakeMetadataRequireCount(children,1,"retired scalar child exclusion");
            if(DatumGetBool(LakeMetadataValue(children,0,1,BOOLOID)))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("retired scalar field has a nested history")));
            LakeMetaRows* proof=LakeMetadataQuery(psprintf(
                "SELECT min(begin_snapshot),max(end_snapshot),min(column_type::text),"
                "bool_and(COALESCE(parent_column IS NULL AND column_id>=0 AND column_order=column_id "
                "AND begin_snapshot>=0 AND end_snapshot>begin_snapshot AND end_snapshot<=$2 "
                "AND (next_begin IS NULL OR end_snapshot=next_begin),false)),"
                "count(DISTINCT pg_catalog.textsend(column_type::text))=1,"
                "count(*)=count(DISTINCT begin_snapshot) "
                "FROM(SELECT *,lead(begin_snapshot) OVER(ORDER BY begin_snapshot) AS next_begin "
                "FROM %s WHERE table_id=$1 AND column_id=$3) h",rel),
                3,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
            LakeMetadataRequireCount(proof,1,"retired native field lifetime");
            if(!DatumGetBool(LakeMetadataValue(proof,0,4,BOOLOID))||
               !DatumGetBool(LakeMetadataValue(proof,0,6,BOOLOID)))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native retired field history is not one complete stable identity")));
            bool sameRetiredType=DatumGetBool(LakeMetadataValue(proof,0,5,BOOLOID));
            if(!sameRetiredType&&!binding->alter_type_evolution)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native retired field history is not one complete stable identity")));
            LakeRetiredColumn* retired=&binding->retired_columns[i];LakeInlineColumn* f=&retired->column;
            f->column_id=id;f->introduced_snapshot=DatumGetInt64(LakeMetadataValue(proof,0,1,INT8OID));
            retired->retired_snapshot=DatumGetInt64(LakeMetadataValue(proof,0,2,INT8OID));
            LakeMetaRows* last=LakeMetadataQuery(psprintf("SELECT column_name::text,nulls_allowed,column_type::text FROM %s "
                "WHERE table_id=$1 AND column_id=$3 AND begin_snapshot<=$2 ORDER BY begin_snapshot DESC LIMIT 1",rel),
                3,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
            LakeMetadataRequireCount(last,1,"retired native final field");
            const char* retiredTypeStr=LakeMetadataText(last,0,3);
            f->type=LakeInlineType(retiredTypeStr);f->typmod=f->type==NUMERICOID?LakeNumericDuckTypmod(retiredTypeStr):-1;f->element_column_id=-1;
            if(!sameRetiredType){
                bool promoted=false;
                (void)LakeVerifyColumnTypeChain(binding->metadata_schema_oid,LakePermanentTableId(binding),
                    id,binding->snapshot_id,f->type,f->typmod,NULL,NULL,&promoted,snapshot);
                if(promoted)binding->has_altered_types=true;
            }
            if(f->type==BYTEAOID||f->type==UUIDOID||f->type==JSONOID||f->type==NUMERICOID)LakeNativeTypeRequireBusinessVersion(f->type);
            if(f->introduced_snapshot<tableStart||retired->retired_snapshot>binding->snapshot_id||
               retired->retired_snapshot<=f->introduced_snapshot)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native retired field lifetime is outside the table")));
            const char* name=LakeMetadataText(last,0,1);
            if(!*name||strlen(name)>=NAMEDATALEN)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid retired native field name")));
            f->name=MemoryContextStrdup(owner,name);f->not_null=!DatumGetBool(LakeMetadataValue(last,0,2,BOOLOID));
            if(withDefaults){
                /* Same literal/default parser, all retained initial bytes and
                 * per-version current representation checks as living IDs. */
                LakeInlineBinding historical=*binding;historical.snapshot_id=retired->retired_snapshot-1;
                MemoryContextSwitchTo(owner);LakeReadColumnDefault(&historical,f,tableStart,snapshot);
                binding->column_map_bytes=historical.column_map_bytes;MemoryContextSwitchTo(scratch);
            }
        }
        MemoryContextSwitchTo(owner);MemoryContextDelete(scratch);
    }PG_CATCH();{MemoryContextSwitchTo(owner);MemoryContextDelete(scratch);PG_RE_THROW();}PG_END_TRY();
}

static void LakeInlineSourceColumns(LakeInlineBinding* binding,LakeInlineSource* item,Snapshot snapshot)
{
    Oid types[2]={INT8OID,INT8OID};
    Datum args[2]={Int64GetDatum(LakePermanentTableId(binding)),Int64GetDatum(item->origin_snapshot)};
    LakeMetaRows* rows=LakeMetadataQuery(psprintf(
        "SELECT column_id,column_name::text,column_type::text,parent_column FROM %s "
        "WHERE table_id=$1 AND begin_snapshot<=$2 AND (end_snapshot IS NULL OR end_snapshot>$2) "
        "ORDER BY column_order LIMIT %d",LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_column"),
        2*MaxHeapAttributeNumber+1),2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    if(rows->count>2*MaxHeapAttributeNumber)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native physical source field bound exceeded")));
    LakeReserveColumnMapBytes(binding,(Size)rows->count*(sizeof(LakeInlineColumn)+sizeof(char*)+2*NAMEDATALEN)+
        (Size)binding->column_count*sizeof(int));
    int count=0;LakeInlineColumn* old=LakeReadColumnTree(rows,&count);
    if(binding->not_null_constraints)LakeReadFieldNullability(binding,old,count,item->origin_snapshot,snapshot);
    if((!binding->drop_columns&&count>binding->column_count)||(!binding->column_evolution&&count!=binding->column_count))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("inline schema evolution is not enabled")));
    item->column_count=count;item->columns=old;
    item->column_names=(char**)palloc0(sizeof(char*)*count);
    item->logical_to_source=(int*)palloc(sizeof(int)*binding->column_count);
    bool* used=(bool*)palloc0(sizeof(bool)*count);int previous=-1;
    for(int c=0;c<binding->column_count;++c){
        const LakeInlineColumn* current=&binding->columns[c];int physical=-1;
        for(int k=0;k<count;++k)if(old[k].column_id==current->column_id)physical=k;
        item->logical_to_source[c]=physical;
        if(physical<0){
            if(!LakeColumnCanBeMissing(binding,c,item->origin_snapshot))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("historical inline field lacks an initial value proof")));
            continue;
        }
        bool typeOk=binding->alter_type_evolution ?
            LakeIsTypeCompatibleOrPromoted(old[physical].type,old[physical].typmod,current->type,current->typmod) :
            (old[physical].type==current->type&&(current->type!=NUMERICOID||old[physical].typmod==current->typmod));
        if(physical<=previous||!typeOk||old[physical].element_column_id!=current->element_column_id||
           (!LakeHasColumnRenameVersion()&&strcmp(old[physical].name,current->name)!=0))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("inline column identity, order or type changed")));
        previous=physical;used[physical]=true;
        if(old[physical].type==VECTOROID)old[physical].typmod=current->typmod;
        old[physical].introduced_snapshot=current->introduced_snapshot;LakeCopyColumnDefault(&old[physical],current,binding);
    }
    for(int k=0;k<count;++k){
        item->column_names[k]=pstrdup(old[k].name);
        if(used[k])continue;
        const LakeRetiredColumn* r=LakeFindRetiredColumn(binding,old[k].column_id);
        LakeValidateRetiredBirth(binding,old[k].column_id,item->origin_snapshot,item->origin_snapshot);
        bool retiredTypeOk=binding->alter_type_evolution ?
            LakeIsTypeCompatibleOrPromoted(old[k].type,old[k].typmod,r->column.type,r->column.typmod) :
            (old[k].type==r->column.type&&(r->column.type!=NUMERICOID||old[k].typmod==r->column.typmod));
        if(!retiredTypeOk||old[k].element_column_id!=-1)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("retired inline field changed its certified type")));
        if(old[k].type==VECTOROID)old[k].typmod=r->column.typmod;
        old[k].introduced_snapshot=r->column.introduced_snapshot;
        /* Preserve physical name and origin nullability, never current constraints. */
        LakeCopyColumnDefault(&old[k],&r->column,binding);
    }
    pfree(used);
}

const LakeInlineColumn* LakeFindFutureColumn(const LakeInlineBinding* binding, int64 columnId)
{
    if (!binding || !binding->historical_read || binding->future_count <= 0)
        return NULL;
    for (int i = 0; i < binding->future_count; ++i)
        if (binding->future_columns[i].column_id == columnId)
            return &binding->future_columns[i];
    return NULL;
}

static void LakeReadFutureColumns(LakeInlineBinding* binding, Snapshot snapshot)
{
    if (!binding->historical_read || !binding->column_evolution || binding->snapshot_id >= binding->visible_head_snapshot)
        return;
    MemoryContext owner = CurrentMemoryContext;
    MemoryContext scratch = AllocSetContextCreate(owner, "Native future field history", ALLOCSET_DEFAULT_SIZES);
    PG_TRY();
    {
        MemoryContextSwitchTo(scratch);
        Oid types[3] = {INT8OID, INT8OID, INT8OID};
        Datum args[3] = {
            Int64GetDatum(LakePermanentTableId(binding)),
            Int64GetDatum(binding->visible_head_snapshot),
            Int64GetDatum(binding->snapshot_id)
        };
        const char* rel = LakeMetadataRelation(binding->metadata_schema_oid, "ducklake_column");
        LakeMetaRows* rows = LakeMetadataQuery(psprintf(
            "SELECT column_id,min(begin_snapshot),"
            "(SELECT c2.column_type::text FROM %s c2 WHERE c2.table_id=$1 AND c2.column_id=c1.column_id "
            " AND c2.begin_snapshot<=$2 AND c2.parent_column IS NULL ORDER BY c2.begin_snapshot DESC LIMIT 1),"
            "count(DISTINCT pg_catalog.textsend(column_type::text))=1 "
            "FROM %s c1 WHERE table_id=$1 AND begin_snapshot<=$2 AND parent_column IS NULL "
            "GROUP BY column_id HAVING min(begin_snapshot)>$3 ORDER BY column_id LIMIT %d",
            rel, rel, MaxHeapAttributeNumber + 1),
            3, types, args, NULL, snapshot, true, SPI_OK_SELECT, binding->metadata_schema_oid);
        if (rows->count > MaxHeapAttributeNumber)
            ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("native future field count exceeds the host bound")));
        binding->future_count = (int)rows->count;
        LakeReserveColumnMapBytes(binding, (Size)rows->count * sizeof(LakeInlineColumn));
        binding->future_columns = rows->count ? (LakeInlineColumn*)MemoryContextAllocZero(owner, sizeof(LakeInlineColumn) * rows->count) : NULL;
        for (uint32 i = 0; i < rows->count; ++i) {
            int64 id = DatumGetInt64(LakeMetadataValue(rows, i, 1, INT8OID));
            int64 intro = DatumGetInt64(LakeMetadataValue(rows, i, 2, INT8OID));
            const char* typeText = LakeMetadataText(rows, i, 3);
            bool sameType = DatumGetBool(LakeMetadataValue(rows, i, 4, BOOLOID));
            if (intro <= binding->snapshot_id || (!sameType && !binding->alter_type_evolution))
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("native future field history changed type or introduction")));
            int64 reserved = Min(Min(LAKE_FILE_ROW_ID, LAKE_FILE_SNAPSHOT_ID), Min(LAKE_DELETE_PATH_ID, LAKE_DELETE_POSITION_ID));
            if (id < 0 || id >= reserved)
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("future native field collides with hidden field namespace")));
            Datum childArgs[2] = {args[0], Int64GetDatum(id)};
            LakeMetaRows* children = LakeMetadataQuery(psprintf(
                "SELECT EXISTS(SELECT 1 FROM %s WHERE table_id=$1 AND parent_column=$2)", rel),
                2, types, childArgs, NULL, snapshot, true, SPI_OK_SELECT, binding->metadata_schema_oid);
            LakeMetadataRequireCount(children, 1, "future scalar child exclusion");
            if (DatumGetBool(LakeMetadataValue(children, 0, 1, BOOLOID)))
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("future scalar field has a nested history")));
            LakeInlineColumn* f = &binding->future_columns[i];
            f->column_id = id;
            f->introduced_snapshot = intro;
            f->type = LakeInlineType(typeText);
            f->typmod = f->type == NUMERICOID ? LakeNumericDuckTypmod(typeText) : -1;
            f->element_column_id = -1;
            if (!sameType) {
                bool promoted = false;
                (void)LakeVerifyColumnTypeChain(binding->metadata_schema_oid, LakePermanentTableId(binding),
                    id, binding->visible_head_snapshot, f->type, f->typmod, NULL, NULL, &promoted, snapshot);
            }
            if (f->type == BYTEAOID || f->type == UUIDOID || f->type == JSONOID || f->type == NUMERICOID)
                LakeNativeTypeRequireBusinessVersion(f->type);
        }
        MemoryContextSwitchTo(owner);
        MemoryContextDelete(scratch);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(owner);
        MemoryContextDelete(scratch);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

static void LakeReadInlineSources(LakeInlineBinding* binding,Snapshot snapshot)
{
    Oid types[2]={INT8OID,INT8OID};
    Datum args[2]={Int64GetDatum(LakePermanentTableId(binding)),Int64GetDatum(binding->snapshot_id)};
    const char* versionTable=LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_schema_versions");
    LakeMetaRows* current=LakeMetadataQuery(psprintf(
        "SELECT schema_version,begin_snapshot FROM %s WHERE table_id=$1 AND "
        "schema_version=(SELECT max(schema_version) FROM %s WHERE table_id=$1 AND begin_snapshot<=$2)",
        versionTable,versionTable),2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    /* This new path is exclusively a native CREATE binding. Its original
     * creator and every partition DDL write an explicit per-table version. */
    LakeMetadataRequireCount(current,1,"current native CREATE inline schema version");
    int64 version=DatumGetInt64(LakeMetadataValue(current,0,1,INT8OID));
    if(version<0||version>binding->schema_version)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid current inline schema version")));
    Datum carrierArgs[2]={args[0],Int64GetDatum(version)};
    LakeMetaRows* rows=LakeMetadataQuery(psprintf(
        "SELECT table_name::text,schema_version FROM %s WHERE table_id=$1%s ORDER BY schema_version LIMIT %d",
        LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_inlined_data_tables"),
        binding->historical_read?" AND schema_version<=$2":"",LAKE_INLINE_SOURCE_LIMIT+1),
        binding->historical_read?2:1,types,carrierArgs,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    if(rows->count>LAKE_INLINE_SOURCE_LIMIT)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake inline source descriptor budget exceeded")));
    int64 origin=LakeInlineSourceOrigin(binding,version,snapshot);
    /* Validate the writer's schema even when its carrier has not been created.
     * Missing version metadata cannot authorize add/drop/type/order changes. */
    LakeInlineSource writerSchema={};writerSchema.origin_snapshot=origin;
    LakeInlineSourceColumns(binding,&writerSchema,snapshot);
    if(writerSchema.column_count!=binding->column_count)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("current native write schema omits fields")));
    LakeReserveColumnMapBytes(binding,(Size)rows->count*(sizeof(LakeInlineSource)+NAMEDATALEN));
    binding->inline_sources_loaded=true;binding->inline_source_count=(int)rows->count;
    binding->inline_sources=rows->count?(LakeInlineSource*)palloc0(sizeof(LakeInlineSource)*rows->count):NULL;
    binding->inline_write_schema_version=version;binding->inline_write_origin_snapshot=origin;
    binding->inline_write_index=-1;
    for(uint32 i=0;i<rows->count;++i){
        LakeInlineSource* item=&binding->inline_sources[i];
        item->table_name=LakeMetadataText(rows,i,1);
        item->schema_version=DatumGetInt64(LakeMetadataValue(rows,i,2,INT8OID));
        if(item->schema_version<0||item->schema_version>version)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid registered inline schema version")));
        item->origin_snapshot=LakeInlineSourceOrigin(binding,item->schema_version,snapshot);
        LakeInlineSourceColumns(binding,item,snapshot);
        (void)LakeMetadataRelation(binding->metadata_schema_oid,item->table_name);
        item->relation_oid=get_relname_relid(item->table_name,binding->metadata_schema_oid);
        if(!OidIsValid(item->relation_oid))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("registered native lake inline carrier is missing")));
        /* Only scalar fields of the scratch binding are changed by this
         * loader; its source partition owns separate keys in our context. */
        LakeInlineBinding source=*binding;source.columns=item->columns;source.column_count=item->column_count;
        LakeLoadInlineIdentityPartition(&source,item->schema_version,item->origin_snapshot,snapshot);
        item->partition=source.inline_partition;
        if(item->schema_version==version)binding->inline_write_index=(int)i;
    }
    LakeSetInlineWriteSource(binding,binding->inline_write_index);
}

static LakeInlineBinding* LakeReadStandardTable(Oid serverOid, int64 tableId, Snapshot snapshot,
    bool physicalIndex=false,int64 sourceFile=-1,int64 admittedSnapshot=-1,bool nativeCreateEvolution=false,bool definitionOnly=false,
    bool historicalRead=false,int64 historicalSnapshot=-1,int64 historicalSchemaVersion=-1,int64 visibleHeadSnapshot=-1)
{
    LakeInlineBinding* binding=LakeLoadCatalogHeader(serverOid,snapshot);
    if(historicalRead){
        if(historicalSnapshot<0||historicalSchemaVersion<0||visibleHeadSnapshot<historicalSnapshot)
            ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid native lake historical snapshot target")));
        binding->snapshot_id=historicalSnapshot;
        binding->schema_version=historicalSchemaVersion;
        binding->historical_read=true;
        binding->visible_head_snapshot=visibleHeadSnapshot;
        binding->read_only=true;
    }else if(admittedSnapshot>=0) {
        /* Only the Native typed publisher uses this to ignore its own newly
         * claimed empty snapshot row before any business writes. The complete
         * certified snapshot row is read, never a guessed schema version. */
        if(admittedSnapshot>binding->snapshot_id||binding->snapshot_id-admittedSnapshot>1)
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native publication source is outside its certified head")));
        Oid type=INT8OID;Datum argument=Int64GetDatum(admittedSnapshot);
        LakeMetaRows* prior=LakeMetadataQuery(psprintf("SELECT schema_version FROM %s WHERE snapshot_id=$1",
            LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_snapshot")),1,&type,&argument,NULL,
            snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
        LakeMetadataRequireCount(prior,1,"certified native snapshot schema");
        binding->snapshot_id=admittedSnapshot;binding->schema_version=DatumGetInt64(LakeMetadataValue(prior,0,1,INT8OID));
        if(binding->schema_version<0)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid certified native schema version")));
    }
    binding->identity=LakePermanentIdentity(tableId);
    Oid types[2] = {INT8OID, INT8OID};
    Datum args[2] = {Int64GetDatum(tableId), Int64GetDatum(binding->snapshot_id)};
    char* tables = LakeMetadataRelation(binding->metadata_schema_oid, "ducklake_table");
    LakeMetaRows* table = LakeMetadataQuery(psprintf(
        "SELECT table_uuid::text FROM %s WHERE table_id=$1 AND begin_snapshot <= $2 "
        "AND (end_snapshot IS NULL OR end_snapshot > $2)", tables),
        2, types, args, NULL, snapshot, true, SPI_OK_SELECT, binding->metadata_schema_oid);
    if(historicalRead&&table->count==0)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("native lake table did not exist at the requested snapshot")));
    LakeMetadataRequireCount(table, 1, "visible table definition");
    binding->table_uuid = LakeMetadataText(table, 0, 1);
    char* columns = LakeMetadataRelation(binding->metadata_schema_oid, "ducklake_column");
    LakeMetaRows* cols = LakeMetadataQuery(psprintf(
        "SELECT column_id, column_name::text, column_type::text, parent_column "
        "FROM %s WHERE table_id=$1 AND begin_snapshot <= $2 "
        "AND (end_snapshot IS NULL OR end_snapshot > $2) ORDER BY column_order", columns),
        2, types, args, NULL, snapshot, true, SPI_OK_SELECT, binding->metadata_schema_oid);
    binding->columns=LakeReadColumnTree(cols,&binding->column_count);
    binding->column_evolution=nativeCreateEvolution&&LakeColumnEvolutionVersionEnabled();
    binding->rewrite_birth_bound=binding->column_evolution&&LakeMergeAdjacentVersionEnabled();
    binding->literal_defaults=binding->column_evolution&&LakeLiteralDefaultVersionEnabled();
    binding->alter_defaults=binding->literal_defaults&&LakeAlterDefaultVersionEnabled();
    binding->not_null_constraints=nativeCreateEvolution&&LakeNotNullVersionEnabled();
    binding->drop_columns=nativeCreateEvolution&&LakeDropColumnVersionEnabled();
    binding->alter_type_evolution=binding->column_evolution&&LakeAlterTypeOpsVersionEnabled();
    if(binding->not_null_constraints)
        LakeReadFieldNullability(binding,binding->columns,binding->column_count,binding->snapshot_id,snapshot);
    if(!nativeCreateEvolution&&LakeLiteralDefaultVersionEnabled()){
        LakeMetaRows* defaults=LakeMetadataQuery(psprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE table_id=$1 "
            "AND begin_snapshot<=$2 AND (end_snapshot IS NULL OR end_snapshot>$2) AND "
            "(initial_default IS NOT NULL OR (default_value IS NOT NULL AND "
            "pg_catalog.textsend(default_value::text)<>pg_catalog.textsend('NULL'::text))))",columns),
            2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
        LakeMetadataRequireCount(defaults,1,"external/native-parent default admission");
        if(DatumGetBool(LakeMetadataValue(defaults,0,1,BOOLOID)))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("non-NULL lake defaults require a protected native CREATE binding")));
    }
    if(binding->column_evolution)LakeReadColumnIntroductions(binding,snapshot);
    LakeReadRetiredColumns(binding,snapshot);
    if(historicalRead)LakeReadFutureColumns(binding,snapshot);
    if(physicalIndex&&!LakeIxSmpExtendedVersionEnabled()){
        LakeRejectBinaryAcceleration(binding,"physical index build");
        LakeRejectUuidAcceleration(binding,"physical index build");
        LakeRejectJsonAcceleration(binding,"physical index build");
        LakeRejectNumericAcceleration(binding,"physical index build");
    }
    if(physicalIndex&&(binding->has_added_columns||binding->has_dropped_columns))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake physical index build after ADD COLUMN requires its schema adapter")));
    LakeLoadCurrentIdentityPartition(binding,snapshot);
    if(definitionOnly)return binding;
    if(physicalIndex){LakeLoadIndexDataFile(binding,snapshot,sourceFile);return binding;}
    LakeLoadDataFiles(binding,snapshot);
    if(nativeCreateEvolution&&LakePartitionEvolutionVersionEnabled()){LakeReadInlineSources(binding,snapshot);return binding;}
    char* inlined = LakeMetadataRelation(binding->metadata_schema_oid, "ducklake_inlined_data_tables");
    Datum inlinedArgs[2] = {Int64GetDatum(tableId), Int64GetDatum(binding->schema_version)};
    LakeMetaRows* sources = LakeMetadataQuery(psprintf(
        "SELECT table_name::text, schema_version FROM %s WHERE table_id=$1%s ORDER BY schema_version",
        inlined, binding->historical_read ? " AND schema_version<=$2" : ""),
        binding->historical_read ? 2 : 1, types, inlinedArgs, NULL, snapshot, true, SPI_OK_SELECT, binding->metadata_schema_oid);
    /* Multi-schema projection/default adaptation is deliberately not guessed. */
    if (sources->count > 1)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("multiple inline schema versions require the schema evolution adapter")));
    if (sources->count == 1) {
        int64 version = DatumGetInt64(LakeMetadataValue(sources, 0, 2, INT8OID));
        if (version < 0 || version > binding->schema_version)
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid inline schema version")));
        /* Recover the source schema at its own creation snapshot, as in
         * DuckLake GetInlinedTableColumnsSql, rather than interpreting old
         * physical column names using the current schema.
         */
        char* versions = LakeMetadataRelation(binding->metadata_schema_oid, "ducklake_schema_versions");
        Datum versionArgs[2] = {Int64GetDatum(tableId), Int64GetDatum(version)};
        LakeMetaRows* created = LakeMetadataQuery(psprintf(
            "SELECT begin_snapshot FROM %s WHERE table_id=$1 AND schema_version=$2", versions),
            2, types, versionArgs, NULL, snapshot, true, SPI_OK_SELECT, binding->metadata_schema_oid);
        if (created->count > 1)
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("duplicate inline schema version definition")));
        if (created->count == 0)
            created = LakeMetadataQuery(psprintf("SELECT min(begin_snapshot) FROM %s WHERE table_id=$1", tables),
                1, types, args, NULL, snapshot, true, SPI_OK_SELECT, binding->metadata_schema_oid);
        LakeMetadataRequireCount(created, 1, "inline schema origin");
        int64 sourceSnapshot = DatumGetInt64(LakeMetadataValue(created, 0, 1, INT8OID));
        if (sourceSnapshot < 0 || sourceSnapshot > binding->snapshot_id)
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid inline schema origin")));
        LakeLoadInlineIdentityPartition(binding,version,sourceSnapshot,snapshot);
        Datum historicalArgs[2] = {Int64GetDatum(tableId), Int64GetDatum(sourceSnapshot)};
        LakeMetaRows* historical = LakeMetadataQuery(psprintf(
            "SELECT column_id,column_name::text,column_type::text,parent_column FROM %s "
            "WHERE table_id=$1 AND begin_snapshot <= $2 AND (end_snapshot IS NULL OR end_snapshot > $2) "
            "ORDER BY column_order", columns), 2, types, historicalArgs, NULL, snapshot, true, SPI_OK_SELECT,
            binding->metadata_schema_oid);
        int historicalCount=0;
        LakeInlineColumn* oldColumns=LakeReadColumnTree(historical,&historicalCount);
        if(historicalCount!=binding->column_count)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("inline schema evolution is not enabled")));
        for(int i=0;i<binding->column_count;++i){
            const LakeInlineColumn* old=&oldColumns[i];LakeInlineColumn* current=&binding->columns[i];
            if(old->column_id!=current->column_id||(!LakeHasColumnRenameVersion()&&strcmp(old->name,current->name)!=0)||old->type!=current->type||
                old->element_column_id!=current->element_column_id)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("inline column identity, order or type requires schema evolution support")));
            /* DuckLake resolves inline names at the registration schema
             * version. A logical rename never renames this physical carrier. */
            current->inline_name=pstrdup(old->name);
        }
        char* sourceName = LakeMetadataText(sources, 0, 1);
        /* Relation helper quotes an identifier as a whole, including dots. */
        (void)LakeMetadataRelation(binding->metadata_schema_oid, sourceName);
        binding->inline_oid = get_relname_relid(sourceName, binding->metadata_schema_oid);
    }
    return binding;
}

/* ADD has already changed pg_attribute. Revalidate standard/private identity
 * without requiring old standard fields to equal the new host descriptor. */
LakeInlineBinding* LakeReloadAddColumnDefinition(const LakeInlineBinding* original,Snapshot snapshot)
{
    LakeInlineBinding* now=LakeReadStandardTable(original->server_oid,LakePermanentTableId(original),
        snapshot,false,-1,-1,true,true);
    Oid type=OIDOID;Datum arg=ObjectIdGetDatum(original->relation_oid);
    LakeMetaRows* bound=LakeMetadataQuery("SELECT t.binding_id::text,t.server_oid,t.table_id,t.table_uuid::text,t.read_only "
        "FROM og_lake_internal.relation_binding r JOIN og_lake_internal.table_binding t USING(binding_id) "
        "WHERE r.relation_oid=$1 AND t.state='active' AND t.object_kind='relation'",1,&type,&arg,NULL,snapshot,true,SPI_OK_SELECT);
    LakeMetadataRequireCount(bound,1,"ADD captured host binding");
    if(now->read_only||now->metadata_schema_oid!=original->metadata_schema_oid||
       strcmp(now->data_root,original->data_root)||strcmp(now->table_uuid,original->table_uuid)||
       strcmp(LakeMetadataText(bound,0,1),original->binding_id)||
       DatumGetObjectId(LakeMetadataValue(bound,0,2,OIDOID))!=original->server_oid||
       DatumGetInt64(LakeMetadataValue(bound,0,3,INT8OID))!=LakePermanentTableId(original)||
       strcmp(LakeMetadataText(bound,0,4),original->table_uuid)||DatumGetBool(LakeMetadataValue(bound,0,5,BOOLOID))||
       now->column_count!=original->column_count)
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake ADD source identity changed")));
    for(int c=0;c<now->column_count;++c){
        const LakeInlineColumn* a=&original->columns[c];const LakeInlineColumn* b=&now->columns[c];
        if(a->column_id!=b->column_id||a->type!=b->type||a->element_column_id!=b->element_column_id||strcmp(a->name,b->name)||
           !LakeColumnDefaultsEqual(a,b))
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake ADD source fields changed")));
    }
    return now;
}

static void LakeCheckHostColumns(LakeInlineBinding* binding, Relation relation, bool committed = false, bool nativeLeaf = false)
{
    TupleDesc desc = RelationGetDescr(relation);
    if (desc->natts != LakeHostColumnCount(binding) || (RelationIsPartitioned(relation) && !nativeLeaf))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("lake host column or partition layout is unsupported")));
    if (desc->constr != NULL && desc->constr->num_check != 0)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake inline prototype does not validate host CHECK constraints")));
    binding->host_natts=desc->natts;
    for (int i = 0; i < binding->column_count; ++i) {
        LakeInlineColumn* col = &binding->columns[i];
        Form_pg_attribute attr = &desc->attrs[LakeLogicalToHost(binding,i)];
        bool vector=col->type==VECTOROID;
        bool validTypmod=vector?(attr->atttypmod>=1&&attr->atttypmod<=VECTOR_MAX_DIM):
            LakeNativeFlatTypmod(col->type,attr->atttypmod)&&
            (col->type!=NUMERICOID||attr->atttypmod==col->typmod);
        if (attr->attisdropped || (binding->not_null_constraints?attr->attnotnull!=col->not_null:attr->attnotnull) ||
            attr->atttypid != col->type || !validTypmod ||
            ((!committed || !LakeHasColumnRenameVersion()) && strcmp(NameStr(attr->attname), col->name) != 0))
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("lake column %d does not match its native relation binding", i + 1)));
        col->typmod=attr->atttypmod;col->output_attnum=attr->attnum;
        /* Only committed bindings have the independently checked column_id to
         * attnum mapping below. Initial ATTACH still requires exact names. */
        if(committed && LakeHasColumnRenameVersion()) col->name=pstrdup(NameStr(attr->attname));
    }
    if(binding->inline_sources_loaded)
        for(int n=0;n<binding->inline_source_count;++n)
            for(int c=0;c<binding->column_count;++c){
                LakeInlineSource* source=&binding->inline_sources[n];int physical=source->logical_to_source[c];
                if(physical>=0){
                    if(source->columns[physical].type==VECTOROID)
                        source->columns[physical].typmod=binding->columns[c].typmod;
                    source->columns[physical].output_attnum=binding->columns[c].output_attnum;
                }
            }
    LakeValidateHostDefaults(binding,relation);
    LakeBindPartitionHostColumns(binding,relation);
}

static void LakeCheckCommittedColumnMap(LakeInlineBinding* binding,Relation relation,Snapshot snapshot)
{
    Oid uuidType[1] = {UUIDOID};
    Datum uuidArg[1] = {DirectFunctionCall1(uuid_in, CStringGetDatum(binding->binding_id))};
    LakeMetaRows* mapped = LakeMetadataQuery(
        "SELECT column_id,attnum::integer,type_oid,typmod,collation_oid FROM og_lake_internal.column_binding "
        "WHERE binding_id=$1 ORDER BY attnum", 1, uuidType, uuidArg, NULL, snapshot, true, SPI_OK_SELECT);
    TupleDesc desc=RelationGetDescr(relation);
    if((!binding->drop_columns&&desc->natts!=binding->column_count)||
       desc->natts!=binding->column_count+binding->retired_count)
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake host holes differ from snapshot field history")));
    LakeMetadataRequireCount(mapped,desc->natts,"complete protected host column binding");
    binding->host_natts=desc->natts;int live=0,retired=0;
    bool* seen=(bool*)palloc0(sizeof(bool)*binding->column_count);
    bool* ended=(bool*)palloc0(sizeof(bool)*binding->retired_count);
    for(int h=0;h<desc->natts;++h){
        Form_pg_attribute attr=&desc->attrs[h];
        int64 id=DatumGetInt64(LakeMetadataValue(mapped,h,1,INT8OID));
        Oid type=DatumGetObjectId(LakeMetadataValue(mapped,h,3,OIDOID));
        int32 typmod=DatumGetInt32(LakeMetadataValue(mapped,h,4,INT4OID));
        Oid coll=DatumGetObjectId(LakeMetadataValue(mapped,h,5,OIDOID));
        if(DatumGetInt32(LakeMetadataValue(mapped,h,2,INT4OID))!=h+1||attr->attnum!=h+1)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native host field map has a missing or duplicate slot")));
        if(attr->attisdropped){
            int k=-1;for(int j=0;j<binding->retired_count;++j)
                if(binding->retired_columns[j].column.column_id==id)k=j;
            if(!binding->drop_columns||k<0||ended[k]||binding->retired_columns[k].column.type!=type||
               (type==NUMERICOID?(!LakeNumericTypmod(typmod)||typmod!=binding->retired_columns[k].column.typmod||attr->atttypmod!=typmod):
                   (typmod!=-1||attr->atttypmod!=typmod))||attr->attcollation!=coll||attr->atttypid!=InvalidOid||attr->attnotnull)
                ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native host hole has no exact retired field identity")));
            ended[k]=true;++retired;LakeRetiredColumn* r=&binding->retired_columns[k];
            r->column.output_attnum=(AttrNumber)(h+1);r->column.typmod=typmod;r->collation=coll;
        }else{
            int k=-1;for(int j=0;j<binding->column_count;++j)if(binding->columns[j].column_id==id)k=j;
            if(k<0||seen[k]||k!=live||type!=binding->columns[k].type||type!=attr->atttypid||
               (type==NUMERICOID&&typmod!=binding->columns[k].typmod)||
               typmod!=attr->atttypmod||coll!=attr->attcollation)
                ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native live field map disagrees with host descriptor")));
            seen[k]=true;++live;binding->columns[k].output_attnum=(AttrNumber)(h+1);
            binding->columns[k].typmod=typmod;
        }
    }
    if(live!=binding->column_count||retired!=binding->retired_count)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native field map is incomplete")));
    pfree(seen);pfree(ended);
}

/* SQL31 admits temporal business fields only through the kernel-protected
 * binding_mode=create lifecycle. Standard VARCHAR alone cannot establish its
 * producer's Calendar; neither read_only nor a format version is provenance.
 * This adds no private DuckLake catalog marker. */
static void LakeRejectUncertifiedTemporalBinding(const LakeInlineBinding* binding)
{
    if(LakeBindingHasBinary(binding)||LakeBindingHasNumeric(binding))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("BYTEA and numeric lake fields require a native CREATE binding")));
    for(int i=0;i<binding->column_count;++i)
        if(LakeNativeTypeIsTemporal(binding->columns[i].type))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("temporal lake fields require a native CREATE binding")));
}

static LakeInlineBinding* LakeLoadCommittedBindingInternalImpl(Oid relationOid,Snapshot snapshot,
    bool physicalIndex,int64 sourceFile)
{
    LakeRequireAccessPolicy();
    Relation managedTarget=heap_open(relationOid,AccessShareLock);
    LakeRejectNativePartitionDDL(managedTarget,"ordinary foreign-table lake binding entry");heap_close(managedTarget,NoLock);
    ForeignTable* foreign = GetForeignTable(relationOid);
    LakeTableOptions parsed=LakeParseTableOptions(foreign->options);
    Oid oidType[1] = {OIDOID};
    Datum oidArg[1] = {ObjectIdGetDatum(relationOid)};
    LakeMetaRows* bound = LakeMetadataQuery(
        "SELECT t.binding_id::text,t.server_oid,t.table_id,t.table_uuid::text,t.read_only "
        "FROM og_lake_internal.relation_binding r JOIN og_lake_internal.table_binding t USING(binding_id) "
        "WHERE r.relation_oid=$1 AND t.state='active' AND t.object_kind='relation'",
        1, oidType, oidArg, NULL, snapshot, true, SPI_OK_SELECT);
    LakeMetadataRequireCount(bound, 1, "relation binding");
    Oid serverOid = DatumGetObjectId(LakeMetadataValue(bound, 0, 2, OIDOID));
    int64 tableId = DatumGetInt64(LakeMetadataValue(bound, 0, 3, INT8OID));
    if (serverOid != foreign->serverid || (!parsed.create && tableId != parsed.table_id))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("native lake relation binding is stale")));
    LakeInlineBinding* binding = LakeReadStandardTable(serverOid, tableId, snapshot,physicalIndex,sourceFile,-1,parsed.create);
    if(!parsed.create) LakeRejectUncertifiedTemporalBinding(binding);
    binding->read_only |= DatumGetBool(LakeMetadataValue(bound, 0, 5, BOOLOID));
    binding->relation_oid = relationOid;
    binding->binding_id = LakeMetadataText(bound, 0, 1);
    if (strcmp(binding->table_uuid, LakeMetadataText(bound, 0, 4)) != 0)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("native lake table identity changed")));
    Relation relation = heap_open(relationOid, AccessShareLock);
    LakeCheckCommittedColumnMap(binding,relation,snapshot);
    LakeCheckHostColumns(binding, relation, true);
    /* The complete ordinary mapping must fit together with the REAL cooked
     * host descriptor retained by its next transaction read. Metadata-only
     * definition retry fences and physical-index projections keep their own
     * scope; this preflight neither copies nor charges the returned binding. */
    if(!physicalIndex)LakeTxnCheckHostConstraintCopy(binding,RelationGetDescr(relation));
    heap_close(relation, NoLock);
    return binding;
}

static LakeInlineBinding* LakeLoadCommittedBindingInternal(Oid relationOid,Snapshot snapshot,
    bool physicalIndex,int64 sourceFile)
{
    int fastScope = LakeEnterFastValidationScope();
    LakeInlineBinding* volatile binding = NULL;
    PG_TRY();
    {
        binding = LakeLoadCommittedBindingInternalImpl(relationOid, snapshot, physicalIndex, sourceFile);
    }
    PG_CATCH();
    {
        LakeLeaveFastValidationScope(fastScope);
        PG_RE_THROW();
    }
    PG_END_TRY();
    LakeLeaveFastValidationScope(fastScope);
    return binding;
}

/* A future ANALYZE release inherits the already qualified SQL23 read/INSERT
 * contract. It must not replace that retained contract before ALTER EXTENSION
 * commits. Keeping A empty (or accidentally equal to23) grants no new feature. */




bool LakeNativePartitionVersionEnabled(void)
{
    return (LakeNativeInstalledParentCapabilities()&LAKE_NATIVE_PARENT_READ)!=0;
}

bool LakeNativePartitionWriteVersionEnabled(void)
{
    return (LakeNativeInstalledParentCapabilities()&LAKE_NATIVE_PARENT_INSERT)!=0;
}

bool LakeNativePartitionMutationVersionEnabled(void)
{
    return (LakeNativeInstalledParentCapabilities()&LAKE_NATIVE_PARENT_MUTATION)!=0;
}

bool LakeNativePartitionCopyFromVersionEnabled(void)
{
    return (LakeNativeInstalledParentCapabilities()&LAKE_NATIVE_PARENT_COPY)!=0;
}

bool LakeNativePartitionAnalyzeVersionEnabled(void)
{
    return (LakeNativeInstalledParentCapabilities()&LAKE_NATIVE_PARENT_ANALYZE)!=0;
}

LakeInlineBinding* LakeReadNativePartitionSource(Oid server,int64 tableId,Snapshot snapshot,
    Relation parent,const char* bindingId,int64 admittedSnapshot)
{
    (void)LakeNativeRangeDescribe(parent);
    LakeInlineBinding* binding=LakeReadStandardTable(server,tableId,snapshot,false,-1,admittedSnapshot);
    if(!LakeNativePartitionExpandedVersionEnabled()){
        LakeRejectUncertifiedTemporalBinding(binding); /* Legacy pre-0.74 native parent keeps temporal closed. */
        LakeRejectUuidAcceleration(binding,"native parent/child partition binding");
        LakeRejectJsonAcceleration(binding,"native parent/child partition binding");
        LakeRejectNumericAcceleration(binding,"native parent/child partition binding");
    }
    /* The conservative partition-intersection probe must not resolve user
     * equality/cast functions on a substituted metadata type under SPI owner. */
    const char* probeTables[]={"ducklake_data_file","ducklake_partition_info","ducklake_partition_info","ducklake_partition_info","ducklake_partition_info","ducklake_partition_column"};
    const char* probeColumns[]={"partition_id","partition_id","table_id","begin_snapshot","end_snapshot","partition_id"};
    for(int probe=0;probe<6;++probe) {
        (void)LakeMetadataRelation(binding->metadata_schema_oid,probeTables[probe]);
        Relation meta=heap_open(get_relname_relid(probeTables[probe],binding->metadata_schema_oid),AccessShareLock);
        int att=get_attnum(RelationGetRelid(meta),probeColumns[probe]);
        if(att<=0||att>RelationGetDescr(meta)->natts||RelationGetDescr(meta)->attrs[att-1].attisdropped||
           RelationGetDescr(meta)->attrs[att-1].atttypid!=INT8OID)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native parent partition probe requires primitive metadata IDs")));
        heap_close(meta,NoLock);
    }
    Oid types[2]={INT8OID,INT8OID};Datum args[2]={Int64GetDatum(tableId),Int64GetDatum(binding->snapshot_id)};
    LakeMetaRows* filePartition=LakeMetadataQuery(psprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE table_id=$1 AND partition_id IS NOT NULL "
        "AND begin_snapshot<=$2 AND(end_snapshot IS NULL OR end_snapshot>$2)) OR EXISTS(SELECT 1 FROM %s p "
        "JOIN %s c ON c.partition_id=p.partition_id WHERE p.table_id=$1 AND p.begin_snapshot<=$2 "
        "AND(p.end_snapshot IS NULL OR p.end_snapshot>$2))",LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_data_file"),
        LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_partition_info"),LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_partition_column")),
        2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    LakeMetadataRequireCount(filePartition,1,"native parent file-partition capability");
    if(DatumGetBool(LakeMetadataValue(filePartition,0,1,BOOLOID)))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent with DuckLake file partitioning requires its combined qualification")));
    binding->relation_oid=RelationGetRelid(parent);binding->read_only=true;
    LakeCheckHostColumns(binding,parent,bindingId!=NULL,true);
    if(bindingId) {
        binding->binding_id=pstrdup(bindingId);
        LakeCheckCommittedColumnMap(binding,parent,snapshot);
    }
    return binding;
}

static LakeInlineBinding* LakeLoadHistoricalInlineBindingImpl(Oid relationOid, int64 targetSnapshot,
    int64 targetSchemaVersion, int64 visibleHeadSnapshot, Snapshot snapshot, TupleDesc* historicalDescriptorOut)
{
    LakeRequireAccessPolicy();
    Relation managedTarget = heap_open(relationOid, AccessShareLock);
    LakeRejectNativePartitionDDL(managedTarget, "time_travel");
    heap_close(managedTarget, NoLock);
    ForeignTable* foreign = GetForeignTable(relationOid);
    LakeTableOptions parsed = LakeParseTableOptions(foreign->options);
    Oid oidType[1] = {OIDOID};
    Datum oidArg[1] = {ObjectIdGetDatum(relationOid)};
    LakeMetaRows* bound = LakeMetadataQuery(
        "SELECT t.binding_id::text,t.server_oid,t.table_id,t.table_uuid::text "
        "FROM og_lake_internal.relation_binding r JOIN og_lake_internal.table_binding t USING(binding_id) "
        "WHERE r.relation_oid=$1 AND t.state='active' AND t.object_kind='relation'",
        1, oidType, oidArg, NULL, snapshot, true, SPI_OK_SELECT);
    LakeMetadataRequireCount(bound, 1, "time_travel relation binding");
    Oid serverOid = DatumGetObjectId(LakeMetadataValue(bound, 0, 2, OIDOID));
    int64 tableId = DatumGetInt64(LakeMetadataValue(bound, 0, 3, INT8OID));
    if (serverOid != foreign->serverid || (!parsed.create && tableId != parsed.table_id))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("native lake relation binding is stale")));
    LakeInlineBinding* binding = LakeReadStandardTable(serverOid, tableId, snapshot,
        false, -1, -1, parsed.create, false, true, targetSnapshot, targetSchemaVersion, visibleHeadSnapshot);
    if (!parsed.create)
        LakeRejectUncertifiedTemporalBinding(binding);
    binding->read_only = true;
    binding->relation_oid = relationOid;
    binding->binding_id = LakeMetadataText(bound, 0, 1);
    if (strcmp(binding->table_uuid, LakeMetadataText(bound, 0, 4)) != 0)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("native lake table identity changed")));

    Oid uuidType[1] = {UUIDOID};
    Datum uuidArg[1] = {DirectFunctionCall1(uuid_in, CStringGetDatum(binding->binding_id))};
    LakeMetaRows* mapped = LakeMetadataQuery(
        "SELECT column_id,attnum::integer,type_oid,typmod,collation_oid FROM og_lake_internal.column_binding "
        "WHERE binding_id=$1 ORDER BY attnum", 1, uuidType, uuidArg, NULL, snapshot, true, SPI_OK_SELECT);
    if (mapped->count == 0 || mapped->count > MaxHeapAttributeNumber)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid native lake column binding inventory")));
    for (uint32 m = 0; m < mapped->count; ++m) {
        int64 idM = DatumGetInt64(LakeMetadataValue(mapped, m, 1, INT8OID));
        for (uint32 prev = 0; prev < m; ++prev)
            if (DatumGetInt64(LakeMetadataValue(mapped, prev, 1, INT8OID)) == idM)
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("duplicate column_id in native lake column_binding")));
    }

    bool wholeTableSelect = (pg_class_aclcheck(relationOid, GetUserId(), ACL_SELECT) == ACLCHECK_OK);
    Relation aclRel = wholeTableSelect ? NULL : heap_open(relationOid, AccessShareLock);
    TupleDesc histDesc = CreateTemplateTupleDesc(binding->column_count, false);
    binding->host_natts = binding->column_count;
    for (int i = 0; i < binding->column_count; ++i) {
        LakeInlineColumn* col = &binding->columns[i];
        int matchIdx = -1;
        for (uint32 m = 0; m < mapped->count; ++m) {
            if (DatumGetInt64(LakeMetadataValue(mapped, m, 1, INT8OID)) == col->column_id) {
                matchIdx = (int)m;
                break;
            }
        }
        if (matchIdx < 0)
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("historical native lake column lacks a protected host column binding")));
        if (!wholeTableSelect) {
            int32 mapAttnum = DatumGetInt32(LakeMetadataValue(mapped, matchIdx, 2, INT4OID));
            TupleDesc hostDesc = RelationGetDescr(aclRel);
            if (mapAttnum < 1 || mapAttnum > hostDesc->natts || hostDesc->attrs[mapAttnum - 1].attisdropped)
                aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_CLASS, RelationGetRelationName(aclRel));
            if (pg_attribute_aclcheck(relationOid, (AttrNumber)mapAttnum, GetUserId(), ACL_SELECT) != ACLCHECK_OK)
                aclcheck_error_col(ACLCHECK_NO_PRIV, ACL_KIND_CLASS, RelationGetRelationName(aclRel),
                    NameStr(hostDesc->attrs[mapAttnum - 1].attname));
        }
        Oid mapType = DatumGetObjectId(LakeMetadataValue(mapped, matchIdx, 3, OIDOID));
        int32 mapTypmod = DatumGetInt32(LakeMetadataValue(mapped, matchIdx, 4, INT4OID));
        Oid mapColl = DatumGetObjectId(LakeMetadataValue(mapped, matchIdx, 5, OIDOID));
        bool vector = col->type == VECTOROID;
        bool exactMatch = (mapType == col->type) &&
            (vector ? (mapTypmod >= 1 && mapTypmod <= VECTOR_MAX_DIM) :
             LakeNativeFlatTypmod(col->type, mapTypmod) &&
             (col->type != NUMERICOID || (LakeNumericTypmod(mapTypmod) && mapTypmod == col->typmod)));
        bool widenedMatch = binding->alter_type_evolution && !vector &&
            LakeNativeFlatTypmod(mapType, mapTypmod) &&
            LakeIsWideningPromotion(col->type, col->typmod, mapType, mapTypmod);
        if (!exactMatch && !widenedMatch)
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("historical native lake column binding disagrees with DuckLake metadata")));
        if (exactMatch && vector)
            col->typmod = mapTypmod;
        col->output_attnum = (AttrNumber)(i + 1);
        TupleDescInitEntry(histDesc, (AttrNumber)(i + 1), col->name, col->type, col->typmod, 0);
        TupleDescInitEntryCollation(histDesc, (AttrNumber)(i + 1), mapColl);
    }
    if (aclRel != NULL)
        heap_close(aclRel, NoLock);
    if (binding->inline_sources_loaded) {
        for (int n = 0; n < binding->inline_source_count; ++n) {
            for (int c = 0; c < binding->column_count; ++c) {
                LakeInlineSource* source = &binding->inline_sources[n];
                int physical = source->logical_to_source[c];
                if (physical >= 0) {
                    if (source->columns[physical].type == VECTOROID)
                        source->columns[physical].typmod = binding->columns[c].typmod;
                    source->columns[physical].output_attnum = binding->columns[c].output_attnum;
                }
            }
        }
    }
    *historicalDescriptorOut = BlessTupleDesc(histDesc);
    return binding;
}

LakeInlineBinding* LakeLoadHistoricalInlineBinding(Oid relationOid, int64 targetSnapshot,
    int64 targetSchemaVersion, int64 visibleHeadSnapshot, Snapshot snapshot, TupleDesc* historicalDescriptorOut)
{
    int fastScope = LakeEnterFastValidationScope();
    LakeInlineBinding* volatile binding = NULL;
    PG_TRY();
    {
        binding = LakeLoadHistoricalInlineBindingImpl(
            relationOid, targetSnapshot, targetSchemaVersion, visibleHeadSnapshot, snapshot, historicalDescriptorOut);
    }
    PG_CATCH();
    {
        LakeLeaveFastValidationScope(fastScope);
        PG_RE_THROW();
    }
    PG_END_TRY();
    LakeLeaveFastValidationScope(fastScope);
    return binding;
}

static LakeInlineBinding* LakeLoadBoundSnapshotInlineBindingImpl(Oid relationOid, int64 targetSnapshot,
    int64 targetSchemaVersion, int64 visibleHeadSnapshot, Snapshot snapshot, bool definitionOnly)
{
    LakeRequireAccessPolicy();
    Relation host = heap_open(relationOid, AccessShareLock);
    LakeRejectNativePartitionDDL(host, "bind_snapshot");
    if (LakeTxnFindCreatedTable(relationOid, false) != NULL) {
        heap_close(host, NoLock);
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("native lake table did not exist at the requested snapshot")));
    }
    ForeignTable* foreign = GetForeignTable(relationOid);
    LakeTableOptions parsed = LakeParseTableOptions(foreign->options);
    Oid oidType[1] = {OIDOID};
    Datum oidArg[1] = {ObjectIdGetDatum(relationOid)};
    LakeMetaRows* bound = LakeMetadataQuery(
        "SELECT t.binding_id::text,t.server_oid,t.table_id,t.table_uuid::text "
        "FROM og_lake_internal.relation_binding r JOIN og_lake_internal.table_binding t USING(binding_id) "
        "WHERE r.relation_oid=$1 AND t.state='active' AND t.object_kind='relation'",
        1, oidType, oidArg, NULL, snapshot, true, SPI_OK_SELECT);
    if (bound->count == 0) {
        heap_close(host, NoLock);
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("native lake table did not exist at the requested snapshot")));
    }
    LakeMetadataRequireCount(bound, 1, "bind_snapshot relation binding");
    Oid serverOid = DatumGetObjectId(LakeMetadataValue(bound, 0, 2, OIDOID));
    int64 tableId = DatumGetInt64(LakeMetadataValue(bound, 0, 3, INT8OID));
    if (serverOid != foreign->serverid || (!parsed.create && tableId != parsed.table_id)) {
        heap_close(host, NoLock);
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("native lake relation binding is stale")));
    }
    LakeInlineBinding* binding = NULL;
    PG_TRY();
    {
        binding = LakeReadStandardTable(serverOid, tableId, snapshot,
            false, -1, -1, parsed.create, definitionOnly, true, targetSnapshot, targetSchemaVersion, visibleHeadSnapshot);
        if (!parsed.create)
            LakeRejectUncertifiedTemporalBinding(binding);
        binding->read_only = true;
        binding->bound_snapshot_read = true;
        binding->relation_oid = relationOid;
        binding->binding_id = LakeMetadataText(bound, 0, 1);
        if (strcmp(binding->table_uuid, LakeMetadataText(bound, 0, 4)) != 0)
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("native lake table identity changed")));

        Oid uuidType[1] = {UUIDOID};
        Datum uuidArg[1] = {DirectFunctionCall1(uuid_in, CStringGetDatum(binding->binding_id))};
        LakeMetaRows* mapped = LakeMetadataQuery(
            "SELECT column_id,attnum::integer,type_oid,typmod,collation_oid FROM og_lake_internal.column_binding "
            "WHERE binding_id=$1 ORDER BY attnum", 1, uuidType, uuidArg, NULL, snapshot, true, SPI_OK_SELECT);
        if (mapped->count == 0 || mapped->count > MaxHeapAttributeNumber)
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid native lake column binding inventory")));
        for (uint32 m = 0; m < mapped->count; ++m) {
            int64 idM = DatumGetInt64(LakeMetadataValue(mapped, m, 1, INT8OID));
            for (uint32 prev = 0; prev < m; ++prev)
                if (DatumGetInt64(LakeMetadataValue(mapped, prev, 1, INT8OID)) == idM)
                    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("duplicate column_id in native lake column_binding")));
        }

        TupleDesc hostDesc = RelationGetDescr(host);
        binding->host_natts = hostDesc->natts;
        bool seenHost[MaxHeapAttributeNumber] = {false};
        for (int i = 0; i < binding->column_count; ++i) {
            LakeInlineColumn* col = &binding->columns[i];
            int matchIdx = -1;
            for (uint32 m = 0; m < mapped->count; ++m) {
                if (DatumGetInt64(LakeMetadataValue(mapped, m, 1, INT8OID)) == col->column_id) {
                    matchIdx = (int)m;
                    break;
                }
            }
            if (matchIdx < 0)
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("historical native lake column lacks a protected host column binding")));
            int32 mapAttnum = DatumGetInt32(LakeMetadataValue(mapped, matchIdx, 2, INT4OID));
            Oid mapType = DatumGetObjectId(LakeMetadataValue(mapped, matchIdx, 3, OIDOID));
            int32 mapTypmod = DatumGetInt32(LakeMetadataValue(mapped, matchIdx, 4, INT4OID));
            Oid mapColl = DatumGetObjectId(LakeMetadataValue(mapped, matchIdx, 5, OIDOID));
            if (mapAttnum < 1 || mapAttnum > hostDesc->natts)
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("historical native lake column host attnum out of range")));
            bool vector = col->type == VECTOROID;
            bool exactMatch = (mapType == col->type) &&
                (vector ? (mapTypmod >= 1 && mapTypmod <= VECTOR_MAX_DIM) :
                 LakeNativeFlatTypmod(col->type, mapTypmod) &&
                 (col->type != NUMERICOID || (LakeNumericTypmod(mapTypmod) && mapTypmod == col->typmod)));
            bool widenedMatch = binding->alter_type_evolution && !vector &&
                LakeNativeFlatTypmod(mapType, mapTypmod) &&
                LakeIsWideningPromotion(col->type, col->typmod, mapType, mapTypmod);
            if (!exactMatch && !widenedMatch)
                ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("historical native lake column binding disagrees with DuckLake metadata")));
            if (exactMatch && vector)
                col->typmod = mapTypmod;
            col->target_type = mapType;
            col->target_typmod = mapTypmod;
            Form_pg_attribute hostAttr = &hostDesc->attrs[mapAttnum - 1];
            if (hostAttr->attisdropped) {
                col->output_attnum = InvalidAttrNumber;
            } else {
                if (seenHost[mapAttnum - 1])
                    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("duplicate host column mapping for bound snapshot")));
                seenHost[mapAttnum - 1] = true;
                if (hostAttr->atttypid != mapType || hostAttr->atttypmod != mapTypmod || hostAttr->attcollation != mapColl)
                    ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("host relation column type changed incompatibly with historical lake schema")));
                if (binding->not_null_constraints && hostAttr->attnotnull && !col->not_null)
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("host NOT NULL constraint on column \"%s\" was added after bound snapshot " INT64_FORMAT,
                            NameStr(hostAttr->attname), targetSnapshot)));
                col->output_attnum = (AttrNumber)mapAttnum;
            }
        }
        if (binding->inline_sources_loaded) {
            for (int n = 0; n < binding->inline_source_count; ++n) {
                for (int c = 0; c < binding->column_count; ++c) {
                    LakeInlineSource* source = &binding->inline_sources[n];
                    int physical = source->logical_to_source[c];
                    if (physical >= 0) {
                        if (source->columns[physical].type == VECTOROID)
                            source->columns[physical].typmod = binding->columns[c].typmod;
                        source->columns[physical].output_attnum = binding->columns[c].output_attnum;
                    }
                }
            }
        }
        heap_close(host, NoLock);
    }
    PG_CATCH();
    {
        heap_close(host, NoLock);
        PG_RE_THROW();
    }
    PG_END_TRY();
    return binding;
}

LakeInlineBinding* LakeLoadBoundSnapshotInlineBinding(Oid relationOid, int64 targetSnapshot,
    int64 targetSchemaVersion, int64 visibleHeadSnapshot, Snapshot snapshot, bool definitionOnly)
{
    int fastScope = LakeEnterFastValidationScope();
    LakeInlineBinding* volatile binding = NULL;
    PG_TRY();
    {
        binding = LakeLoadBoundSnapshotInlineBindingImpl(
            relationOid, targetSnapshot, targetSchemaVersion, visibleHeadSnapshot, snapshot, definitionOnly);
    }
    PG_CATCH();
    {
        LakeLeaveFastValidationScope(fastScope);
        PG_RE_THROW();
    }
    PG_END_TRY();
    LakeLeaveFastValidationScope(fastScope);
    return binding;
}

LakeInlineBinding* LakeLoadCommittedInlineBinding(Oid relationOid,Snapshot snapshot)
{return LakeLoadCommittedBindingInternal(relationOid,snapshot,false,-1);}

LakeInlineBinding* LakeLoadIndexSourceBinding(Oid relationOid,int64 sourceFile,Snapshot snapshot)
{return LakeLoadCommittedBindingInternal(relationOid,snapshot,true,sourceFile);}

LakeInlineBinding* LakeLoadInlineBinding(Oid relationOid, Snapshot snapshot)
{
    LakeRequireAccessPolicy();
    LakeTxnTable* local=LakeTxnFindCreatedTable(relationOid);
    if(local!=NULL) return local->binding;
    return LakeLoadCommittedInlineBinding(relationOid,snapshot);
}

/* Planner/executor constraint proof only. No file list, source carrier, default
 * payload or transaction read root is needed before a possibly dummy plan. */
static LakeInlineBinding* LakeLoadConstraintDefinitionImpl(Relation relation,Snapshot snapshot)
{
    LakeRequireAccessPolicy();Oid relationOid=RelationGetRelid(relation);
    LakeTxnTable* local=LakeTxnFindCreatedTable(relationOid);
    if(local!=NULL)return local->binding;
    ForeignTable* foreign=GetForeignTable(relationOid);
    LakeTableOptions parsed=LakeParseTableOptions(foreign->options);
    if(!parsed.create||!LakeNotNullVersionEnabled())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake constraints require a protected CREATE binding")));
    Oid oidType=OIDOID;Datum oidArg=ObjectIdGetDatum(relationOid);
    LakeMetaRows* bound=LakeMetadataQuery(
        "SELECT t.binding_id::text,t.server_oid,t.table_id,t.table_uuid::text,t.read_only "
        "FROM og_lake_internal.relation_binding r JOIN og_lake_internal.table_binding t USING(binding_id) "
        "WHERE r.relation_oid=$1 AND t.state='active' AND t.object_kind='relation'",
        1,&oidType,&oidArg,NULL,snapshot,true,SPI_OK_SELECT);
    LakeMetadataRequireCount(bound,1,"constraint relation binding");
    if(DatumGetObjectId(LakeMetadataValue(bound,0,2,OIDOID))!=foreign->serverid)
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake constraint binding server changed")));
    LakeInlineBinding* binding=LakeLoadCatalogHeader(foreign->serverid,snapshot);
    binding->relation_oid=relationOid;binding->not_null_constraints=true;
    binding->drop_columns=LakeDropColumnVersionEnabled();binding->column_evolution=binding->drop_columns;
    binding->alter_type_evolution=binding->column_evolution&&LakeAlterTypeOpsVersionEnabled();
    binding->identity=LakePermanentIdentity(DatumGetInt64(LakeMetadataValue(bound,0,3,INT8OID)));
    binding->binding_id=LakeMetadataText(bound,0,1);binding->table_uuid=LakeMetadataText(bound,0,4);
    Oid types[2]={INT8OID,INT8OID};Datum args[2]={Int64GetDatum(LakePermanentTableId(binding)),Int64GetDatum(binding->snapshot_id)};
    LakeMetaRows* table=LakeMetadataQuery(psprintf("SELECT table_uuid::text FROM %s WHERE table_id=$1 "
        "AND begin_snapshot<=$2 AND(end_snapshot IS NULL OR end_snapshot>$2)",
        LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_table")),
        2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    LakeMetadataRequireCount(table,1,"constraint visible table");
    if(strcmp(binding->table_uuid,LakeMetadataText(table,0,1)))
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake constraint table identity changed")));
    LakeMetaRows* columns=LakeMetadataQuery(psprintf("SELECT column_id,column_name::text,column_type::text,parent_column "
        "FROM %s WHERE table_id=$1 AND begin_snapshot<=$2 AND(end_snapshot IS NULL OR end_snapshot>$2) "
        "ORDER BY column_order",LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_column")),
        2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    binding->columns=LakeReadColumnTree(columns,&binding->column_count);
    LakeReadRetiredColumns(binding,snapshot,false);
    LakeCheckCommittedColumnMap(binding,relation,snapshot);
    LakeReadFieldNullability(binding,binding->columns,binding->column_count,binding->snapshot_id,snapshot);
    return binding;
}

LakeInlineBinding* LakeLoadConstraintDefinition(Relation relation,Snapshot snapshot)
{
    int fastScope = LakeEnterFastValidationScope();
    LakeInlineBinding* volatile binding = NULL;
    PG_TRY();
    {
        binding = LakeLoadConstraintDefinitionImpl(relation, snapshot);
    }
    PG_CATCH();
    {
        LakeLeaveFastValidationScope(fastScope);
        PG_RE_THROW();
    }
    PG_END_TRY();
    LakeLeaveFastValidationScope(fastScope);
    return binding;
}

/* Called before the host foreign-default fence/atthasdef assignment. Only
 * this exact server+CREATE+SQL49 path can supply a pre-authenticated Const. */
bool LakeNativePrepareCreateColumnDefault(CreateForeignTableStmt* statement,
    ColumnDef* column,Oid type,int32 typmod,Oid collation,Size* statementBytes)
{
    if(statement==NULL||column==NULL||column->raw_default==NULL)return false;
    ForeignServer* server=GetForeignServerByName(statement->servername,false);
    if(!LakeIsServer(server->serverid)||!LakeCreateLiteralDefaultVersionEnabled())return false;
    LakeTableOptions parsed=LakeParseTableOptions(statement->options);
    if(!parsed.create)return false;
    LakePrepareCreateColumnLiteral(column,type,typmod,collation,statementBytes);
    return true;
}

void LakeNativeBindForeignTable(Oid relationOid, CreateForeignTableStmt* statement)
{
    ForeignTable* foreign = GetForeignTable(relationOid);
    if (!LakeIsServer(foreign->serverid)) return;
    LakeTableOptions parsed=LakeParseTableOptions(foreign->options);
    if(parsed.create) { LakeNativeCreateForeignTable(relationOid,statement,parsed.schema); return; }
    LakeTxnRejectBindingDdl();
    LakeInlineRequireAdministrator();
    Snapshot snapshot = LakeCatalogSnapshot();
    LakeInlineBinding* binding = LakeReadStandardTable(foreign->serverid, parsed.table_id, snapshot);
    LakeRejectUncertifiedTemporalBinding(binding);
    LakePublishHostBinding(binding,relationOid,snapshot);
    if(LakeLogicalDropVersionEnabled()) LakeValidateAttachedSourceAfterBinding(binding);
}

void LakePublishHostBinding(LakeInlineBinding* binding, Oid relationOid, Snapshot snapshot)
{
    (void)LakePermanentTableId(binding);
    Relation relation = heap_open(relationOid, AccessShareLock);
    LakeCheckHostColumns(binding, relation);
    Datum uuid = binding->binding_id==NULL ? OidFunctionCall0(F_GEN_RANDOM_UUID) : DirectFunctionCall1(uuid_in,CStringGetDatum(binding->binding_id));
    Oid types[6] = {UUIDOID,OIDOID,INT8OID,UUIDOID,INT8OID,OIDOID};
    Datum args[6] = {uuid,ObjectIdGetDatum(binding->server_oid),Int64GetDatum(LakePermanentTableId(binding)),
        DirectFunctionCall1(uuid_in,CStringGetDatum(binding->table_uuid)),Int64GetDatum(binding->schema_version),
        ObjectIdGetDatum(relationOid)};
    (void)LakeMetadataQuery(
        psprintf("INSERT INTO og_lake_internal.table_binding(binding_id,server_oid,table_id,table_uuid,schema_version,"
        "object_kind,state,read_only) VALUES($1,$2,$3,$4,$5,'relation','active',%s)",binding->read_only?"true":"false"),
        5,types,args,NULL,snapshot,false,SPI_OK_INSERT);
    Oid relTypes[2] = {OIDOID,UUIDOID};
    Datum relArgs[2] = {args[5],uuid};
    (void)LakeMetadataQuery("INSERT INTO og_lake_internal.relation_binding(relation_oid,binding_id) VALUES($1,$2)",
        2,relTypes,relArgs,NULL,snapshot,false,SPI_OK_INSERT);
    for (int i=0;i<binding->column_count;++i) {
        Form_pg_attribute attr = &RelationGetDescr(relation)->attrs[i];
        Oid colTypes[6] = {UUIDOID,INT8OID,INT4OID,OIDOID,INT4OID,OIDOID};
        Datum colArgs[6] = {uuid,Int64GetDatum(binding->columns[i].column_id),Int32GetDatum(i+1),
            ObjectIdGetDatum(attr->atttypid),Int32GetDatum(attr->atttypmod),ObjectIdGetDatum(attr->attcollation)};
        (void)LakeMetadataQuery("INSERT INTO og_lake_internal.column_binding(binding_id,column_id,attnum,type_oid,typmod,"
            "collation_oid,codec_version) VALUES($1,$2,$3,$4,$5,$6,1)",
            6,colTypes,colArgs,NULL,snapshot,false,SPI_OK_INSERT);
    }
    binding->binding_id = DatumGetCString(DirectFunctionCall1(uuid_out, uuid));
    LakePrepareRowLockBinding(binding, relationOid, snapshot);
    heap_close(relation,NoLock);
}

void LakeNativeForgetForeignTable(Oid relationOid)
{
    ForeignTable* foreign=GetForeignTable(relationOid);
    if (!LakeIsServer(foreign->serverid)) return;
    LakeTxnTable* pending=LakeTxnFindCreatedTable(relationOid,false);
    if(pending!=NULL) {
        LakeNativeDropCreatedCarrier(pending);
        (void)LakeTxnDropCreatedTable(relationOid);
        return;
    }
    Oid internalSchema = get_namespace_oid("og_lake_internal", true);
    /* Only the explicit deletion-set scope may tolerate partially deleted
     * extension internals. Normal DROP must fail on a damaged binding set.
     */
    /* Only the immutable native CREATE route owns logical DROP. ATTACH and
     * extension teardown retain host-only detach, including every old SQL.
     * CREATE forbids table_id; kernel ALTER blocks changing binding_mode. */
    bool logicalDrop=LakeLogicalDropVersionEnabled()&&!LakeInExtensionDropScope()&&
        LakeParseTableOptions(foreign->options).create;
    if(!logicalDrop) LakeTxnRejectBindingDdl();
    if (LakeInExtensionDropScope()) return;
    if (!OidIsValid(internalSchema) || !OidIsValid(get_relname_relid("table_binding", internalSchema)) ||
        !OidIsValid(get_relname_relid("relation_binding", internalSchema)) ||
        !OidIsValid(get_relname_relid("column_binding", internalSchema)))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("native lake binding tables are incomplete")));
    LakeInlineRequireAdministrator();
    Oid type=OIDOID; Datum arg=ObjectIdGetDatum(relationOid);
    if(logicalDrop) LakeStageCommittedDrop(relationOid,foreign->serverid);
    LakeIndexRetireRelation(relationOid,LakeCatalogSnapshot());
    LakeDropRowLockBinding(relationOid, LakeCatalogSnapshot());
    LakeAutoAnalyzeForgetRelation(relationOid,LakeCatalogSnapshot());
    /* Cascades remove the relation and column bindings in the same transaction. */
    LakeMetaRows* deleted = LakeMetadataQuery("DELETE FROM og_lake_internal.table_binding WHERE binding_id IN "
        "(SELECT binding_id FROM og_lake_internal.relation_binding WHERE relation_oid=$1)",
        1,&type,&arg,NULL,LakeCatalogSnapshot(),false,SPI_OK_DELETE);
    LakeMetadataRequireCount(deleted, 1, "deleted relation binding");
}

void LakeNativeForgetForeignServer(Oid serverOid)
{
    if (!LakeIsServer(serverOid)) return;
    Oid internalSchema = get_namespace_oid("og_lake_internal", true);
    LakeTxnRejectBindingDdl();
    if (LakeInExtensionDropScope()) return;
    if (!OidIsValid(internalSchema) || !OidIsValid(get_relname_relid("catalog_binding", internalSchema)))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("native lake catalog binding table is missing")));
    LakeInlineRequireAdministrator();
    LakeLockCatalogRegistration();
    Oid type=OIDOID; Datum arg=ObjectIdGetDatum(serverOid);
    Snapshot snapshot=RegisterSnapshot(GetLatestSnapshot());
    PG_TRY();
    {
        LakeMetaRows* bindingRows = LakeMetadataQuery(
            "SELECT metadata_schema_oid, catalog_id, binding_generation, data_root "
            "FROM og_lake_internal.catalog_binding WHERE server_oid=$1",
            1, &type, &arg, NULL, snapshot, true, SPI_OK_SELECT);
        if (bindingRows->count == 1) {
            LakeCommittedCatalogKey resetKey;
            resetKey.database_oid = u_sess->proc_cxt.MyDatabaseId;
            resetKey.server_oid = serverOid;
            resetKey.metadata_schema_oid = DatumGetObjectId(LakeMetadataValue(bindingRows, 0, 1, OIDOID));
            pg_uuid_t* catUuid = DatumGetUUIDP(LakeMetadataValue(bindingRows, 0, 2, UUIDOID));
            errno_t krc = memcpy_s(resetKey.catalog_id, UUID_LEN, catUuid->data, UUID_LEN);
            securec_check(krc, "", "");
            resetKey.binding_generation = DatumGetInt64(LakeMetadataValue(bindingRows, 0, 3, INT8OID));
            resetKey.data_root = LakeMetadataText(bindingRows, 0, 4);
            LakeHostStageCatalogReset(&resetKey, GetCurrentSubTransactionId());
        }
        if(OidIsValid(LakeMetadataOwner()))
            (void)LakeMetadataQuery("DELETE FROM og_lake_internal.metadata_relation WHERE metadata_schema_oid IN "
                "(SELECT metadata_schema_oid FROM og_lake_internal.catalog_binding WHERE server_oid=$1)",
                1,&type,&arg,NULL,snapshot,false,SPI_OK_DELETE);
        LakeAutoAnalyzeForgetServer(serverOid,snapshot);
        (void)LakeMetadataQuery("DELETE FROM og_lake_internal.catalog_binding WHERE server_oid=$1",
            1,&type,&arg,NULL,snapshot,false,SPI_OK_DELETE);
        UnregisterSnapshot(snapshot);
    }
    PG_CATCH();
    {
        UnregisterSnapshot(snapshot);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

static void LakePublishCatalogBinding(const char* serverName, Oid schemaOid, const char* root, bool readOnly)
{
    CreateForeignServerStmt* stmt=makeNode(CreateForeignServerStmt);
    stmt->servername=pstrdup(serverName); stmt->fdwname=pstrdup("og_lake_fdw");
    ObjectAddress server=CreateForeignServer(stmt);
    CommandCounterIncrement();
    Oid types[5]={OIDOID,OIDOID,UUIDOID,TEXTOID,BOOLOID};
    Datum args[5]={ObjectIdGetDatum(server.objectId),ObjectIdGetDatum(schemaOid),
        OidFunctionCall0(F_GEN_RANDOM_UUID),CStringGetTextDatum(root),BoolGetDatum(readOnly)};
    LakeMetaRows* inserted=LakeMetadataQuery(
        "INSERT INTO og_lake_internal.catalog_binding(server_oid,metadata_schema_oid,catalog_id,"
        "data_root,credential_ref,format_version,read_only,binding_generation) "
        "VALUES($1,$2,$3,$4,NULL,'1.1',$5,1)",5,types,args,NULL,InvalidSnapshot,false,SPI_OK_INSERT);
    LakeMetadataRequireCount(inserted,1,"new catalog binding");
    CommandCounterIncrement();
}

Datum og_lake_attach_catalog(PG_FUNCTION_ARGS)
{
    LakeInlineRequireAdministrator();
    LakeTxnRejectBindingDdl();
    if(PG_ARGISNULL(0)||PG_ARGISNULL(1)||PG_ARGISNULL(3))
        ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
            errmsg("native lake attach requires catalog names and read-only mode")));
    if(!PG_ARGISNULL(2))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake credential-backed catalog registration is not implemented")));
    if(!PG_GETARG_BOOL(3)) LakeRequireWriteVersion();
    const char* serverName=NameStr(*PG_GETARG_NAME(0));
    const char* schemaName=NameStr(*PG_GETARG_NAME(1));
    LakeLockCatalogRegistration(); /* Includes old installed SQL versions. */
    LakeCheckCatalogNames(serverName,schemaName,false);
    Oid schemaOid=get_namespace_oid(schemaName,false);
    if(IsSystemNamespace(schemaOid)||IsToastNamespace(schemaOid)||isTempNamespace(schemaOid))
        ereport(ERROR,(errcode(ERRCODE_INVALID_SCHEMA_NAME),errmsg("reserved lake metadata schema")));
    LakeRegisterMetadataObjects(schemaOid);
    CommandCounterIncrement();
    Snapshot snapshot=RegisterSnapshot(GetLatestSnapshot());
    char* canonicalRoot=NULL;
    PG_TRY();
    {
        LakeCheckFormat(schemaOid,snapshot);
        char* metadata=LakeMetadataRelation(schemaOid,"ducklake_metadata");
        LakeMetaRows* data=LakeMetadataQuery(psprintf(
            "SELECT value::text FROM %s WHERE key='data_path' AND scope IS NULL",metadata),
            0,NULL,NULL,NULL,snapshot,true,SPI_OK_SELECT,schemaOid);
        LakeMetadataRequireCount(data,1,"data path");
        canonicalRoot=LakeNormalizeLocalRoot(LakeMetadataText(data,0,1),false);
        UnregisterSnapshot(snapshot);
    }
    PG_CATCH();
    {
        UnregisterSnapshot(snapshot);
        PG_RE_THROW();
    }
    PG_END_TRY();
    LakeCheckRootAvailable(canonicalRoot);
    LakePublishCatalogBinding(serverName,schemaOid,canonicalRoot,PG_GETARG_BOOL(3));
    PG_RETURN_VOID();
}

Datum og_lake_create_catalog(PG_FUNCTION_ARGS)
{
    LakeInlineRequireAdministrator();
    LakeTxnRejectBindingDdl();
    char* installed=LakeInstalledVersion();
    if(installed==NULL || (strcmp(installed,"0.6")!=0 && strcmp(installed,"0.7")!=0 && strcmp(installed,"0.8")!=0 && strcmp(installed,"0.9")!=0 && (strcmp(installed,"0.10")!=0&&strcmp(installed,"0.11")!=0&&strcmp(installed,"0.12")!=0&&strcmp(installed,"0.13")!=0&&strcmp(installed,"0.14")!=0&&(strcmp(installed,"0.15")!=0&&(strcmp(installed,"0.16")!=0&&(strcmp(installed,"0.17")!=0&&strcmp(installed,"0.18")!=0&&(strcmp(installed,"0.19")!=0&&(strcmp(installed,"0.20")!=0&&(strcmp(installed,"0.21")!=0&&(strcmp(installed,"0.22")!=0&&(strcmp(installed,"0.23")!=0&&!LakeNativeFutureContractEnabled(installed))))))))))))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native catalog creation requires installed extension 0.6")));
    if(PG_ARGISNULL(0)||PG_ARGISNULL(1)||PG_ARGISNULL(2))
        ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
            errmsg("native lake creation requires catalog names and a data root")));
    if(!PG_ARGISNULL(3))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake credential-backed catalog creation is not implemented")));
    if(u_sess->attr.attr_common.XactReadOnly)
        ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
            errmsg("cannot create a lake catalog in a read-only transaction")));
    const char* serverName=NameStr(*PG_GETARG_NAME(0));
    const char* schemaName=NameStr(*PG_GETARG_NAME(1));
    LakeLockCatalogRegistration();
    Oid owner=LakeMetadataOwner();
    if(!OidIsValid(owner))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("native catalog creation requires a configured private owner")));
    LakeCheckMaskingAdmission();
    LakeCheckCatalogNames(serverName,schemaName,true);
    char* canonicalRoot=LakeNormalizeLocalRoot(TextDatumGetCString(PG_GETARG_DATUM(2)),true);
    LakeCheckRootAvailable(canonicalRoot);

    /* No IF NOT EXISTS and no filesystem side effects. The source-derived DDL
     * is executed with pg_catalog-only resolution as the authorized caller. */
    LakeCreateUtility(psprintf("CREATE SCHEMA %s",quote_identifier(schemaName)));
    Oid schemaOid=get_namespace_oid(schemaName,false);
    for(Size i=0;i<lengthof(lakeCatalogDdl);++i) {
        CHECK_FOR_INTERRUPTS();
        LakeCreateUtility(psprintf("CREATE TABLE %s.%s",quote_identifier(schemaName),lakeCatalogDdl[i].suffix));
    }
    for(Size i=0;i<lengthof(lakeCatalogDdl);++i) {
        Oid relid=get_relname_relid(lakeCatalogDdl[i].name,schemaOid);
        if(!OidIsValid(relid))
            ereport(ERROR,(errcode(ERRCODE_UNDEFINED_TABLE),errmsg("new lake catalog relation disappeared")));
        ATExecChangeOwner(relid,owner,false,AccessExclusiveLock);
    }
    AlterSchemaOwner_oid(schemaOid,owner);
    CommandCounterIncrement();
    LakeValidatePrivateNamespace(schemaOid,owner);
    LakeRegisterMetadataObjects(schemaOid);
    CommandCounterIncrement();

    /* Registration of the new empty physical relations precedes private SPI.
     * The initialization values below are the actual V1_1 protocol, not a
     * test fixture. InvalidSnapshot asks writable SPI to take its normal new
     * command snapshot; it does not change a previously fixed business view. */
    const char* snapshots=LakeMetadataRelation(schemaOid,"ducklake_snapshot");
    const char* changes=LakeMetadataRelation(schemaOid,"ducklake_snapshot_changes");
    const char* metadata=LakeMetadataRelation(schemaOid,"ducklake_metadata");
    const char* schemas=LakeMetadataRelation(schemaOid,"ducklake_schema");
    LakeMetadataRequireCount(LakeMetadataQuery(psprintf(
        "INSERT INTO %s(snapshot_id,snapshot_time,schema_version,next_catalog_id,next_file_id) "
        "VALUES(0,pg_catalog.now(),0,1,0)",snapshots),0,NULL,NULL,NULL,InvalidSnapshot,false,
        SPI_OK_INSERT,schemaOid),1,"initial lake snapshot");
    LakeMetadataRequireCount(LakeMetadataQuery(psprintf(
        "INSERT INTO %s(snapshot_id,changes_made,author,commit_message,commit_extra_info) "
        "VALUES(0,'created_schema:\"main\"',NULL,NULL,NULL)",changes),0,NULL,NULL,NULL,
        InvalidSnapshot,false,SPI_OK_INSERT,schemaOid),1,"initial lake changes");
    Oid rootType=TEXTOID; Datum rootValue=CStringGetTextDatum(canonicalRoot);
    LakeMetadataRequireCount(LakeMetadataQuery(psprintf(
        "INSERT INTO %s(key,value,scope,scope_id) VALUES "
        "('version','1.1',NULL,NULL),('created_by','openGauss og_lake " OG_LAKE_MODULE_VERSION "',NULL,NULL),"
        "('data_path',$1,NULL,NULL),('encrypted','false',NULL,NULL)",metadata),1,&rootType,&rootValue,
        NULL,InvalidSnapshot,false,SPI_OK_INSERT,schemaOid),4,"initial lake metadata");
    Oid uuidType=UUIDOID; Datum uuid=OidFunctionCall0(F_GEN_RANDOM_UUID);
    LakeMetadataRequireCount(LakeMetadataQuery(psprintf(
        "INSERT INTO %s(schema_id,schema_uuid,begin_snapshot,end_snapshot,schema_name,path,path_is_relative,parent_schema_id) "
        "VALUES(0,$1,0,NULL,'main','main/',true,NULL)",schemas),1,&uuidType,&uuid,NULL,InvalidSnapshot,false,
        SPI_OK_INSERT,schemaOid),1,"initial lake schema");
    CommandCounterIncrement();
    LakePublishCatalogBinding(serverName,schemaOid,canonicalRoot,false);
    PG_RETURN_VOID();
}

bool LakeHasFlushVersion(void)
{
    char* version=LakeInstalledVersion();
    return version!=NULL&&(strcmp(version,"0.11")==0||strcmp(version,"0.12")==0||strcmp(version,"0.13")==0||(strcmp(version,"0.14")==0||(strcmp(version,"0.15")==0||(strcmp(version,"0.16")==0||(strcmp(version,"0.17")==0||(strcmp(version,"0.18")==0||(strcmp(version,"0.19")==0||(strcmp(version,"0.20")==0||(strcmp(version,"0.21")==0||(strcmp(version,"0.22")==0||(strcmp(version,"0.23")==0||LakeNativeFutureContractEnabled(version))))))))))));
}

void LakeRequireFlushVersion(void)
{
    if(!LakeHasFlushVersion())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake flush requires extension 0.11")));
}

/* Version check is consumed before execution descriptors are shared. */
bool LakeSmallintVersionEnabled(void)
{
    char* version=LakeInstalledVersion();
    bool enabled=version!=NULL&&(strcmp(version,"0.20")==0||(strcmp(version,"0.21")==0||(strcmp(version,"0.22")==0||(strcmp(version,"0.23")==0||LakeNativeFutureContractEnabled(version)))));
    if(version!=NULL)pfree(version);
    return enabled;
}

bool LakeHasColumnRenameVersion(void)
{
    char* version=LakeInstalledVersion();
    return version!=NULL && (strcmp(version,"0.18")==0||(strcmp(version,"0.19")==0||(strcmp(version,"0.20")==0||(strcmp(version,"0.21")==0||(strcmp(version,"0.22")==0||(strcmp(version,"0.23")==0||LakeNativeFutureContractEnabled(version)))))));
}

void LakeRequireColumnRenameVersion(void)
{
    if(!LakeHasColumnRenameVersion())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake column RENAME requires extension 0.18")));
}

void LakeRequireTableRenameVersion(void)
{
    char* version=LakeInstalledVersion();
    if(version==NULL || (strcmp(version,"0.13")!=0&&(strcmp(version,"0.14")!=0&&(strcmp(version,"0.15")!=0&&(strcmp(version,"0.16")!=0&&(strcmp(version,"0.17")!=0&&(strcmp(version,"0.18")!=0&&(strcmp(version,"0.19")!=0&&(strcmp(version,"0.20")!=0&&(strcmp(version,"0.21")!=0&&(strcmp(version,"0.22")!=0&&(strcmp(version,"0.23")!=0&&!LakeNativeFutureContractEnabled(version)))))))))))))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake table RENAME requires extension0.13")));
}

bool LakeHasIndexCatalogVersion(void)
{
    char* version=LakeInstalledVersion();
    return version!=NULL && (strcmp(version,"0.14")==0||(strcmp(version,"0.15")==0||(strcmp(version,"0.16")==0||(strcmp(version,"0.17")==0||(strcmp(version,"0.18")==0||(strcmp(version,"0.19")==0||(strcmp(version,"0.20")==0||(strcmp(version,"0.21")==0||(strcmp(version,"0.22")==0||(strcmp(version,"0.23")==0||LakeNativeFutureContractEnabled(version)))))))))));
}

bool LakeHasInlineIndexVersion(void)
{
    char* version=LakeInstalledVersion();
    return version!=NULL && (strcmp(version,"0.15")==0||(strcmp(version,"0.16")==0||(strcmp(version,"0.17")==0||(strcmp(version,"0.18")==0||(strcmp(version,"0.19")==0||(strcmp(version,"0.20")==0||(strcmp(version,"0.21")==0||(strcmp(version,"0.22")==0||(strcmp(version,"0.23")==0||LakeNativeFutureContractEnabled(version))))))))));
}


bool LakeHasIndexRootVersion(void)
{
    char* version=LakeInstalledVersion();
    return version!=NULL && (strcmp(version,"0.16")==0||(strcmp(version,"0.17")==0||(strcmp(version,"0.18")==0||(strcmp(version,"0.19")==0||(strcmp(version,"0.20")==0||(strcmp(version,"0.21")==0||(strcmp(version,"0.22")==0||(strcmp(version,"0.23")==0||LakeNativeFutureContractEnabled(version)))))))));
}

/* Future SQL21 capability; root owns the final combined runtime version. */
bool LakeRewriteVersionEnabled(void)
{
    char* version=LakeInstalledVersion();
    bool enabled=version!=NULL&&(strcmp(version,"0.21")==0||(strcmp(version,"0.22")==0||(strcmp(version,"0.23")==0||LakeNativeFutureContractEnabled(version))));
    if(version)pfree(version);return enabled;
}
