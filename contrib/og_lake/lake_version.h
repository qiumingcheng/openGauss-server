#ifndef OG_LAKE_VERSION_H
#define OG_LAKE_VERSION_H
#include "utils/snapshot.h"
#define OG_LAKE_MODULE_VERSION "0.74"
#define OG_LAKE_NATIVE_PARTITION_EXPANDED_SQL_VERSION "0.74"
#define OG_LAKE_TRUNCATE_PARTITION_SQL_VERSION "0.73"
#define OG_LAKE_VECTOR_SMP_SQL_VERSION "0.72"
#define OG_LAKE_JSON_FORMAT_SQL_VERSION "0.71"
#define OG_LAKE_ALTER_TYPE_OPS_SQL_VERSION "0.70"
#define OG_LAKE_PHYSICAL_CLEANUP_SQL_VERSION "0.69"
#define OG_LAKE_SNAPSHOT_PIN_SQL_VERSION "0.68"
#define OG_LAKE_IX_OBS_SQL_VERSION "0.67"
#define OG_LAKE_EXTENDED_DEFAULTS_SQL_VERSION "0.66"
#define OG_LAKE_STATS_EXTENDED_SQL_VERSION "0.65"
#define OG_LAKE_IX_SMP_EXTENDED_SQL_VERSION "0.64"
#define OG_LAKE_PARTITION_COMBO_SQL_VERSION "0.63"
#define OG_LAKE_COPY_IO_SQL_VERSION "0.62"
#define OG_LAKE_EXTERNAL_FILE_SQL_VERSION "0.61"
#define OG_LAKE_MERGE_ADJACENT_SQL_VERSION "0.60"
#define OG_LAKE_BIND_SNAPSHOT_SQL_VERSION "0.59"
#define OG_LAKE_TABLE_CHANGES_SQL_VERSION "0.58"
#define OG_LAKE_TIME_TRAVEL_SQL_VERSION "0.57"
#define OG_LAKE_LAST_COMMITTED_SQL_VERSION "0.56"
#define OG_LAKE_SNAPSHOT_METADATA_SQL_VERSION "0.55"
#define OG_LAKE_NUMERIC_BUSINESS_SQL_VERSION "0.54"
#define OG_LAKE_JSON_BUSINESS_SQL_VERSION "0.53"
#define OG_LAKE_UUID_BUSINESS_SQL_VERSION "0.52"
#define OG_LAKE_MULTI_FILE_REWRITE_SQL_VERSION "0.51"
#define OG_LAKE_REWRITE_MAX_SOURCES 16
#define OG_LAKE_SNAPSHOT_EXPIRY_SQL_VERSION "0.50"
#define OG_LAKE_CREATE_LITERAL_DEFAULT_SQL_VERSION "0.49"
#define OG_LAKE_PARTITION_REWRITE_SQL_VERSION "0.48"
#define OG_LAKE_INT2_ROW_SMP_SQL_VERSION "0.47"
#define OG_LAKE_INT2_SCALAR_SQL_VERSION "0.46"
#define OG_LAKE_JOURNAL_SPILL_SQL_VERSION "0.38"
#define OG_LAKE_ADD_COLUMN_SQL_VERSION "0.37"
#define OG_LAKE_LOGICAL_DROP_SQL_VERSION "0.36"
#define OG_LAKE_CORE_FOLLOWUPS_SQL_VERSION "0.35"
/* Installed SQL capability; the module default alone grants no rights. */
extern bool LakeIxSmpExtendedVersionEnabled(void);
extern bool LakePartitionComboVersionEnabled(void);
extern bool LakeCopyIoVersionEnabled(void);
extern bool LakeExternalFileVersionEnabled(void);
extern bool LakeMergeAdjacentVersionEnabled(void);
extern bool LakeBindSnapshotVersionEnabled(void);
extern bool LakeTableChangesVersionEnabled(void);
extern bool LakeTimeTravelVersionEnabled(void);
extern bool LakeLastCommittedVersionEnabled(void);
extern bool LakeSnapshotMetadataVersionEnabled(void);
extern bool LakeCoreFollowupsVersionEnabled(void);
extern bool LakeInt2ScalarIndexVersionEnabled(void);
extern bool LakeInt2RowSmpVersionEnabled(void);
extern bool LakePartitionRewriteVersionEnabled(void);
extern bool LakeCreateLiteralDefaultVersionEnabled(void);
extern bool LakeSnapshotExpiryVersionEnabled(void);
extern bool LakeMultiFileRewriteVersionEnabled(void);
extern int LakeRewriteSourceLimit(void);
extern bool LakeStatsExtendedVersionEnabled(void);
extern bool LakeExtendedDefaultsVersionEnabled(void);
extern bool LakeIxObsVersionEnabled(void);
extern bool LakeSnapshotPinVersionEnabled(void);
extern bool LakePhysicalCleanupVersionEnabled(void);
extern bool LakeAlterTypeOpsVersionEnabled(void);
extern bool LakeJsonFormatVersionEnabled(void);
extern bool LakeVectorSmpVersionEnabled(void);
extern bool LakeTruncatePartitionVersionEnabled(void);
extern bool LakeNativePartitionExpandedVersionEnabled(void);
extern void LakeInvalidateRelationSafe(Oid relationOid);
extern void LakeInvalidateCatalogRelations(Oid serverOid, Snapshot snapshot);
extern void LakeHostRecordAnnRescan(uint64 hits, uint64 misses, uint64 loads,
    uint64 evictions, uint64 invalidations, uint64 queries, uint64 peakBytes);
extern char* LakeHostAnnRescanJson(void);
extern void LakeRecordAutoAnalyzeOutcome(bool attempted, bool succeeded);
extern void LakeSnapshotPinRegister(Oid serverOid, int64 snapshotId, const char* pinKind, uint64 handle);
extern void LakeSnapshotPinUnregisterHandle(uint64 handle);
extern void LakeSnapshotPinReleaseSession(void);
extern int64 LakeSnapshotPinMinForCatalog(Oid databaseOid, Oid serverOid);
extern void LakeSnapshotPinStatsForCatalog(Oid databaseOid, Oid serverOid, int* countOut, int64* minOut);
#endif
