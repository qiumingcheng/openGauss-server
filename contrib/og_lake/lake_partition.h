/* Standard DuckLake file partitions; immutable host-only descriptors. */
#ifndef OG_LAKE_PARTITION_H
#define OG_LAKE_PARTITION_H
#include "postgres.h"
#include "utils/snapshot.h"
#include "utils/rel.h"

#define OG_LAKE_IDENTITY_PARTITION_SQL_VERSION "0.22"
#define OG_LAKE_FILE_PARTITION_KEYS_SQL_VERSION "0.29"
#define OG_LAKE_BUCKET_PARTITION_SQL_VERSION "0.30"
#define OG_LAKE_TIME_PARTITION_SQL_VERSION "0.32"
#define OG_LAKE_TIME_PARTITION_PRUNE_SQL_VERSION "0.33"

/* Each rule requires its installed capability or a reviewed inheriting SQL contract. */
enum LakePartitionTransform {
    LAKE_PARTITION_IDENTITY = 1, LAKE_PARTITION_BUCKET = 2,
    LAKE_PARTITION_YEAR = 3, LAKE_PARTITION_MONTH = 4,
    LAKE_PARTITION_DAY = 5, LAKE_PARTITION_HOUR = 6,
    LAKE_PARTITION_EPOCH_YEAR = 7, LAKE_PARTITION_EPOCH_MONTH = 8,
    LAKE_PARTITION_EPOCH_DAY = 9, LAKE_PARTITION_EPOCH_HOUR = 10,
    LAKE_PARTITION_TRUNCATE = 11
};
struct LakePartitionField {
    int64 column_id;
    int column_index; /* Current top-level business descriptor, zero based. */
    Oid type; /* Source/business OID, never the transformed bucket OID. */
    Oid result_type; /* identity=type; bucket=INT4OID; time=INT8OID; truncate=INT2->INT4,INT4->INT8,else type. */
    int32 typmod;
    Oid collation;
    bool host_bound; /* Set only after validating the actual host descriptor. */
    LakePartitionTransform transform;
    int64 parameter; /* identity/time=0; bucket/truncate=1..INT32_MAX. */
};
struct LakePartitionSpec {
    bool present; /* Includes a legal empty RESET marker. */
    bool enabled;
    int64 partition_id;
    int64 begin_snapshot;
    int64 end_snapshot; /* -1 if NULL; begin==end is legal historically. */
    int key_count;
    LakePartitionField* keys; /* Ordered by exact standard partition_key_index. */
};
struct LakePartitionValue {
    bool is_null;
    Datum value; /* Fully owned; text is a copied varlena, never a SPI pointer. */
    char* canonical_text; /* NULL iff is_null; DuckLake's typed text encoding. */
};
struct LakePartitionTuple {
    int count;
    LakePartitionValue* values;
};
struct LakePartitionFile {
    LakePartitionSpec spec; /* The file's historical partition_id, not current. */
    LakePartitionTuple tuple;
};
/* Retain internal call-site names while introducing the generic descriptor. */
typedef LakePartitionSpec LakeIdentityPartitionSpec;
typedef LakePartitionFile LakeIdentityPartitionFile;
struct LakeInlineBinding;
struct LakeDataFile;

/* Installed SQL30 bucket rules, retained by reviewed SQL31..33 contracts. */
extern bool LakeBucketPartitionVersionEnabled(void);
/* Installed SQL32 time transforms; SQL33 retains the same source/value rules. */
extern bool LakeTimePartitionVersionEnabled(void);
/* Equality/NULL proof requires the exact installed SQL33 query capability. */
extern bool LakeTimePartitionPruneVersionEnabled(void);
extern bool LakePartitionIsTimeTransform(LakePartitionTransform transform);
extern bool LakePartitionIsTruncateTransform(const char* text);
extern Oid LakePartitionResultType(LakePartitionTransform transform,Oid type);
extern bool LakePartitionTruncateInt64Floor(int64 value,int64 width,int64* output);
extern bool LakePartitionFieldSupported(const LakePartitionField* field);
extern bool LakePartitionNeedsSourceVersionMap(const LakePartitionSpec* spec);
extern bool LakePartitionParseTransform(const char* text,LakePartitionTransform* transform,int64* parameter);
extern char* LakePartitionTransformText(const LakePartitionField* field);
extern Datum LakePartitionApplyTransform(const LakePartitionField* field,Datum value);
extern bool LakePartitionHasBucket(const LakePartitionSpec* spec);
extern bool LakeIdentityPartitionVersionEnabled(void);
/* Installed SQL29 ordered keys, retained through SQL33; SQL22..26 keep one int4 key. */
extern bool LakeIdentityPartitionKeysVersionEnabled(void);
extern void LakeRequireIdentityPartitionVersion(void);
extern bool LakeIdentityPartitioned(const LakeInlineBinding* binding);
extern bool LakeIdentityPartitionSpecEqual(const LakeIdentityPartitionSpec* a,
    const LakeIdentityPartitionSpec* b);
extern bool LakeIdentityPartitionFileEqual(const LakeIdentityPartitionFile* a,
    const LakeIdentityPartitionFile* b);
extern void LakeLoadCurrentIdentityPartition(LakeInlineBinding* binding,Snapshot snapshot);
extern void LakeLoadInlineIdentityPartition(LakeInlineBinding* binding,
    int64 sourceSchemaVersion,int64 sourceOrigin,Snapshot snapshot);
extern void LakeLoadFileIdentityPartition(LakeInlineBinding* binding,LakeDataFile* file,
    int64 partitionId,Snapshot snapshot);
extern void LakeRejectIdentityPartitionIntersection(const LakeInlineBinding* binding,
    const char* operation);
/* Called while the real host relation remains locked and after column checks.
 * This completes construction; scan/publication consumers do not mutate it. */
extern void LakeBindPartitionHostColumns(LakeInlineBinding* binding,Relation relation);
extern int LakePartitionFindIdentityKey(const LakePartitionSpec* spec,int64 columnId);
extern LakePartitionSpec LakePartitionCopySpec(const LakePartitionSpec* source,MemoryContext target);
extern LakePartitionTuple LakePartitionCopyTuple(const LakePartitionSpec* spec,
    const LakePartitionTuple* source,MemoryContext target);
extern bool LakePartitionTupleEqual(const LakePartitionSpec* spec,
    const LakePartitionTuple* a,const LakePartitionTuple* b);
/* attrs maps business column_index to zero-based input slot positions. */
extern bool LakePartitionTupleMatches(const LakePartitionSpec* spec,
    const LakePartitionTuple* tuple,const Datum* values,const bool* nulls,const int* attrs);
extern LakePartitionTuple LakePartitionFormTuple(const LakePartitionSpec* spec,
    const Datum* values,const bool* nulls,const int* attrs,MemoryContext target);
#endif
