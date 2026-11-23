/* Transaction-private standard rewrite_delete & merge_adjacent; no physical GC or new row IDs. */
#ifndef OG_LAKE_REWRITE_H
#define OG_LAKE_REWRITE_H
#include "lake_flush.h"
#include "lake_version.h"
struct LakeRewriteOperation {
    MemoryContext context;
    LakeTxnTable* table;
    SubTransactionId owner_subid;
    int64 cutoff,next_file_at_cutoff;
    int source_count;
    int64 source_ids[OG_LAKE_REWRITE_MAX_SOURCES];
    char* source_records[OG_LAKE_REWRITE_MAX_SOURCES];
    char* delete_records[OG_LAKE_REWRITE_MAX_SOURCES];
    char* source_versions[OG_LAKE_REWRITE_MAX_SOURCES];
    bool partitioned;
    LakePartitionFile partition; /* Independent op-owned historical spec and complete tuple. */
    char* partition_policy_record;
    char* partition_spec_record;
    char* partition_value_records[OG_LAKE_REWRITE_MAX_SOURCES];
    LakeInlineBinding* input;
    int64 rows;
    int64* null_counts;
    char** minima;
    char** maxima;
    LakeFlushOutput output;
};
extern void LakeRewriteCheckSourceObjects(LakeRewriteOperation*);
extern void LakeRewriteCheckSources(LakeRewriteOperation*,Snapshot,bool capture=false);
extern void LakeRewriteCheckBinding(const LakeRewriteOperation*,const LakeInlineBinding*);
extern bool LakeTryStageMergeAdjacent(Oid relationOid,int maxFiles,int64 targetFileSizeOverride,
    bool backgroundCall,char** schemaNameOut,char** tableNameOut,int64* filesProcessedOut,int64* filesCreatedOut);
extern "C" Datum og_lake_rewrite_data_files(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_merge_adjacent_files(PG_FUNCTION_ARGS);
#endif
