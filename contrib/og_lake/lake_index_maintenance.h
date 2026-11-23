/* Background file-index phases. No standard DuckLake catalog changes. */
#ifndef OG_LAKE_INDEX_MAINTENANCE_H
#define OG_LAKE_INDEX_MAINTENANCE_H
#include "lake_inline_private.h"
#include "lake_file_sdk.h"

enum LakeIndexArtifactHealth { LAKE_ARTIFACT_VALID,LAKE_ARTIFACT_REBUILD,LAKE_ARTIFACT_RETRY };
struct LakeIndexArtifactTask {
    LakeInlineBinding* binding; /* Exactly one physical file; no inline/deletes. */
    int column,metric;
    bool bitmap,pq;
    uint32 pq_chunks;
    int64 generation;
    char *uuid,*definition_hash,*root,*segment,*token;
    char *version,*schema,*source,*context,*directory,*manifest;
    int algorithm,format;
    bool known;
    char *known_segment,*known_token,*known_directory,*known_manifest,*known_source,*known_schema,*known_checksum,*known_path;
    int known_algorithm,known_format;
    int64 known_rows,known_bytes;
    LakeFileCandidate* candidates[5];
    int candidate_count;
};
/* Method adapters reuse the synchronous algorithm/format implementation.
 * These functions do object I/O only, with no index/catalog DDL guard. */
extern void LakeAnnDescribeMaintenanceTask(LakeIndexArtifactTask*);
extern LakeIndexArtifactHealth LakeAnnCheckMaintenanceTask(LakeIndexArtifactTask*);
extern void LakeAnnBuildMaintenanceTask(LakeIndexArtifactTask*);
extern void LakeScalarDescribeMaintenanceTask(LakeIndexArtifactTask*);
extern LakeIndexArtifactHealth LakeScalarCheckMaintenanceTask(LakeIndexArtifactTask*);
extern void LakeScalarBuildMaintenanceTask(LakeIndexArtifactTask*);
extern LakeIndexArtifactHealth LakeIndexArtifactFiles(const char* directory,const char* const* names,int count);
extern bool LakeIndexArtifactMetadata(const LakeIndexArtifactTask*);

struct LakeIndexMaintenanceResult {
    bool attempted,built,verified,obsolete,retry;
    Oid relation_oid;
    int64 source_file;
    char index_uuid[37];
};
/* Worker only, entered and returned outside a DB transaction. One complete
 * segment at most; uses its own budget, not the short flush-worker budget. */
extern LakeIndexMaintenanceResult LakeMaintainOneFileIndex(void);
extern void LakeIndexMaintenanceTestGate(const LakeIndexArtifactTask*,int phase);
/* Existing maintenance shared memory owns these bounded scheduling hints.
 * They never authorize catalog publication or represent a RAC task lease. */
extern bool LakeIndexMaintenanceCursor(bool definition,const char* uuid,bool advance,
    int64* after,int64* end,char* definition_after,int64 generation=-1);
extern void LakeIndexCursorReset(const char* uuid);
extern char* LakeMaintenanceTelemetryJson(void);
#endif
