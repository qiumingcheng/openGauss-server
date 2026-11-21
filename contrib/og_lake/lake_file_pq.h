/* Native optional CPU PQ sidecars. Catalog format2 binds both immutable roots;
 * the raw graph stays authoritative for topology and fallback. */
#ifndef OG_LAKE_FILE_PQ_H
#define OG_LAKE_FILE_PQ_H
#include "postgres.h"
#include "lake_ann_format.h"
#include "lake_file_sdk.h"
#define LAKE_PQ_MODEL_FILE "ann_pq_model.parquet"
#define LAKE_PQ_CODES_FILE "ann_pq_codes.parquet"
#define LAKE_PQ_MANIFEST_FILE "ann_pq_manifest.parquet"
struct LakePqDescriptor {
    char graph_root[65],pq_root[65];
    uint32 chunks,training_rows,node_count;
};
struct LakePqBuilt {LakeFileCandidate* files[3];char* descriptor;bool enabled;};
struct LakePqLease;
struct LakePqInput {
    LakePqDescriptor descriptor;
    int64 source_file;
    const char* source_hash;
    const char* schema_hash;
    const char* directory;
};
extern bool LakePqCompiled(void);
/* Strict canonical TEXT, not a sidecar self-reported trust root. Format1 is
 * exactly64hex. Format2 is a versioned dual-root descriptor in manifest_hash. */
extern bool LakePqParseDescriptor(const char*,int format,LakePqDescriptor*);
extern LakePqBuilt LakePqBuild(OgLakeAnnReader*,int64 sourceFile,const char* sourceHash,
    const char* schemaHash,const char* graphRoot,const char* directory,uint32 chunks);
extern bool LakePqVerify(const OgLakeParquetSdkApi*,const OgLakeAnnSpec*,
    const LakePqDescriptor*,int64,const char*,const char*,const char*,int* status=NULL);
extern LakePqLease* LakePqOpen(const OgLakeParquetSdkApi*,const OgLakeAnnSpec*,
    const LakePqDescriptor*,int64 sourceFile,const char* sourceHash,const char* schemaHash,
    const char* directory,const float* query);
/* False disables PQ; caller MUST restart the whole frontier before emitting.
 * Cancellation raises query cancellation, never converts to raw retry. */
extern bool LakePqPrecomputeDistances(LakePqLease*,uint32 nodeCount,float* output);
extern bool LakePqDistance(LakePqLease*,uint32 node,float*);
extern void LakePqClose(LakePqLease**);
#endif
