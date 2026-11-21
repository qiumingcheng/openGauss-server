/* Statement-lifetime, dedicated-serial PARAM_EXEC immutable graph/PQ images.
 * No query LUT, scalar mask, source row or candidate survives a rescan. */
#ifndef OG_LAKE_ANN_RESCAN_H
#define OG_LAKE_ANN_RESCAN_H
#include "postgres.h"
#include "lake_ann_format.h"
struct LakeAnnRescanCache;
struct LakeAnnRescanEntry;
struct LakePqInput;
struct LakeAnnRescanKey {
    int64 file_id,column_id;
    int dimension,metric;
    const char *index_uuid,*object_version,*directory,*source_hash,*schema_hash,*context_hash,*manifest_hash;
};
struct LakeAnnRescanStats {uint64 hits,misses,loads,evictions,invalidations,queries,bytes,peak;};
extern LakeAnnRescanCache* LakeAnnRescanCreate(MemoryContext);
extern void LakeAnnRescanClose(LakeAnnRescanCache**);
extern LakeAnnRescanStats LakeAnnRescanStatsRead(const LakeAnnRescanCache*);
extern LakeAnnRescanEntry* LakeAnnRescanFind(LakeAnnRescanCache*,const LakeAnnRescanKey*,bool countMiss=true);
extern LakeAnnRescanEntry* LakeAnnRescanLoad(LakeAnnRescanCache*,const LakeAnnRescanKey*,OgLakeAnnReader*,const LakePqInput*);
extern void LakeAnnRescanInvalidate(LakeAnnRescanCache*,LakeAnnRescanEntry*,const LakeAnnRescanKey*);
extern bool LakeAnnRescanIsPoisoned(const LakeAnnRescanCache*,const LakeAnnRescanKey*);
extern const OgLakeAnnSpec* LakeAnnRescanSpec(const LakeAnnRescanEntry*);
extern const OgLakeAnnNode* LakeAnnRescanNode(const LakeAnnRescanEntry*,uint32);
extern const uint32* LakeAnnRescanEntries(const LakeAnnRescanEntry*,uint32*);
extern bool LakeAnnRescanQuery(LakeAnnRescanEntry*,const float*);
extern bool LakeAnnRescanPrecomputeDistances(LakeAnnRescanEntry*,uint32 nodeCount,float* output);
extern bool LakeAnnRescanDistance(LakeAnnRescanEntry*,uint32,float*);
extern void LakeAnnRescanEndQuery(LakeAnnRescanEntry*);
extern bool LakeAnnSharedPoolEnabled(void);
extern LakeAnnRescanEntry* LakeAnnSharedAcquire(LakeAnnRescanCache*,const LakeAnnRescanKey*,bool hasPq,MemoryContext owner);
extern LakeAnnRescanEntry* LakeAnnSharedLoad(LakeAnnRescanCache*,const LakeAnnRescanKey*,OgLakeAnnReader*,const LakePqInput*,MemoryContext owner);
extern void LakeAnnSharedInvalidate(const LakeAnnRescanKey*);
#endif
