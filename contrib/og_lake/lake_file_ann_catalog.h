#ifndef OG_LAKE_FILE_ANN_CATALOG_H
#define OG_LAKE_FILE_ANN_CATALOG_H
#include "lake_inline_private.h"
/* Returns -1 only when index_root is not configured, as scalar builds do. */
extern int LakeBuildAnnFileIndexes(LakeInlineBinding*,const char* uuid,int column,int metric,
    int64 generation,const char* definitionHash,Snapshot,bool pq=false,uint32 pqChunks=1);
/* POD-only metadata images prepared by the top consumer before worker startup.
 * Multiple READY segments per source retain their original order/fallback. */
struct LakeAnnFileSource {
    int64 file_id;int format;
    const char *object_version,*directory,*source_hash,*schema_hash,*context_hash,*manifest_hash;
};
struct LakeAnnFileSources {
    const char* index_uuid;int64 column_id;int metric;bool raw_only;
    uint32 count;LakeAnnFileSource* entries;
};
extern LakeAnnFileSources* LakePrepareAnnFileSources(LakeInlineBinding*,const char* uuid,
    AttrNumber column,int metric,char* const* versions,MemoryContext owner,Size maxBytes=0,bool rawOnly=false);
struct LakeFileAnnSelection;
struct LakeAnnScalarExpr;
struct LakeAnnRescanCache;
/* Derived reads complete before any source row is emitted. Unavailable or bad
 * graph artifacts leave their file uncovered; cancellation is always raised.
 * minimum is the SQL LIMIT, counted as distinct physical rows, not nodes. */
extern LakeFileAnnSelection* LakeSelectAnnFiles(LakeInlineBinding*,const char* uuid,
    AttrNumber column,int metric,Datum query,uint32 beam,uint32 minimum,char* const* objectVersions,
    const LakeAnnScalarExpr* filter = NULL,const LakeAnnFileSources* prebound = NULL,LakeAnnRescanCache* rescan = NULL,
    uint32 emitLimit = 0);
extern bool LakeAnnFileCovered(const LakeFileAnnSelection*,int fileIndex);
extern bool LakeAnnFilePqUsed(const LakeFileAnnSelection*,int fileIndex);
extern uint32 LakeAnnFilePositionCount(const LakeFileAnnSelection*,int fileIndex);
struct OgLakeAnnAddress;
extern const OgLakeAnnAddress* LakeAnnFilePosition(const LakeFileAnnSelection*,int fileIndex,uint32 index);
extern void LakeAnnFileCoverageEstimate(const LakeInlineBinding*,const char* uuid,AttrNumber column,
    Snapshot,double* rows,double* bytes,double* groups,int* files,struct LakeCostLedger* ledger=NULL);
#endif
