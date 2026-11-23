#ifndef OG_LAKE_FILE_SCALAR_H
#define OG_LAKE_FILE_SCALAR_H
#include "lake_inline_private.h"
#include "access/skey.h"
#include "utils/memutils.h"
/* The caller holds registration, host relation and definition locks. Each
 * segment covers all physical positions, including ended/future versions. */
extern int LakeBuildScalarFileIndexes(LakeInlineBinding* binding,const char* indexUuid,
    int column,int64 generation,const char* definitionHash,Snapshot snapshot,bool bitmap=false);

/* Pure immutable descriptors only: no Relation, Snapshot, SPI Datum or SDK
 * handle may cross a Stream participant boundary. A missing entry means a
 * complete source scan. The caller's query owner keeps these strings alive. */
struct LakeScalarFileSource {
    int64 file_id;
    const char* object_version;
    const char* directory;
    const char* context_hash;
    const char* manifest_hash;
    bool bitmap;
};
struct LakeScalarFileSources {
    const char* index_uuid;
    int64 column_id;
    Oid type;
    int files;
    LakeScalarFileSource* entries;
};
extern LakeScalarFileSources* LakePrepareScalarFileSources(LakeInlineBinding*,const char* indexUuid,
    AttrNumber column,char* const* objectVersions,MemoryContext,Size maxBytes=0);
struct LakeFileScalarSelection;
extern LakeFileScalarSelection* LakeSelectScalarFiles(LakeInlineBinding* binding,const char* indexUuid,
    AttrNumber column,const ScanKeyData* key,char* const* objectVersions,MemoryContext context,const LakeScalarFileSources* sources=NULL);
extern bool LakeScalarFileCovered(const LakeFileScalarSelection* selection,int fileIndex);
extern bool LakeScalarFileBitmap(const LakeFileScalarSelection* selection,int fileIndex);
extern int64 LakeScalarFilePositionCount(const LakeFileScalarSelection* selection,int fileIndex);
extern int64 LakeScalarFilePosition(const LakeFileScalarSelection* selection,int fileIndex,int64 index);
extern bool LakeScalarFileContains(const LakeFileScalarSelection* selection,int fileIndex,int64 position);
extern void LakeScalarFileSelectionClose(LakeFileScalarSelection* selection);
/* Planning estimates never authorize pruning; execution verifies real files. */
extern void LakeScalarFileCoverageEstimate(const LakeInlineBinding* binding,const char* indexUuid,
    AttrNumber column,Snapshot snapshot,double* rows,double* bytes,double* groups,int* files,struct LakeCostLedger* ledger=NULL);
#endif
