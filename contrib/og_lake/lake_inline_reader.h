/* One native visible-row stream for query execution and ANALYZE. */
#ifndef OG_LAKE_INLINE_READER_H
#define OG_LAKE_INLINE_READER_H
#include "postgres.h"
#include "foreign/fdwapi.h"
#include "access/skey.h"
struct LakeScalarFileSources;

struct LakeTxnTable;
struct LakeInlineBinding;
struct LakeInlineReader;
struct LakeAnnScalarExpr;
struct LakeAnnFileSources;
struct LakeInlineRowIdentity {
    uint8 source;
    Oid inline_oid; /* Actual scanned carrier, never inferred from the current writer. */
    int64 row_id;
    int64 begin_snapshot;
    uint64 local_token;
    uint64 file_id;
    uint64 file_position;
    int64 end_snapshot; /* Physical-inline ANALYZE enumeration only. */
    bool ended;
};

extern LakeInlineReader* LakeInlineReaderOpen(LakeTxnTable* table,
    CommandId commandLimit, bool forAnalyze = false, bool physicalInline = false,
    char* const* expectedVersions = NULL, bool deferredFileAdmission = false);
extern int LakeInlineReaderAdmittedFiles(const LakeInlineReader*);
extern void LakeInlineReaderApplyFilePruning(LakeInlineReader*,const bool* skipped,int fileCount);
/* table may be an immutable SQL0.17 statement view. Its binding/snapshot stay
 * fixed across rescan; private rows and logical hides use its shared journal. */
/* Output Datums belong to CurrentMemoryContext, never to the next reader step. */
extern bool LakeInlineReaderNext(LakeInlineReader* reader, Datum* values,
    bool* nulls, LakeInlineRowIdentity* identity);
extern void LakeInlineReaderUseIndex(LakeInlineReader* reader, Oid indexOid,
    AttrNumber column, const ScanKeyData* key);
extern bool LakeInlineReaderUseAnn(LakeInlineReader* reader, Oid indexOid,
    AttrNumber column, int metric, Datum query, bool isnull);
extern void LakeInlineReaderUseFileScalar(LakeInlineReader* reader,const char* indexUuid,
    AttrNumber column,const ScanKeyData* key,const LakeScalarFileSources* sources=NULL);
extern int LakeInlineReaderScalarFiles(const LakeInlineReader* reader);
extern int LakeInlineReaderBitmapFiles(const LakeInlineReader* reader);
extern void LakeInlineReaderUseFileAnn(LakeInlineReader*,const char* uuid,AttrNumber column,
    int metric,Datum query,bool isnull,bool enabled,uint32 minimum,const LakeAnnScalarExpr* filter = NULL,
    const LakeAnnFileSources* prebound = NULL,bool hasResidualQual = false);
extern void LakeInlineReaderEnableAnnRescanCache(LakeInlineReader*);
extern int LakeInlineReaderPqFiles(const LakeInlineReader*);
struct LakeAnnRescanStats;
extern LakeAnnRescanStats LakeInlineReaderAnnRescanStats(const LakeInlineReader*);
extern int LakeInlineReaderAnnFiles(const LakeInlineReader*);
extern void LakeInlineReaderReScan(LakeInlineReader* reader);
extern void LakeInlineReaderClose(LakeInlineReader* reader);
class VectorBatch;
extern void LakeInlineReaderSetProjected(LakeInlineReader* reader, const bool* projected, int count);
extern int LakeInlineReaderFillVectorBatch(LakeInlineReader* reader, VectorBatch* batch,
    Size* payloadBytes, Size payloadLimit, Size* colBatchBytes,
    bool* needFallback, bool* eos);
extern BlockNumber LakeInlinePhysicalPages(const LakeInlineBinding* binding);
extern void LakeInstallAnalyzeCallbacks(FdwRoutine* routine);
extern int64 LakeInlineReaderAnnCandidateRows(const LakeInlineReader*);
#endif
