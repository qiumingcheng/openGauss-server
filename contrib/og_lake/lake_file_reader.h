#ifndef OG_LAKE_FILE_READER_H
#define OG_LAKE_FILE_READER_H
#include "lake_inline_private.h"
#include "access/skey.h"
struct LakeScalarFileSources;
struct LakeFileReader;
extern const char* LakeFileReaderObjectVersion(const LakeFileReader*,int fileIndex);
/* Borrow the prevalidated visible delete union until the reader context ends. */
extern const int64* LakeFileReaderVisibleDeletes(const LakeFileReader*,int fileIndex,int* count);
struct LakeInlineRowIdentity;
struct LakeTxnTable;
struct LakeAnnScalarExpr;
struct LakeAnnFileSources;
struct LakeAnnFileMask;
/* Reader-observed I/O is expressly not total I/O: independent footer_open
 * calls have no SDK counter. Reserve those conservatively and expose both. */
struct LakeAnalyzeIoBudget {
    uint64 max_read_bytes,max_plan_bytes,max_decoded_bytes;
    uint64 planned_bytes,planned_decoded_bytes,reserved_footer_bytes;
    uint64 observed_reader_bytes,observed_reader_calls,observed_decoded_rows;
    uint64 unobserved_footer_opens;
    uint32 files,groups;
    int64 started;
    int max_milliseconds;
};
typedef void (*LakeAnalyzeRowVisitor)(Datum*,bool*,const LakeInlineRowIdentity*,void*);
extern void LakeFileReadSamplePositions(LakeTxnTable*,CommandId commandLimit,int fileIndex,const int64* positions,int count,
    LakeAnalyzeRowVisitor,void*,LakeAnalyzeIoBudget*);
extern LakeFileReader* LakeFileReaderOpen(LakeInlineBinding* binding,
    LakeTxnTable* table = NULL, CommandId commandLimit = InvalidCommandId,
    LakeAnalyzeIoBudget* analyzeBudget = NULL, char* const* expectedVersions = NULL,
    bool deferredAdmission = false);
/* Every selected source is admitted before ANY inline/private/file row is
 * returned. A true skip entry is a proof result, not an index coverage hint. */
/* Rewrite-only same-batch full physical key proof; ordinary readers remain unchanged. */
extern void LakeFileReaderVerifyRewritePartition(LakeFileReader*);
extern int LakeFileReaderAdmittedFiles(const LakeFileReader*);
extern void LakeFileReaderRequireAdmitted(const LakeFileReader*);
extern void LakeFileReaderApplyPruning(LakeFileReader*,const bool* skipped,int fileCount);
/* A statement binding is never replaced during this reader's lifetime. The
 * optional table supplies the shared logical-row overlay at commandLimit. */
extern bool LakeFileReaderNext(LakeFileReader* reader,Datum* values,bool* nulls,LakeInlineRowIdentity* identity);
extern void LakeFileReaderReScan(LakeFileReader* reader);
extern void LakeFileReaderClose(LakeFileReader* reader);
extern void LakeFileReaderUseScalar(LakeFileReader* reader,const char* indexUuid,
    AttrNumber column,const ScanKeyData* key,const LakeScalarFileSources* sources=NULL);
extern int LakeFileReaderScalarFiles(const LakeFileReader* reader);
extern int LakeFileReaderBitmapFiles(const LakeFileReader* reader);
/* minimum is the requested SQL Top-K; insufficient candidates scan the file. */
extern void LakeFileReaderUseAnn(LakeFileReader*,const char* indexUuid,AttrNumber column,
    int metric,Datum query,bool enabled,uint32 minimum,const LakeAnnScalarExpr* filter = NULL,
    const LakeAnnFileSources* prebound = NULL,bool hasResidualQual = false);
extern void LakeFileReaderEnableAnnRescanCache(LakeFileReader*);
struct LakeAnnRescanStats;
extern LakeAnnRescanStats LakeFileReaderAnnRescanStats(const LakeFileReader*);
extern int LakeFileReaderPqFiles(const LakeFileReader*);
extern int LakeFileReaderAnnFiles(const LakeFileReader*);
struct LakeSdkHandles;
struct OgLakeSdkIoStats;
typedef void (*LakeDeleteVersionVisitor)(int64 position,int64 snapshot,void* argument);
extern void LakeFileVisitDeletes(LakeDataFile* file,LakeSdkHandles* handles,
    LakeDeleteVersionVisitor visitor,void* argument,OgLakeSdkIoStats* stats = NULL,LakeAnalyzeIoBudget* budget = NULL);
/* Derived indexes cover every physical position, including ended/future
 * versions. Visibility and deletion filtering belongs to the later query. */
typedef void (*LakeScalarKeyVisitor)(int64 fileId,int64 position,Datum key,bool isnull,void* argument);
extern void LakeFileVisitScalarKeys(LakeInlineBinding* binding,int fileIndex,AttrNumber column,
    const char* objectVersion,LakeScalarKeyVisitor visitor,void* argument,bool metadataPrebound=false);
extern bool LakeFileReadAnnScalarMask(LakeInlineBinding*,int fileIndex,const char* objectVersion,
    const LakeAnnScalarExpr*,LakeAnnFileMask*,MemoryContext,bool metadataPrebound=false);
/* Complete physical vector input, including future/deleted versions. Only the
 * vector and needed hidden identities are decoded. Callback borrows its row
 * and Datum until return; the row scratch context resets after every input. */
typedef void (*LakeAnnRowVisitor)(const LakeInlineRowIdentity*,Datum,bool,void*);
extern void LakeFileVisitAnnRows(LakeInlineBinding*,int fileIndex,AttrNumber column,
    const char* objectVersion,LakeAnnRowVisitor,void*);
/* NULL means this provider has no verifiable immutable-object identity. */
struct ArrowArray;
struct ArrowSchema;
struct OgLakeSdkBatch;
class VectorBatch;
extern bool LakeFileDecodeNull(const ArrowArray* array,int64 row);
extern Datum LakeFileDecodeValue(const ArrowArray* array,const ArrowSchema* schema,
    int64 row,Oid type,Size* binaryRowBytes=NULL,int32 typmod=-1);
extern OgLakeSdkBatch* LakeFileCheckArrowBatch(OgLakeSdkBatch* batch,int columns);
extern char* LakeFileObjectVersion(LakeInlineBinding* binding,int fileIndex);
extern int64 LakeFileReaderAnnCandidateRows(const LakeFileReader*);
extern void LakeFileReaderSetProjected(LakeFileReader* reader,const bool* projected,int count);
extern int LakeFileReaderFillVectorBatch(LakeFileReader* reader,VectorBatch* batch,
    const bool* projected,Size* payloadBytes,Size payloadLimit,Size* colBatchBytes,
    bool* needFallback,bool* eos);
#endif
