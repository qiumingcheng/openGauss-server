/* Native-side ownership only; SDK implementation stays behind its C ABI. */
#ifndef OG_LAKE_FILE_SDK_H
#define OG_LAKE_FILE_SDK_H
#include "postgres.h"
#include "utils/memutils.h"
#include "sdk/lake_parquet_sdk.h"
#include "lake_parquet_fd_sdk.h"
#include "vendor/arrow20/abi.h"

/* Standard DuckLake/Iceberg reserved Parquet field identities. */
#define LAKE_FILE_ROW_ID 2147483540LL
#define LAKE_FILE_SNAPSHOT_ID 2147483539LL
#define LAKE_DELETE_PATH_ID 2147483546LL
#define LAKE_DELETE_POSITION_ID 2147483545LL
#define LAKE_FILE_DELETE_POSITION_LIMIT (1024 * 1024)

struct LakeSdkHandles {
    const OgLakeParquetSdkApi* api;
    OgLakeSdkFooter* footer;
    OgLakeSdkReader* reader;
    OgLakeSdkBatch* batch;
};
struct LakeExternalFdSdk {
    const OgLakeParquetSdkApi* base;
    const OgLakeParquetDecimalApi* decimal;
    const OgLakeParquetFdApi* fd;
};
extern const OgLakeParquetSdkApi* LakeFileProvider(void);
extern const LakeExternalFdSdk* LakeFileExternalFdProvider(void);
extern const OgLakeParquetLifecycleApi* LakeFileLifecycleProvider(void);
extern const OgLakeParquetNestedApi* LakeFileNestedProvider(void);
extern const OgLakeParquetDecimalApi* LakeFileDecimalProvider(void);
extern void LakeFileCheckDecimalWith(const OgLakeParquetDecimalApi* decimalApi,
    const OgLakeSdkFooter* footer,int ordinal,int32 typmod);
extern void LakeFileCheckDecimal(const OgLakeSdkFooter* footer,int ordinal,int32 typmod);
extern void LakeFileReadDecimalParameters(const OgLakeSdkFooter* footer,int ordinal,int* precision,int* scale);
extern const OgLakeParquetMaintenanceApi* LakeFileMaintenanceProvider(void);
extern OgLakeSdkLimits LakeFileLimits(void);
extern LakeSdkHandles* LakeFileHandles(MemoryContext context);
extern void LakeFileHandlesClose(LakeSdkHandles* handles);
extern void LakeFileCheckStatus(int status,const OgLakeSdkError* error);
extern void LakeEnsureLocalDirectory(const char* path);

/* Session-owned candidates outlive transaction memory and ordinary resource
 * cleanup. Only immutable, finished candidates can be marked published.
 * Publication is still rolled back by a later transaction/subtransaction abort.
 * No SDK call belongs inside the catalog snapshot claim. */
struct LakeFileCandidate {
    const OgLakeParquetSdkApi* api;
    const OgLakeParquetLifecycleApi* lifecycle;
    OgLakeSdkWriter* writer;
    OgLakeSdkWrittenFile written;
    SubTransactionId owner_subid;
    bool finished;
    bool published;
    LakeFileCandidate* next;
    char path[MAXPGPATH];
};
extern LakeFileCandidate** LakeHostFileCandidatesAddress(void);
extern int LakeFileCandidateSlotsAvailable(void); /* Whole host transaction, not per group. */
extern LakeFileCandidate* LakeFileCandidateOpen(const OgLakeSdkWriteOptions* options,
    int listCount=0,const OgLakeSdkListField* lists=NULL,
    int decimalCount=0,const OgLakeSdkDecimalField* decimals=NULL);
extern void LakeFileCandidateFinish(LakeFileCandidate* candidate);
extern void LakeFileCandidatePublish(LakeFileCandidate* candidate);
extern void LakeFileCandidatesSubCommit(LakeFileCandidate* head,
    SubTransactionId child,SubTransactionId parent);
/* Called only after native locks/resources have been released. Does not
 * allocate database memory, raise ERROR or inspect catalog/transaction state. */
extern void LakeFileCandidatesEnd(LakeFileCandidate** head,bool committed,
    SubTransactionId child=InvalidSubTransactionId);
#endif
