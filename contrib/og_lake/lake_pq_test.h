/* Superuser diagnostics only. No SQL entry is installed by the extension.
 * Events called from resource cleanup are allocation/error-free counters. */
#ifndef OG_LAKE_PQ_TEST_H
#define OG_LAKE_PQ_TEST_H
#include "postgres.h"
enum LakePqTestEvent {LAKE_PQ_TEST_OPEN,LAKE_PQ_TEST_CLOSE,LAKE_PQ_TEST_DESTROY,
    LAKE_PQ_TEST_OWNER,LAKE_PQ_TEST_CONTEXT,LAKE_PQ_TEST_EXPLICIT,LAKE_PQ_TEST_REPARENT,
    LAKE_PQ_TEST_RESTART,LAKE_PQ_TEST_SUBCOMMIT,LAKE_PQ_TEST_ARM_DISTANCE,LAKE_PQ_TEST_DISTANCE};
extern void LakeTestPqEvent(LakePqTestEvent);
/* Modes1=pause with live frontier,2=ERROR with live frontier,
 * 3=real internal SUBCOMMIT while frontier lives,5=dedicated owner-first release,
 * 6=injected third distance failure requiring a complete raw restart. */
extern int LakeTestPqTakeMode(int64 sourceFile,int64* gate);
extern void LakeTestPqPause(int64 gate);
extern bool LakeTestPqFailDistance(void);
extern bool LakeTestPqDiagnosticArmed(void);
enum LakeAnnCacheEvent {LAKE_ANN_CACHE_OPEN,LAKE_ANN_CACHE_CLOSE,LAKE_ANN_CACHE_LOAD,LAKE_ANN_CACHE_HIT,LAKE_ANN_CACHE_EVICT,
    LAKE_ANN_CACHE_QUERY,LAKE_ANN_CACHE_BUDGET,LAKE_ANN_CACHE_ALLOC,LAKE_ANN_CACHE_FREE,LAKE_ANN_CACHE_REPARENT};
extern bool LakeTestAnnCacheObserved(void);
/* Private modes10/11/12 target a genuine cached image, never a legacy frontier.
 * Mode13 errors after cache registration but before immutable-image allocation.
 * Registration remains owned-test/superuser-only; no extension SQL installs it. */
extern bool LakeTestAnnCacheDiagnosticPending(void);
extern int LakeTestAnnCacheTakeMode(int64 sourceFile,int64* gate,bool beforeLoad=false);
extern void LakeTestAnnCachePause(int64 gate);
extern void LakeTestAnnCacheEvent(LakeAnnCacheEvent,uint64);
#endif
