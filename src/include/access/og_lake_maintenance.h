/* Native maintenance threads. Cluster locks remain the host's responsibility. */
#ifndef OG_LAKE_MAINTENANCE_H
#define OG_LAKE_MAINTENANCE_H
#include "postgres.h"
#include "gs_thread.h"
struct LakeWorkerStart {
    int child_slot;
    int cancel_key;
    Oid database_oid;
    Oid after_relation;
    uint64 generation;
};
extern Size LakeMaintenanceShmemSize(void);
extern void LakeMaintenanceShmemInit(void);
extern bool IsNativeLakeMaintenanceProcess(void);
extern bool IsNativeLakeLauncherProcess(void);
extern void LakeMaintenanceLauncherMain(void);
extern void LakeMaintenanceWorkerMain(const LakeWorkerStart* start);
extern bool LakeMaintenanceTakeRequest(LakeWorkerStart* start);
extern void LakeMaintenanceWorkerSpawned(uint64 generation,ThreadId pid);
extern void LakeMaintenanceWorkerSpawnFailed(uint64 generation);
extern void LakeMaintenanceWorkerExited(ThreadId pid);
extern char* LakeMaintenanceStateJson(void);
extern void LakeMaintenanceNoteCommit(Oid database_oid);
#endif
