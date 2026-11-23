/* Transaction-local logical DROP; no new durable DuckLake metadata format. */
#ifndef OG_LAKE_DROP_H
#define OG_LAKE_DROP_H
#include "lake_txn_private.h"

struct LakeDropEvent {
    MemoryContext context;
    LakeInlineBinding* binding; /* Header/identity only: never loads business files. */
    SubTransactionId owner_subid;
    char* catalog_uuid;
    int64 catalog_generation;
    int64 source_begin;
    int64 schema_id;
    Size charged_bytes;
    LakeDropEvent* next;
};
/* Installed SQL0.36 only; older retained SQL keeps host-only detach. */
extern bool LakeLogicalDropVersionEnabled(void);
extern LakeTxnState* LakeTxnBeginDrop(Oid serverOid);
extern void LakeTxnAdoptDrop(LakeDropEvent* event);
/* Only the kernel DROP hook, before deleting pg_foreign_table/host bindings. */
extern void LakeStageCommittedDrop(Oid relationOid, Oid serverOid);
/* After ATTACH owns the unique (server_oid, table_id) host-binding key. */
extern void LakeValidateAttachedSourceAfterBinding(LakeInlineBinding* before);
extern LakeDropEvent** LakeCollectDrops(LakeTxnState* state, int* count);
extern void LakeValidateDrops(LakeDropEvent** events, int count, Snapshot snapshot, int64 head);
extern void LakeWriteDrops(LakeDropEvent** events, int count, Snapshot snapshot, int64 commit);
extern void LakePublishDrops(LakeTxnState* state);
#endif
