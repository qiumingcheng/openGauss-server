/* Transaction-local RENAME journal; no new durable metadata format. */
#ifndef OG_LAKE_RENAME_H
#define OG_LAKE_RENAME_H
#include "lake_txn_private.h"

struct LakeRenameEvent {
    MemoryContext context;
    LakeInlineBinding* binding;
    SubTransactionId owner_subid;
    Oid host_namespace;
    int64 source_begin;
    bool column_rename;
    int64 column_id;
    int64 column_source_begin;
    AttrNumber column_attnum;
    char* old_column_name;
    char* new_column_name;
    int64 schema_id;
    char* schema_name;
    char* schema_uuid;
    char* old_name;
    char* new_name;
    char* path;
    bool path_relative;
    bool path_relative_null;
    Size charged_bytes;
    LakeRenameEvent* next;
};
extern LakeTxnState* LakeTxnBeginRename(Oid serverOid);
extern void LakeTxnForgetReadOnlyTableForRename(Oid relationOid);
extern void LakeTxnAdoptRename(LakeRenameEvent* event);
extern LakeRenameEvent** LakeCollectRenames(LakeTxnState* state, int* count);
extern void LakeValidateRenames(LakeRenameEvent** events, int count, Snapshot snapshot, int64 head);
extern void LakeWriteRenames(LakeRenameEvent** events, int count, Snapshot snapshot, int64 commit, int64 schema);
extern void LakePublishRenames(LakeTxnState* state);
#endif
