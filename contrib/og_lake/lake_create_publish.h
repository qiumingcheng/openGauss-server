/* Host adaptation of DuckLake 1.1 publication, never a durable protocol extension. */
#ifndef OG_LAKE_CREATE_PUBLISH_H
#define OG_LAKE_CREATE_PUBLISH_H
#include "lake_inline_private.h"
#include "lib/stringinfo.h"

/* A read-only projection of the transaction-local CREATE journal. In particular,
 * binding->identity is not read as a permanent ID: the table is not assigned one.
 * Callers exclude dropped/aborted objects before building the attempt array. */
struct LakeCreateDefinition {
    const LakeInlineBinding* binding;
    int64 schema_id;
    const char* schema_uuid;
    const char* schema_name; /* one already committed top-level schema */
    const char* table_name;
    const char* relative_path; /* generated once from the stable table UUID/name */
    const char* inline_name; /* NULL iff the trusted CREATE deliberately made no carrier */
};
struct LakeCreateAttempt {
    const LakeCreateDefinition* definition;
    int64 permanent_table_id;
    int64 commit_snapshot;
    int64 commit_schema_version;
};

enum LakePublicationChangeKind {
    LAKE_CHANGE_CREATED_TABLE,
    LAKE_CHANGE_INLINED_INSERT,
    LAKE_CHANGE_INLINED_DELETE,
    LAKE_CHANGE_FILE_INSERT,
    LAKE_CHANGE_REWRITE_FILE,
    LAKE_CHANGE_MERGE_FILE,
    LAKE_CHANGE_FILE_DELETE,
    LAKE_CHANGE_ALTERED_TABLE,
    LAKE_CHANGE_INLINE_FLUSH /* certificate parser only; old commit grammar stays closed */
};
struct LakePublicationChange {
    LakePublicationChangeKind kind;
    int64 table_id; /* only for numeric change types */
    char* schema_name; /* palloc, only for CREATED_TABLE */
    char* table_name;
};

/* ASCII case folding only, matching pinned DuckDB CIEquals; never locale lower(). */
extern bool LakeIdentifierEquals(const char* left, const char* right);
/* Closed standard table/schema/view/schema_versions shape checks. */
extern char* LakeDefinitionRelation(Oid ns, const char* name);
extern char* LakeCreateRelativePath(const char* tableUuid, const char* tableName);
extern void LakeAppendCreatedTableChange(StringInfo result, const char* schema, const char* table);
/* Known malformed input -> DATA_CORRUPTED; unsupported legal change kind or
 * nested schema -> SERIALIZATION_FAILURE, never ignored or split at quoted commas. */
extern bool LakeNextPublicationChange(const char** cursor, LakePublicationChange* change);
/* Scoped certificate grammar retains the target of inline_flush/flushed_inlined. */
extern bool LakeNextCertificateChange(const char** cursor, LakePublicationChange* change);
/* Pure attempt-local mapping. Allocates no durable ID, mutates no journal. */
extern LakeCreateAttempt* LakePlanCreateAttempt(const LakeCreateDefinition* const* definitions,
    int count, int64 headSnapshot, int64 headSchemaVersion, int64 nextCatalogId,
    int64* finalNextCatalogId, int64* finalSchemaVersion);
/* Before claim: fresh publication snapshot, after options/configuration locks.
 * Checks current parent identity and conflicts with live tables/views. */
extern void LakeValidateCreateDefinition(const LakeCreateDefinition* definition,
    Snapshot publicationSnapshot, int64 headSnapshot);
/* After successful snapshot claim only, inside the same internal subtransaction.
 * Writes exactly table/column/optional inline-registration/schema_versions.
 * Does NOT create physical tables, write host bindings/stats/rows/changes, or commit. */
extern void LakeWriteCreateDefinition(const LakeCreateAttempt* attempt, Snapshot publicationSnapshot);
/* After ALL definition/host-binding writes and before readonly own-row reads:
 * push a copied attempt snapshot and advance only its CID. The caller MUST keep
 * this base active across the subsequent SPI calls, then PopActiveSnapshot()
 * before ReleaseCurrentSubTransaction. Subabort cleans it on ERROR. Never
 * mutate the registered business snapshot. A bare unregistered copied snapshot
 * is unsafe: the first readonly SPI PopActiveSnapshot would free it. */
extern Snapshot LakeCreatePushOwnWriteSnapshot(Snapshot publicationSnapshot);
#endif
