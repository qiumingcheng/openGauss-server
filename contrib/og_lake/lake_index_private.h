/* Host-owned derived indexes; never part of the DuckLake standard catalog. */
#ifndef OG_LAKE_INDEX_PRIVATE_H
#define OG_LAKE_INDEX_PRIVATE_H
#include "lake_inline_private.h"

/* Metadata state is independent of backend availability. An ACTIVE definition
 * with no verified READY coverage must retain complete ordinary scanning. */
enum LakeIndexDefinitionState { LAKE_INDEX_ACTIVE, LAKE_INDEX_DROPPING };
enum LakeIndexSegmentState { LAKE_SEGMENT_BUILDING, LAKE_SEGMENT_READY, LAKE_SEGMENT_RETIRED, LAKE_SEGMENT_FAILED };

struct LakeIndexDefinition {
    char* index_uuid;
    char* binding_id; /* NULL only for retained, detached DROPPING metadata. */
    char* index_name;
    Oid server_oid;
    int64 table_id;
    char* table_uuid;
    int64 column_id;
    Oid type_oid;
    Oid collation_oid;
    int64 generation;
    char* definition_hash;
    char* normalized_definition;
    int64 registered_snapshot;
    LakeIndexDefinitionState state;
};

/* Trusted catalog validators are called before privileged SPI planning. */
extern bool LakeHasIndexCatalogVersion(void);
extern bool LakeHasInlineIndexVersion(void);
extern bool LakeHasIndexRootVersion(void);
extern void LakeSetCatalogIndexRoot(const char* serverName,const char* input);
extern Oid LakeIndexCatalog(void);
extern void LakeIndexDdl(void);
extern void LakeIndexPermission(const char* uuid, Snapshot snapshot);
extern Relation LakeOpenTrustedInlineIndex(Oid indexOid, Oid owner);
extern Relation LakeOpenInlineIndex(Oid indexOid, const LakeInlineBinding* binding, AttrNumber column);
struct PlannerInfo;
struct RelOptInfo;
extern void LakeAddInlineIndexPaths(PlannerInfo* root, RelOptInfo* relation, Oid relationOid,
    double startup, double total, const struct LakeCostLedger* ledger=NULL);
extern bool LakeInlineIndexAvailable(const LakeInlineBinding* binding, const char* uuid,
    Oid indexOid, AttrNumber column, Snapshot businessSnapshot);
extern "C" Datum og_lake_build_lake_index(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_rebuild_lake_index(PG_FUNCTION_ARGS);
extern void LakeCheckIndexControlShape(Relation relation, int kind);
extern void LakeValidateIndexControlConstraints(Relation relation, Oid owner);
extern bool LakeIndexValidPartialPredicate(Oid indexOid, Node* predicate);
extern void LakeIndexAdoptCatalogForUpgrade(void);
extern void LakeIndexRetireRelation(Oid relationOid, Snapshot snapshot);
extern void LakeIndexRetireFile(const LakeInlineBinding*,int64 fileId,Snapshot snapshot);

extern "C" Datum og_lake_create_lake_index(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_drop_lake_index(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_lake_index_status(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_index_catalog_upgrade(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_index_catalog_validate(PG_FUNCTION_ARGS);
#endif
