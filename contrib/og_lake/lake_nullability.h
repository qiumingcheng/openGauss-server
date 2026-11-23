/* Logical statement constraints are independent of retained physical schemas. */
#ifndef OG_LAKE_NULLABILITY_H
#define OG_LAKE_NULLABILITY_H
#include "lake_inline_private.h"

#define OG_LAKE_NOT_NULL_SQL_VERSION "0.40"
/* BYTEA remains nullable-only under retained SQL43. */
#define OG_LAKE_BINARY_NOT_NULL_SQL_VERSION "0.44"
/* Compact immutable per-query facts; no default/SPI/catalog pointers. */
struct LakeConstraintFieldProof {
    int64 column_id;AttrNumber attnum;Oid type;int32 typmod;Oid collation;
    NameData name;bool not_null;bool dropped;
};
struct LakeConstraintRelationProof {
    int rti;Oid relation_oid,server_oid;int columns;
    LakeConstraintFieldProof* fields;LakeConstraintRelationProof* next;
};
struct LakeConstraintProof {
    int rtable_length,relations;Size bytes;LakeConstraintRelationProof* head;
};
extern LakeConstraintProof* LakeCaptureNotNullPlanProof(PlannedStmt*,Snapshot,MemoryContext);
extern void LakeVerifyNotNullPlanProof(const LakeConstraintProof*,PlannedStmt*);
extern bool LakeNotNullVersionEnabled(void);
extern bool LakeBinaryNotNullVersionEnabled(void);
extern bool LakeNotNullType(Oid type);
extern void LakeReadFieldNullability(LakeInlineBinding*, LakeInlineColumn*, int,
    int64 schemaSnapshot, Snapshot hostSnapshot);
extern void LakeCheckNotNullSlot(const LakeInlineBinding*, TupleTableSlot*);
extern LakeInlineBinding* LakeLoadConstraintDefinition(Relation, Snapshot);
extern void LakeCheckNotNullQuerySnapshot(Query*, Snapshot);
extern void LakeCheckNotNullPlanSnapshot(PlannedStmt*, Snapshot);
extern void* LakeNativePrepareNullability(Relation, List*);
extern void LakeNativeFinishNullability(void*);
#endif
