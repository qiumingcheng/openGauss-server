/* Host scheduling state only; never a DuckLake standard catalog extension. */
#ifndef OG_LAKE_AUTOANALYZE_H
#define OG_LAKE_AUTOANALYZE_H
#include "postgres.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"
struct LakeInlineBinding;
extern bool LakeAutoAnalyzeCatalogAvailable(void);
extern void LakeCheckAutoAnalyzeControl(Relation relation, Oid owner);
extern void LakeAutoAnalyzeNotePublished(const LakeInlineBinding* binding, int64 changedRows, Snapshot snapshot);
/* These record sampling, not completion. Only the worker may publish coverage
 * after the entire native ANALYZE command has returned without ERROR. */
extern void LakeAutoAnalyzeSampleStart(Relation relation, const LakeInlineBinding* binding, Snapshot snapshot);
extern void LakeAutoAnalyzeSampleReturned(Oid relationOid);
extern void LakeAutoAnalyzeForgetRelation(Oid,Snapshot);
extern void LakeAutoAnalyzeForgetServer(Oid,Snapshot);
/* Same dedicated worker; caller supplies its existing abort/error recovery. */
extern bool LakeAutoAnalyzeOne(void (*recover)(bool));
/* Private native diagnostics; no installed extension SQL entry. */
extern void LakeAutoAnalyzeTestGate(Oid,int64,int64);
extern void LakeAutoAnalyzeTestError(Oid,int);
#endif
