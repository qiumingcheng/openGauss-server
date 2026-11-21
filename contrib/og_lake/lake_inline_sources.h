/* Native internal descriptors only; no SDK or persisted locator ABI change. */
#ifndef OG_LAKE_INLINE_SOURCES_H
#define OG_LAKE_INLINE_SOURCES_H
#define OG_LAKE_PARTITION_EVOLUTION_SQL_VERSION "0.34"
#include "lake_inline_private.h"

/* A hard descriptor budget, never a truncated visibility domain. */
static const int LAKE_INLINE_SOURCE_LIMIT = 1024;

extern bool LakePartitionEvolutionVersionEnabled(void);
extern void LakeReserveColumnMapBytes(LakeInlineBinding* binding,Size bytes);
extern bool LakeColumnCanBeMissing(const LakeInlineBinding* binding,int column,int64 birthUpper);
extern LakeInlineColumn* LakeCopyInlineColumns(const LakeInlineColumn* columns,int count,
    LakeInlineBinding* allocationBudget = NULL);
extern bool LakeInlineHasHistoricalSources(const LakeInlineBinding* binding);
extern int LakeInlineReadSourceCount(const LakeInlineBinding* binding);
extern Oid LakeInlineReadSourceOid(const LakeInlineBinding* binding,int index);
extern void LakeValidateInlineSources(const LakeInlineBinding* binding);
extern const LakeInlineSource* LakeFindInlineSource(const LakeInlineBinding* binding,Oid sourceOid);
extern const LakeInlineSource* LakeInlineWriteSource(const LakeInlineBinding* binding);
extern void LakeCopyInlineSources(LakeInlineBinding* target,const LakeInlineBinding* source,MemoryContext owner);
/* Mirror current writer into retained single-source fields only for adapted
 * callers. An absent current carrier must not alias an older readable source. */
extern void LakeSetInlineWriteSource(LakeInlineBinding* binding,int index);
#endif
