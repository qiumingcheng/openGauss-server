/* Export an owned, quiescent native staging index. Does not publish coverage. */
#ifndef OG_LAKE_ANN_EXPORT_H
#define OG_LAKE_ANN_EXPORT_H
#include "postgres.h"
#include "utils/rel.h"
#include "lake_ann_format.h"

struct LakeAnnExport;
/* Resolve a real staging heap TID to its original physical and logical lake
 * identity. Resolver must prove source bounds/version and account for every
 * indexable physical input; repeated frozen/duplicate postings are not new
 * business rows. No SDK operation is allowed under a native buffer lock. */
typedef void (*LakeAnnResolveAddress)(const ItemPointerData*,OgLakeAnnAddress*,void*);
extern LakeAnnExport* LakeAnnExportOpen(Relation index,int metric,const char* context);
extern const OgLakeAnnSpec* LakeAnnExportSpec(const LakeAnnExport*);
extern void LakeAnnExportWrite(LakeAnnExport*,LakeAnnResolveAddress,void*,OgLakeAnnWriter*);
extern void LakeAnnExportClose(LakeAnnExport*);
#endif
