/* Native vector value conversion only. No catalog admission or ANN path. */
#ifndef OG_LAKE_VECTOR_CODEC_H
#define OG_LAKE_VECTOR_CODEC_H
#include "postgres.h"
struct ArrowArray;
struct ArrowSchema;

/* The caller supplies the bound business vector(n) dimension and owns the
 * current tuple memory context. Returned Datums own their bytes in that context.
 * SQL NULL is handled by the caller; these functions accept non-NULL values. */
extern void LakeVectorDimension(int32 dimension);
extern Datum LakeVectorReadText(Datum value,int32 dimension);
extern Datum LakeVectorWriteText(Datum value,int32 dimension);
extern Datum LakeVectorReadArrow(const ArrowArray*,const ArrowSchema*,int64 row,int32 dimension);
#endif
