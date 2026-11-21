#ifndef OG_LAKE_COLUMN_LAYOUT_H
#define OG_LAKE_COLUMN_LAYOUT_H
#include "lake_inline_private.h"
extern bool LakeDropColumnVersionEnabled(void);
extern int LakeHostColumnCount(const LakeInlineBinding*);
extern int LakeHostToLogical(const LakeInlineBinding*,AttrNumber);
extern int LakeLogicalToHost(const LakeInlineBinding*,int);
extern const LakeRetiredColumn* LakeFindRetiredColumn(const LakeInlineBinding*,int64);
extern void LakeGatherHostTuple(const LakeInlineBinding*,TupleDesc,HeapTuple,Datum*,bool*);
extern void LakeScatterHostValues(const LakeInlineBinding*,TupleDesc,const Datum*,const bool*,Datum*,bool*);
extern HeapTuple LakeFormHostTuple(const LakeInlineBinding*,TupleDesc,const Datum*,const bool*);
extern void LakeCopyRetiredColumns(LakeInlineBinding*,const LakeInlineBinding*);
extern bool LakeColumnLayoutsEqual(const LakeInlineBinding*,const LakeInlineBinding*);
extern void LakeValidateRetiredBirth(const LakeInlineBinding*,int64 fieldId,int64 lower,int64 upper);
extern void* LakeNativePrepareDropColumn(Relation,List*);
extern void LakeNativeFinishDropColumn(void*);
#endif
