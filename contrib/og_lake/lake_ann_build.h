/* Native DiskANN construction on a private heap, then portable export. */
#ifndef OG_LAKE_ANN_BUILD_H
#define OG_LAKE_ANN_BUILD_H
#include "lake_inline_private.h"
#include "lake_ann_format.h"
struct LakeFileCandidate;
/* Does not publish catalog state. The caller owns immutable directory names,
 * candidate commit/abort and conditional READY/coverage publication. */
extern void LakeBuildAnnFileSegment(LakeInlineBinding*,int fileIndex,int column,
    int metric,const char* objectVersion,const char* context,const char* directory,
    LakeFileCandidate** nodes,LakeFileCandidate** lookup,OgLakeAnnWritten*);
#endif
