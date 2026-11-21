/* File ANN candidates, never final SQL rows or cached public distances. */
#ifndef OG_LAKE_FILE_ANN_H
#define OG_LAKE_FILE_ANN_H
#include "postgres.h"
#include "lake_ann_format.h"
#include "access/skey.h"
#include "nodes/bitmapset.h"

/* A fixed native scalar comparison, evaluated over the original source
 * column. The mask is physical file positions, never logical row IDs. */
struct LakeAnnScalarQual {AttrNumber column;ScanKeyData key;};
enum LakeAnnScalarKind {LAKE_ANN_SCALAR_LEAF=1,LAKE_ANN_SCALAR_AND=2,
    LAKE_ANN_SCALAR_OR=3,LAKE_ANN_SCALAR_NOT=4};
#define LAKE_ANN_SCALAR_MAX_LEAVES 16
#define LAKE_ANN_SCALAR_MAX_NODES 64
#define LAKE_ANN_SCALAR_MAX_DEPTH 16
#define LAKE_ANN_SCALAR_MAX_ARRAY 65536
/* Runtime-only tree; plans serialize validated metadata and native RHS Exprs.
 * Array leaves use SK_SEARCHARRAY and an equality ScanKey with element subtype.
 * All leaves bind the same source position domain and parameter epoch. */
struct LakeAnnScalarExpr {
    int kind;
    int child_count;
    LakeAnnScalarExpr** children;
    LakeAnnScalarQual leaf;
};
struct LakeAnnFileMask {Bitmapset* positions;uint32 count;};

struct LakeFileAnnCandidate {
    OgLakeAnnAddress address;
    uint32 node_id;
    float internal_distance;
};
struct LakeFileAnnCandidates {
    uint32 count;
    uint32 visited_candidates;
    bool budget_exhausted;
    bool pq_used;
    LakeFileAnnCandidate* rows;
};
/* Borrow a verified same-version graph reader. Caller owns reader close and
 * original-column fetching, source-version/row/birth/delete checks, dedup,
 * scalar predicates, uncovered-source scan and global SQL distance rerank.
 * beam controls the existing DiskANN frontier, not a row visibility limit.
 * The result is allocated in the caller's current native memory context. */
struct LakePqInput;
struct LakeAnnRescanEntry;
extern LakeFileAnnCandidates* LakeSearchAnnFile(OgLakeAnnReader*,Datum query,uint32 beam,
    const LakeAnnFileMask* mask = NULL,const LakePqInput* pq = NULL,LakeAnnRescanEntry* cached = NULL);
#endif
