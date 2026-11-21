/* Planner-only accounting. These images never authorize execution coverage. */
#ifndef OG_LAKE_COST_H
#define OG_LAKE_COST_H
#include "postgres.h"
struct LakeInlineBinding;

enum LakeCostAccess { LAKE_COST_FULL, LAKE_COST_SORTED, LAKE_COST_BITMAP,
    LAKE_COST_ANN_CONSERVATIVE, LAKE_COST_ANN_PQ };
struct LakeCostSource {
    int64 file_id;
    double physical_rows,bytes,groups,full_io,full_cpu;
    bool covered,ann_admissible,explicit_identity,unknown_graph;
    int format;
    uint32 graph_nodes,pq_chunks,training_rows;
    LakeCostAccess access;
    double prepare,run;
};
struct LakeCostLedger {
    double startup,inline_run,inline_io,seq_page,random_page,cpu_tuple,cpu_operator;
    double visible_rows,physical_rows;
    int files,inline_offset;
    LakeCostSource* source;
};
struct LakeCostResult {double startup,total;int covered,unknown_graphs;};
extern LakeCostLedger* LakeCostCreate(const LakeInlineBinding*,double startup,double total,
    double visibleRows,double physicalRows,double visibilityCost,double visibleCost,
    double targetCost,double inlinePages,double seqPage,double randomPage,
    double cpuTuple,double cpuOperator,bool allowAnn);
extern LakeCostLedger* LakeCostCopy(const LakeCostLedger*);
/* Duplicate metadata cannot increase covered population or its savings. */
extern bool LakeCostCover(LakeCostLedger*,int file,int format=1,uint32 nodes=0,
    uint32 chunks=0,uint32 trainingRows=0);
extern void LakeCostScalar(LakeCostLedger*,bool bitmap,double selectivity,double probes);
/* fileBeam is the actual file frontier, not SQL K or the inline probe estimate. */
extern void LakeCostAnn(LakeCostLedger*,uint32 fileBeam,int dimension,int scalarLeaves);
extern LakeCostResult LakeCostFinish(const LakeCostLedger*,double inlineIo=-1,int dop=1,
    double threadCost=0);
#endif
