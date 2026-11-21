#include "postgres.h"
#include "lake_cost.h"
#include "lake_inline_private.h"
#include "lake_inline_sources.h"
#include "utils/memutils.h"
#include <cmath>
#include <cfloat>

/* Finite saturation protects the planner's arithmetic, not a performance
 * preference. Unknown work must not turn into a zero/NaN winning path. */
static const double LAKE_COST_MAX=DBL_MAX/1024.0;
static double LakeCostValue(double x)
{return std::isfinite(x)?Min(LAKE_COST_MAX,Max(0.0,x)):LAKE_COST_MAX;}
static double LakeCostSum(double a,double b)
{return LakeCostValue(LakeCostValue(a)+LakeCostValue(b));}
static double LakeCostProduct(double a,double b)
{return LakeCostValue(LakeCostValue(a)*LakeCostValue(b));}
static double LakeCostPages(double bytes)
{return ceil(LakeCostValue(bytes)/BLCKSZ);}

LakeCostLedger* LakeCostCreate(const LakeInlineBinding* binding,double startup,double total,
    double visibleRows,double physicalRows,double visibilityCost,double visibleCost,
    double targetCost,double inlinePages,double seqPage,double randomPage,
    double cpuTuple,double cpuOperator,bool allowAnn)
{
    auto* ledger=(LakeCostLedger*)palloc0(sizeof(LakeCostLedger));
    ledger->startup=LakeCostValue(startup);ledger->visible_rows=LakeCostValue(visibleRows);
    ledger->physical_rows=LakeCostValue(physicalRows);ledger->seq_page=LakeCostValue(seqPage);
    ledger->random_page=LakeCostValue(randomPage);ledger->cpu_tuple=LakeCostValue(cpuTuple);
    ledger->cpu_operator=LakeCostValue(cpuOperator);ledger->files=binding->file_count;
    ledger->inline_offset=LakeInlineReadSourceCount(binding)>0?1:0;
    ledger->inline_io=LakeCostProduct(inlinePages,ledger->seq_page);
    ledger->source=(LakeCostSource*)palloc0(sizeof(LakeCostSource)*Max(1,ledger->files));
    double fileRows=0;for(int f=0;f<ledger->files;++f)
        fileRows=LakeCostSum(fileRows,(double)binding->files[f].record_count);
    double population=Max(1.0,Max(ledger->physical_rows,fileRows));
    double fullCpu=LakeCostSum(visibilityCost,LakeCostSum(visibleCost,targetCost));
    double sourceWork=0;
    for(int f=0;f<ledger->files;++f){const auto* file=&binding->files[f];auto* source=&ledger->source[f];
        source->file_id=file->file_id;source->physical_rows=LakeCostValue((double)file->record_count);
        source->bytes=LakeCostValue((double)file->file_bytes);source->groups=Max(1.0,(double)file->row_group_count);
        source->full_io=LakeCostProduct(LakeCostPages(source->bytes),ledger->seq_page);
        source->full_cpu=LakeCostProduct(fullCpu,source->physical_rows/population);
        source->access=LAKE_COST_FULL;source->run=LakeCostSum(source->full_io,source->full_cpu);
        source->explicit_identity=file->row_id_start_null||file->partial_max>=0;
        /* A delete object may contain only future marks. Keeping a full scan
         * here is conservative; only the reader can prove its exact visibility.
         * Private transaction changes also retain the complete file baseline. */
        source->ann_admissible=allowAnn&&file->inline_delete_count==0&&file->deletes==NULL&&
            (file->partial_max<0||file->partial_max<=binding->snapshot_id);
        sourceWork=LakeCostSum(sourceWork,source->run);
    }
    ledger->inline_run=LakeCostValue(total-ledger->startup-sourceWork);
    return ledger;
}
LakeCostLedger* LakeCostCopy(const LakeCostLedger* input)
{
    auto* result=(LakeCostLedger*)palloc(sizeof(LakeCostLedger));*result=*input;
    result->source=(LakeCostSource*)palloc(sizeof(LakeCostSource)*Max(1,input->files));
    for(int f=0;f<input->files;++f)result->source[f]=input->source[f];
    return result;
}
bool LakeCostCover(LakeCostLedger* ledger,int file,int format,uint32 nodes,uint32 chunks,uint32 train)
{
    if(!ledger||file<0||file>=ledger->files||ledger->source[file].covered)return false;
    auto* source=&ledger->source[file];source->covered=true;source->format=format;
    source->graph_nodes=nodes;source->pq_chunks=chunks;source->training_rows=train;return true;
}
void LakeCostScalar(LakeCostLedger* ledger,bool bitmap,double selectivity,double probes)
{
    double sel=std::isfinite(selectivity)?Max(0.0,Min(1.0,selectivity)):1.0;
    probes=Max(1.0,LakeCostValue(probes));
    for(int f=0;f<ledger->files;++f){auto* source=&ledger->source[f];if(!source->covered)continue;
        double matched=LakeCostProduct(source->physical_rows,sel);
        double fraction=Min(1.0,matched/source->groups);
        /* No uniform compressed-byte claim: this is an explicit group-count
         * approximation under unknown per-group byte distribution. */
        double candidate=LakeCostProduct(source->run,fraction),prepare=0;
        if(bitmap){
            double dictionary=Min(source->physical_rows,65536.0);
            double bytes=LakeCostSum(source->physical_rows*2.0,LakeCostSum(dictionary*256.0,4096.0));
            prepare=LakeCostSum(ledger->random_page,LakeCostProduct(LakeCostPages(bytes),ledger->seq_page));
            prepare=LakeCostSum(prepare,LakeCostProduct(source->physical_rows,
                ledger->cpu_tuple+3*ledger->cpu_operator));source->access=LAKE_COST_BITMAP;
        }else{
            /* Existing format uses4096-row scalar leaves and full lookup open.
             * Keep the existing structural estimates; no new tuning constant. */
            double lookup=1+LakeCostPages(ceil(source->physical_rows/4096.0)*128.0);
            double leaves=Min(source->physical_rows,Max(1.0,matched)+2*4096.0*probes);
            prepare=LakeCostProduct(lookup+LakeCostPages(leaves*24.0),ledger->random_page);
            source->access=LAKE_COST_SORTED;
        }
        source->prepare=LakeCostValue(prepare);source->run=LakeCostValue(candidate);
    }
}
void LakeCostAnn(LakeCostLedger* ledger,uint32 beam,int dimension,int leaves)
{
    double effEmit=Max((uint32)1,beam),dim=Max(1,dimension);
    double searchBeam=leaves>0?Min(1000.0,Max(256.0,effEmit*2.0)):Min(1000.0,Max(128.0,effEmit*2.0));
    for(int f=0;f<ledger->files;++f){auto* source=&ledger->source[f];
        if(!source->covered||!source->ann_admissible){source->covered=false;continue;}
        double nodes=source->graph_nodes>0?(double)source->graph_nodes:Max(1.0,source->physical_rows);
        double chunks=source->pq_chunks;
        bool hasPq=source->format==2&&source->graph_nodes>0&&chunks>0;
        source->unknown_graph=false;
        source->access=hasPq?LAKE_COST_ANN_PQ:LAKE_COST_ANN_CONSERVATIVE;

        double effRows=Min(source->physical_rows,effEmit);
        double rowFraction=effRows/Max(1.0,source->physical_rows);
        double groupFraction=1.0-pow(1.0-1.0/Max(1.0,source->groups),effRows);
        double ioShare=Min(1.0,0.15*groupFraction+0.85*rowFraction);

        double lookupIo=LakeCostProduct(hasPq?4.0:6.0,ledger->random_page);
        double hops=Min(nodes,64.0+sqrt(nodes)*(hasPq?3.0:4.0));
        double traverseCpu=hasPq
            ?LakeCostProduct(hops,ledger->cpu_tuple*0.25+chunks*ledger->cpu_operator*0.02)
            :LakeCostProduct(hops,ledger->cpu_tuple*0.5+dim*ledger->cpu_operator*0.015);
        /* Exact distance rerank uses tight C++/SIMD loops over contiguous floats,
         * not SQL-operator evaluation per coordinate (~0.015 op cost/dim). */
        double rerankCpu=LakeCostProduct(Min(nodes,searchBeam),
            ledger->cpu_tuple+(1.0+dim*0.015)*ledger->cpu_operator);

        source->prepare=LakeCostSum(lookupIo,LakeCostSum(traverseCpu,rerankCpu));
        source->run=LakeCostSum(LakeCostProduct(source->full_io,ioShare),
            LakeCostProduct(source->full_cpu,rowFraction));
        if(source->explicit_identity)
            source->prepare=LakeCostSum(source->prepare,LakeCostProduct(source->full_io,ioShare));

        if(leaves>0){
            double scalarShare=Min(1.0,(double)leaves*8.0/Max(8.0,dim*4.0+16.0));
            source->prepare=LakeCostSum(source->prepare,LakeCostSum(
                LakeCostProduct(source->full_io,scalarShare),
                LakeCostProduct(source->physical_rows*leaves,ledger->cpu_operator*0.25)));
        }
    }
}
LakeCostResult LakeCostFinish(const LakeCostLedger* ledger,double inlineIo,int dop,double threadCost)
{
    LakeCostResult result={ledger->startup,ledger->startup,0,0};double lane[4]={0,0,0,0},prepare[4]={0,0,0,0};
    int participants=dop==2||dop==4?dop:1;
    double inlineRun=ledger->inline_run;
    if(inlineIo>=0)inlineRun=LakeCostSum(Max(0.0,inlineRun-ledger->inline_io),inlineIo);
    lane[0]=inlineRun;
    for(int f=0;f<ledger->files;++f){const auto* source=&ledger->source[f];
        /* All candidate preparation precedes output within its worker. The
         * serial path therefore charges it fully in startup. Parallel work
         * is placed on the actual static lane, never all divided by DOP. */
        int at=(f+ledger->inline_offset)%participants;
        lane[at]=LakeCostSum(lane[at],LakeCostSum(source->prepare,source->run));
        prepare[at]=LakeCostSum(prepare[at],source->prepare);
        if(source->covered)++result.covered;if(source->unknown_graph)++result.unknown_graphs;
    }
    double work=0,initialization=0;for(int p=0;p<participants;++p){
        work=Max(work,lane[p]);initialization=Max(initialization,prepare[p]);}
    double threads=LakeCostProduct(threadCost,participants-1);
    /* Conservative first-output barrier: charge the slowest lane preparation
     * before LIMIT may prorate remaining work. Preparation already belongs to
     * lane total, so it is NOT added to total a second time. Shared consumer
     * startup and native thread startup are each charged exactly once. */
    result.startup=LakeCostSum(ledger->startup,LakeCostSum(initialization,threads));
    result.total=LakeCostSum(ledger->startup,LakeCostSum(work,threads));
    result.startup=Min(result.startup,result.total);return result;
}
