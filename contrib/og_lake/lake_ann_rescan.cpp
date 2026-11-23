#include "postgres.h"
#include "knl/knl_variable.h"
#include "lake_version.h"
#include "lake_ann_rescan.h"
#include "lake_file_pq.h"
#include "lake_file_sdk.h"
#include "lake_pq_test.h"
#include "miscadmin.h"
#include "utils/resowner.h"
#include <cstdlib>
#include <cstring>
#include <memory>
#include <new>
#include <openssl/sha.h>
#ifndef USE_LAKE_PQ_CPU
LakeAnnRescanCache* LakeAnnRescanCreate(MemoryContext){return NULL;}
void LakeAnnRescanClose(LakeAnnRescanCache** p){if(p)*p=NULL;}
LakeAnnRescanStats LakeAnnRescanStatsRead(const LakeAnnRescanCache*){return LakeAnnRescanStats{};}
LakeAnnRescanEntry* LakeAnnRescanFind(LakeAnnRescanCache*,const LakeAnnRescanKey*,bool){return NULL;}
LakeAnnRescanEntry* LakeAnnRescanLoad(LakeAnnRescanCache*,const LakeAnnRescanKey*,OgLakeAnnReader*,const LakePqInput*){return NULL;}
void LakeAnnRescanInvalidate(LakeAnnRescanCache*,LakeAnnRescanEntry*,const LakeAnnRescanKey*){}
bool LakeAnnRescanIsPoisoned(const LakeAnnRescanCache*,const LakeAnnRescanKey*){return false;}
const OgLakeAnnSpec* LakeAnnRescanSpec(const LakeAnnRescanEntry*){return NULL;}
const OgLakeAnnNode* LakeAnnRescanNode(const LakeAnnRescanEntry*,uint32){return NULL;}
const uint32* LakeAnnRescanEntries(const LakeAnnRescanEntry*,uint32* n){*n=0;return NULL;}
bool LakeAnnRescanQuery(LakeAnnRescanEntry*,const float*){return false;}
bool LakeAnnRescanPrecomputeDistances(LakeAnnRescanEntry*,uint32,float*){return false;}
bool LakeAnnRescanDistance(LakeAnnRescanEntry*,uint32,float*){return false;}
void LakeAnnRescanEndQuery(LakeAnnRescanEntry*){}
bool LakeAnnSharedPoolEnabled(void){return false;}
LakeAnnRescanEntry* LakeAnnSharedAcquire(LakeAnnRescanCache*,const LakeAnnRescanKey*,bool,MemoryContext){return NULL;}
LakeAnnRescanEntry* LakeAnnSharedLoad(LakeAnnRescanCache*,const LakeAnnRescanKey*,OgLakeAnnReader*,const LakePqInput*,MemoryContext){return NULL;}
void LakeAnnSharedInvalidate(const LakeAnnRescanKey*){}
#else
#include "pq_parquet_frontier_draft.h"
#include "utils/memutils.h"
#include <pthread.h>
#include <sys/stat.h>
#include <vector>
/* A budget counter only: no cache entry, handle or query crosses a statement.
 * Disabled under the thread pool and Stream, so cleanup runs on this thread. */
static THR_LOCAL uint64 LakeAnnRetainedBytes=0;
static const uint64 LakeAnnCacheLimit=32ULL*1024*1024;
static const uint64 LakeAnnSharedPoolLimit=512ULL*1024*1024;
static const int LakeAnnCacheSlots=8;
static OgLakeSdkLimits LakeAnnPqLimits(uint32 dim,uint32 nodes,uint32 chunks)
{
    OgLakeSdkLimits limits=LakeFileLimits();
    uint64 est=UINT64CONST(64)*1024*1024+(uint64)nodes*Max(chunks,1U)*4ULL+(uint64)dim*65536ULL;
    if(est>(uint64)limits.memory_bytes)limits.memory_bytes=(int64)Min(est,UINT64CONST(512)*1024*1024);
    return limits;
}
struct LakeAnnFileStamp {
    bool expected;
    uint64 dev,ino,size;
    int64 mtime_sec,mtime_nsec;
    int64 ctime_sec,ctime_nsec;
};
struct LakeAnnSharedSegment {
    unsigned char key[SHA256_DIGEST_LENGTH];
    uint64 stamp,bytes,refcount;
    bool invalidated,has_pq;
    char directory[MAXPGPATH];
    LakeAnnFileStamp stamps[5];
    OgLakeAnnSpec spec;
    char graph_context[65];
    void* graph;
    uint32* shared_edges;
    OgLakeAnnNode* nodes;
    uint32* entries;
    OgLakeSdkLimits limits;
    PqParquetReader* pq;
};
struct LakeAnnRescanEntry {
    LakeAnnRescanCache* cache;
    LakeAnnSharedSegment* shared;
    bool ready;
    unsigned char key[SHA256_DIGEST_LENGTH];uint64 stamp,bytes;
    OgLakeAnnSpec spec;char graph_context[65];
    void* graph;OgLakeAnnNode* nodes;uint32* entries;
    OgLakeSdkLimits limits;PqParquetReader* pq;PqParquetQuery* query;
};
struct LakeAnnRescanCache {
    ResourceOwner owner,diagnostic_parent;uint32 references;bool remembered,closed,observed;
    uint64 clock;LakeAnnRescanStats stats,reported;LakeAnnRescanEntry* entries[LakeAnnCacheSlots];
    unsigned char bad_keys[LakeAnnCacheSlots][SHA256_DIGEST_LENGTH];
    int bad_count;
};
static void Charge(LakeAnnRescanCache* c,uint64 n)
{
    Assert(n<=LakeAnnCacheLimit-LakeAnnRetainedBytes);LakeAnnRetainedBytes+=n;c->stats.bytes+=n;
    c->stats.peak=Max(c->stats.peak,c->stats.bytes);if(c->observed)LakeTestAnnCacheEvent(LAKE_ANN_CACHE_ALLOC,n);
}
static void Uncharge(LakeAnnRescanCache* c,uint64 n)
{
    Assert(n<=c->stats.bytes&&n<=LakeAnnRetainedBytes);c->stats.bytes-=n;LakeAnnRetainedBytes-=n;
    if(c->observed)LakeTestAnnCacheEvent(LAKE_ANN_CACHE_FREE,n);
}
void LakeAnnRescanEndQuery(LakeAnnRescanEntry* e)
{if(e){pq_parquet_query_close(&e->query);}}
static void SharedSegmentDestroy(LakeAnnSharedSegment* s);
static void SharedSegmentUnref(LakeAnnSharedSegment* s);
static void Drop(LakeAnnRescanEntry** p)
{
    auto* e=*p;if(!e)return;*p=NULL;LakeAnnRescanEndQuery(e);
    if(e->shared){
        LakeAnnSharedSegment* s=e->shared;e->shared=NULL;
        SharedSegmentUnref(s);
        std::free(e);
        return;
    }
    pq_parquet_reader_close(&e->pq);std::free(e->graph);
    if(e->bytes)Uncharge(e->cache,e->bytes);std::free(e);
}
static void Release(LakeAnnRescanCache* c)
{
    if(c->closed)return;c->closed=true;
    for(int i=0;i<LakeAnnCacheSlots;++i)Drop(&c->entries[i]);
    if(c->observed)LakeTestAnnCacheEvent(LAKE_ANN_CACHE_CLOSE,0);
}
static void Unref(LakeAnnRescanCache* c)
{if(--c->references==0){Uncharge(c,sizeof(*c));std::free(c);}}
static void OwnerRelease(void* p)
{auto* c=(LakeAnnRescanCache*)p;c->remembered=false;c->owner=NULL;c->diagnostic_parent=NULL;Release(c);Unref(c);}
static void Reparent(void* p,ResourceOwner o)
{auto* c=(LakeAnnRescanCache*)p;c->owner=o;c->diagnostic_parent=NULL;if(c->observed)LakeTestAnnCacheEvent(LAKE_ANN_CACHE_REPARENT,0);}
/* Only diagnostic children belong to us.  Their storage follows the real
 * parent owner, never the query context whose callback still needs this owner.
 * Explicit/context-first cleanup may delete an empty child. Owner-first cleanup
 * leaves deletion to the in-progress host owner-tree walk. */
static void ForgetOwner(LakeAnnRescanCache* c)
{
    ResourceOwner owner=c->owner;bool privateChild=c->diagnostic_parent!=NULL;
    ResourceOwnerForgetLakePq(owner,c);c->remembered=false;c->owner=NULL;c->diagnostic_parent=NULL;
    if(privateChild)ResourceOwnerDelete(owner);
    Unref(c);
}
static void ContextRelease(std::shared_ptr<void> p)
{
    auto* c=(LakeAnnRescanCache*)p.get();
    if(c->remembered)ForgetOwner(c);
    Release(c);Unref(c);
}
LakeAnnRescanCache* LakeAnnRescanCreate(MemoryContext context)
{
    if(StreamThreadAmI()||g_instance.attr.attr_common.enable_thread_pool||LakeTestPqDiagnosticArmed())return NULL;
    ResourceOwner owner=t_thrd.utils_cxt.CurrentResourceOwner;
    if(!owner||sizeof(LakeAnnRescanCache)>LakeAnnCacheLimit-LakeAnnRetainedBytes)return NULL;
    ResourceOwner diagnosticParent=NULL;
    if(LakeTestAnnCacheDiagnosticPending()){
        diagnosticParent=owner;owner=ResourceOwnerCreate(owner,"Lake ANN cache diagnostic child",ResourceOwnerGetMemCxt(diagnosticParent));
    }
    ResourceOwnerEnlargeLakePq(owner);
    auto* callback=(MemoryContextCallback*)MemoryContextAlloc(context,sizeof(MemoryContextCallback));new(callback)MemoryContextCallback();
    auto* c=(LakeAnnRescanCache*)std::calloc(1,sizeof(LakeAnnRescanCache));
    if(!c){if(diagnosticParent)ResourceOwnerDelete(owner);return NULL;}
    c->owner=owner;c->diagnostic_parent=diagnosticParent;c->references=2;c->remembered=true;c->observed=LakeTestAnnCacheObserved();
    if(c->observed)LakeTestAnnCacheEvent(LAKE_ANN_CACHE_OPEN,0);Charge(c,sizeof(*c));
    ResourceOwnerRememberLakePq(owner,c,OwnerRelease,Reparent);
    callback->func=ContextRelease;callback->arg=std::shared_ptr<void>(std::shared_ptr<void>(),c);
    MemoryContextRegisterResetCallback(context,callback);return c;
}
void LakeAnnRescanClose(LakeAnnRescanCache** p)
{
    if(!p||!*p)return;auto* c=*p;*p=NULL;
    if(c->stats.hits>c->reported.hits||c->stats.misses>c->reported.misses||
       c->stats.loads>c->reported.loads||c->stats.evictions>c->reported.evictions||
       c->stats.invalidations>c->reported.invalidations||c->stats.queries>c->reported.queries||
       c->stats.peak>c->reported.peak){
        LakeHostRecordAnnRescan(c->stats.hits-c->reported.hits,c->stats.misses-c->reported.misses,
            c->stats.loads-c->reported.loads,c->stats.evictions-c->reported.evictions,
            c->stats.invalidations-c->reported.invalidations,c->stats.queries-c->reported.queries,c->stats.peak);
        c->reported=c->stats;
    }
    Release(c);
    if(c->remembered)ForgetOwner(c);
}
static bool Key(const LakeAnnRescanKey* k,unsigned char out[SHA256_DIGEST_LENGTH]);
static bool LakeAnnPoisonContains(const LakeAnnRescanCache* c,const unsigned char hash[SHA256_DIGEST_LENGTH])
{
    if(!c)return false;
    for(int i=0;i<c->bad_count;++i)if(!memcmp(c->bad_keys[i],hash,SHA256_DIGEST_LENGTH))return true;
    return false;
}
bool LakeAnnRescanIsPoisoned(const LakeAnnRescanCache* c,const LakeAnnRescanKey* key)
{
    if(!c||c->closed||!key||c->bad_count==0)return false;
    unsigned char hash[SHA256_DIGEST_LENGTH];
    return Key(key,hash)&&LakeAnnPoisonContains(c,hash);
}
static void LakeAnnPoisonHash(LakeAnnRescanCache* c,const unsigned char hash[SHA256_DIGEST_LENGTH])
{
    if(!c||LakeAnnPoisonContains(c,hash)||c->bad_count>=LakeAnnCacheSlots)return;
    memcpy(c->bad_keys[c->bad_count++],hash,SHA256_DIGEST_LENGTH);
}
void LakeAnnRescanInvalidate(LakeAnnRescanCache* c,LakeAnnRescanEntry* entry,const LakeAnnRescanKey* key)
{
    if(key&&LakeAnnSharedPoolEnabled())LakeAnnSharedInvalidate(key);
    if(!c||c->closed)return;
    bool removed=false;
    bool hasEntryHash=false;
    unsigned char entryHash[SHA256_DIGEST_LENGTH];
    if(entry!=NULL){
        memcpy(entryHash,entry->key,sizeof(entryHash));
        hasEntryHash=true;
    }
    for(int i=0;i<LakeAnnCacheSlots;++i){
        if(c->entries[i]&&entry&&c->entries[i]==entry){
            Drop(&c->entries[i]);++c->stats.evictions;removed=true;
        }
    }
    unsigned char hash[SHA256_DIGEST_LENGTH];
    if(key&&Key(key,hash)){
        for(int i=0;i<LakeAnnCacheSlots;++i){
            if(c->entries[i]&&!memcmp(c->entries[i]->key,hash,sizeof(hash))){
                Drop(&c->entries[i]);++c->stats.evictions;removed=true;
            }
        }
        LakeAnnPoisonHash(c,hash);
    }else if(hasEntryHash){
        LakeAnnPoisonHash(c,entryHash);
    }
    ++c->stats.invalidations;
    if(removed&&c->observed)LakeTestAnnCacheEvent(LAKE_ANN_CACHE_EVICT,0);
}
LakeAnnRescanStats LakeAnnRescanStatsRead(const LakeAnnRescanCache* c)
{return c?c->stats:LakeAnnRescanStats{};}
static bool Key(const LakeAnnRescanKey* k,unsigned char out[SHA256_DIGEST_LENGTH])
{
    if(!k)return false;const char* texts[]={k->index_uuid,k->object_version,k->directory,k->source_hash,k->schema_hash,k->context_hash,k->manifest_hash};
    const Size maximum[]={36,256,MAXPGPATH,64,64,64,240};SHA256_CTX s;SHA256_Init(&s);
    const char domain[]="og_lake_statement_ann_image_v1";SHA256_Update(&s,domain,sizeof(domain));
    uint64 values[]={(uint64)k->file_id,(uint64)k->column_id,(uint64)k->dimension,(uint64)k->metric,
        OG_LAKE_ANN_FORMAT_VERSION,PQ_CPU_ALGORITHM_VERSION,PQ_PARQUET_VERSION};
    for(uint64 v:values){unsigned char b[8];for(int i=7;i>=0;--i){b[i]=v&255;v>>=8;}SHA256_Update(&s,b,8);}
    for(int i=0;i<7;++i){if(!texts[i]||strlen(texts[i])>maximum[i])return false;
        uint64 n=strlen(texts[i]);unsigned char b[8];for(int j=7;j>=0;--j){b[j]=n&255;n>>=8;}
        SHA256_Update(&s,b,8);SHA256_Update(&s,texts[i],strlen(texts[i]));}
    SHA256_Final(out,&s);return true;
}
LakeAnnRescanEntry* LakeAnnRescanFind(LakeAnnRescanCache* c,const LakeAnnRescanKey* key,bool countMiss)
{
    if(!c||c->closed||LakeTestPqDiagnosticArmed())return NULL;unsigned char hash[SHA256_DIGEST_LENGTH];if(!Key(key,hash))return NULL;
    if(LakeAnnPoisonContains(c,hash)){if(countMiss)++c->stats.misses;return NULL;}
    for(int i=0;i<LakeAnnCacheSlots;++i){auto* e=c->entries[i];if(e&&e->ready&&!memcmp(hash,e->key,sizeof(hash))){
        ++c->stats.hits;e->stamp=++c->clock;if(c->observed)LakeTestAnnCacheEvent(LAKE_ANN_CACHE_HIT,0);return e;}}
    if(countMiss)++c->stats.misses;return NULL;
}
static int Oldest(LakeAnnRescanCache* c)
{
    int at=-1;for(int i=0;i<LakeAnnCacheSlots;++i)if(c->entries[i]&&c->entries[i]->ready&&!c->entries[i]->query&&(at<0||c->entries[i]->stamp<c->entries[at]->stamp))at=i;
    return at;
}
static bool Fit(LakeAnnRescanCache* c,uint64 n)
{
    if(n>LakeAnnCacheLimit){if(c->observed)LakeTestAnnCacheEvent(LAKE_ANN_CACHE_BUDGET,n);return false;}
    while(n>LakeAnnCacheLimit-LakeAnnRetainedBytes){int at=Oldest(c);if(at<0){if(c->observed)LakeTestAnnCacheEvent(LAKE_ANN_CACHE_BUDGET,n);return false;}
        Drop(&c->entries[at]);++c->stats.evictions;if(c->observed)LakeTestAnnCacheEvent(LAKE_ANN_CACHE_EVICT,0);}
    return true;
}
static uint64 Align(uint64 n){return (n+7)&~UINT64_C(7);}
static void Status(int code,const OgLakeSdkError* error)
{
    if(code==OG_LAKE_SDK_CANCELLED||(code!=OG_LAKE_SDK_OK&&code!=OG_LAKE_SDK_INVALID&&code!=OG_LAKE_SDK_MEMORY&&
       code!=OG_LAKE_SDK_UNSUPPORTED&&code!=OG_LAKE_SDK_IO&&code!=OG_LAKE_SDK_STATE))LakeFileCheckStatus(code,error);
}
LakeAnnRescanEntry* LakeAnnRescanLoad(LakeAnnRescanCache* c,const LakeAnnRescanKey* key,OgLakeAnnReader* graph,const LakePqInput* pq)
{
    if(!c||c->closed||!pq||!pq->descriptor.pq_root[0]||LakeTestPqDiagnosticArmed())return NULL;
    unsigned char hash[SHA256_DIGEST_LENGTH];if(!Key(key,hash)||LakeAnnPoisonContains(c,hash))return NULL;
    int64 diagnosticGate=0;
    if(LakeTestAnnCacheTakeMode(key->file_id,&diagnosticGate,true)==13)
        ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("controlled live-cache error before immutable image load")));
    const auto* spec=og_lake_ann_reader_spec(graph);
    if(!spec||spec->dimension>OG_LAKE_ANN_MAX_DIM||spec->node_count>OG_LAKE_ANN_MAX_PAGES*OG_LAKE_ANN_PAGE_ROWS)return NULL;
    if(pq->descriptor.node_count!=spec->node_count||!pq->descriptor.chunks||spec->dimension%pq->descriptor.chunks)return NULL;
    /* Fixed primitive arrays: exact requested bytes, independent of opaque
     * graph-reader vector capacities. Existing format max-degree/postings are
     * upper bounds, not a guessed allocator multiplier. */
    uint64 n=spec->node_count;
    uint64 nodes=Align(n*sizeof(OgLakeAnnNode)),entries=Align((uint64)spec->entry_count*sizeof(uint32));
    uint64 vectors=Align(n*spec->dimension*sizeof(float)),edges=Align(n*OG_LAKE_ANN_MAX_DEGREE*sizeof(uint32));
    uint64 postings=Align(n*OG_LAKE_ANN_MAX_POSTINGS*sizeof(OgLakeAnnAddress));
    uint64 graphBytes=nodes+entries+vectors+edges+postings;
    if(graphBytes>LakeAnnCacheLimit||!Fit(c,graphBytes+sizeof(LakeAnnRescanEntry)))return NULL;
    int slot=-1;for(int i=0;i<LakeAnnCacheSlots;++i)if(!c->entries[i]){slot=i;break;}
    if(slot<0){slot=Oldest(c);if(slot<0)return NULL;Drop(&c->entries[slot]);++c->stats.evictions;if(c->observed)LakeTestAnnCacheEvent(LAKE_ANN_CACHE_EVICT,0);}
    auto* e=(LakeAnnRescanEntry*)std::calloc(1,sizeof(LakeAnnRescanEntry));if(!e)return NULL;
    c->entries[slot]=e;e->cache=c;memcpy(e->key,hash,sizeof(hash));e->stamp=++c->clock;
    PG_TRY();{
        e->graph=std::malloc(Max(graphBytes,UINT64_C(1)));
        if(e->graph){
            char* p=(char*)e->graph;e->nodes=(OgLakeAnnNode*)p;p+=nodes;e->entries=(uint32*)p;p+=entries;
            float* v=(float*)p;p+=vectors;uint32* neighbors=(uint32*)p;p+=edges;auto* addresses=(OgLakeAnnAddress*)p;
            e->spec=*spec;memcpy(e->graph_context,spec->context_hash,65);e->spec.context_hash=e->graph_context;uint32 count=0;
            for(uint32 id=0;id<spec->node_count;++id){const OgLakeAnnNode* node=NULL;OgLakeSdkError error;
                HOLD_INTERRUPTS();int code=og_lake_ann_reader_node(graph,id,&node,&error);RESUME_INTERRUPTS();LakeFileCheckStatus(code,&error);CHECK_FOR_INTERRUPTS();
                e->nodes[id]=*node;e->nodes[id].vector=v+(uint64)id*spec->dimension;
                e->nodes[id].neighbors=neighbors+(uint64)id*OG_LAKE_ANN_MAX_DEGREE;
                e->nodes[id].postings=addresses+(uint64)id*OG_LAKE_ANN_MAX_POSTINGS;
                memcpy((void*)e->nodes[id].vector,node->vector,spec->dimension*sizeof(float));
                if(node->neighbor_count)memcpy((void*)e->nodes[id].neighbors,node->neighbors,node->neighbor_count*sizeof(uint32));
                if(node->posting_count)memcpy((void*)e->nodes[id].postings,node->postings,node->posting_count*sizeof(OgLakeAnnAddress));
                if(node->entry){if(count>=spec->entry_count)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("cached graph entry population differs")));e->entries[count++]=id;}
            }
            if(count!=spec->entry_count)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("cached graph entry population differs")));
            PqParquetSpec ps={};ps.struct_size=sizeof(ps);ps.version=PQ_PARQUET_VERSION;ps.dimension=spec->dimension;
            ps.metric=spec->metric;ps.chunks=pq->descriptor.chunks;ps.training_rows=pq->descriptor.training_rows;
            ps.node_count=spec->node_count;ps.source_file_id=pq->source_file;ps.source_version_hash=pq->source_hash;ps.schema_hash=pq->schema_hash;
            OgLakeSdkLimits limits=LakeAnnPqLimits(spec->dimension,spec->node_count,pq->descriptor.chunks);
            OgLakeSdkError error;char context[65];uint64 adapter=0,cpu=0,sdk=0;
            HOLD_INTERRUPTS();int code=pq_parquet_graph_context(&ps,spec->context_hash,pq->descriptor.graph_root,context,&error);RESUME_INTERRUPTS();LakeFileCheckStatus(code,&error);ps.context_hash=context;
            HOLD_INTERRUPTS();code=pq_parquet_budget(&ps,&limits,&adapter,&cpu,&sdk,&error);RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
            if(code==OG_LAKE_SDK_OK){
                limits.footer_bytes=Min(limits.footer_bytes,(int64)sdk);e->limits=limits;
                PqParquetPaths paths={sizeof(paths),psprintf("%s/%s",pq->directory,LAKE_PQ_MODEL_FILE),psprintf("%s/%s",pq->directory,LAKE_PQ_CODES_FILE),psprintf("%s/%s",pq->directory,LAKE_PQ_MANIFEST_FILE)};
                HOLD_INTERRUPTS();code=pq_parquet_reader_open(LakeFileProvider(),&ps,&limits,&paths,pq->descriptor.pq_root,&e->pq,&error);RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
                if(code==OG_LAKE_SDK_OK){PqParquetWritten written={};written.struct_size=sizeof(written);PqCpuInfo model={},codes={};model.struct_size=sizeof(model);codes.struct_size=sizeof(codes);
                    HOLD_INTERRUPTS();code=pq_parquet_reader_info(e->pq,&written,&model,&codes,&error);RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
                    /* adapter is the existing qualified component reservation,
                     * including its wrapper/strings; no opaque sizeof guessed. */
                    uint64 charge=graphBytes+sizeof(*e)+adapter+model.resident_bytes+codes.resident_bytes;
                    if(code==OG_LAKE_SDK_OK&&Fit(c,charge)){
                        e->bytes=charge;Charge(c,charge);e->ready=true;++c->stats.loads;if(c->observed)LakeTestAnnCacheEvent(LAKE_ANN_CACHE_LOAD,0);
                        int64 gate=0;int mode=LakeTestAnnCacheTakeMode(key->file_id,&gate);
                        if(mode){
                            /* Only this cache was registered with the private
                             * child. No caller buffer/snapshot/lock is moved. */
                            ResourceOwner child=c->owner,parent=c->diagnostic_parent;
                            if(!parent||child==parent)
                                ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("cache diagnostic has no private source owner")));
                            ResourceOwnerConcat(parent,child);
                            ResourceOwnerDelete(child);
                            if(c->owner!=parent||!c->remembered||c->closed)
                                ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("live cache ownership lost after actual Concat")));
                            c->diagnostic_parent=NULL;
                            if(mode==11)ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("controlled live-cache error after Concat")));
                            if(mode==12)LakeTestAnnCachePause(gate);
                        }
                    }
                }
            }
            Status(code,&error);
            if(!e->ready){
                if(code!=OG_LAKE_SDK_OK&&code!=OG_LAKE_SDK_MEMORY)
                    LakeAnnRescanInvalidate(c,NULL,key);
                Drop(&c->entries[slot]);
            }
        }else Drop(&c->entries[slot]);
    }PG_CATCH();{Drop(&c->entries[slot]);PG_RE_THROW();}PG_END_TRY();
    return c->entries[slot];
}
const OgLakeAnnSpec* LakeAnnRescanSpec(const LakeAnnRescanEntry* e){return e?&e->spec:NULL;}
const OgLakeAnnNode* LakeAnnRescanNode(const LakeAnnRescanEntry* e,uint32 id)
{if(!e||id>=e->spec.node_count)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("cached ANN node is out of range")));return &e->nodes[id];}
const uint32* LakeAnnRescanEntries(const LakeAnnRescanEntry* e,uint32* count)
{*count=e->spec.entry_count;return e->entries;}
static void SharedInvalidateSegment(LakeAnnSharedSegment* s);
static bool HandleCachedQueryError(LakeAnnRescanEntry* e,int code)
{
    LakeAnnRescanEndQuery(e);
    if(code!=OG_LAKE_SDK_MEMORY){
        if(e->shared){
            e->ready=false;
            SharedInvalidateSegment(e->shared);
        }else if(e->cache&&!e->cache->closed){
            e->ready=false;
            pq_parquet_reader_close(&e->pq);
        }
        if(e->cache&&!e->cache->closed){
            LakeAnnPoisonHash(e->cache,e->key);
            ++e->cache->stats.invalidations;
            if(e->cache->observed)LakeTestAnnCacheEvent(LAKE_ANN_CACHE_EVICT,0);
        }
    }
    return false;
}
bool LakeAnnRescanQuery(LakeAnnRescanEntry* e,const float* vector)
{
    if(!e||!e->pq)return false;Assert(!e->query);
    OgLakeSdkLimits limits=e->limits.struct_size==sizeof(e->limits)?e->limits:LakeFileLimits();
    OgLakeSdkError error;
    HOLD_INTERRUPTS();int code=pq_parquet_query_open(e->pq,&limits,vector,e->spec.dimension,&e->query,&error);RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
    Status(code,&error);
    if(code!=OG_LAKE_SDK_OK)return HandleCachedQueryError(e,code);
    if(e->cache){++e->cache->stats.queries;if(e->cache->observed)LakeTestAnnCacheEvent(LAKE_ANN_CACHE_QUERY,0);}
    return true;
}
bool LakeAnnRescanPrecomputeDistances(LakeAnnRescanEntry* e,uint32 nodeCount,float* output)
{
    if(!e||!e->pq||!e->query||!output||nodeCount==0||nodeCount!=e->spec.node_count||
       LakeTestPqDiagnosticArmed()||LakeTestAnnCacheObserved())return false;
    OgLakeSdkLimits limits=e->limits.struct_size==sizeof(e->limits)?e->limits:LakeFileLimits();
    OgLakeSdkError error;
    HOLD_INTERRUPTS();int code=pq_parquet_distances(e->pq,e->query,&limits,0,nodeCount,output,nodeCount,&error);RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
    Status(code,&error);
    if(code!=OG_LAKE_SDK_OK)return HandleCachedQueryError(e,code);
    return true;
}
bool LakeAnnRescanDistance(LakeAnnRescanEntry* e,uint32 id,float* result)
{
    if(!e||!e->query)return false;
    OgLakeSdkLimits limits=e->limits.struct_size==sizeof(e->limits)?e->limits:LakeFileLimits();
    OgLakeSdkError error;
    HOLD_INTERRUPTS();int code=pq_parquet_distances(e->pq,e->query,&limits,id,1,result,1,&error);RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
    Status(code,&error);
    if(code!=OG_LAKE_SDK_OK)return HandleCachedQueryError(e,code);
    return true;
}

static struct SharedAnnPool {
    pthread_mutex_t mu=PTHREAD_MUTEX_INITIALIZER;
    std::vector<LakeAnnSharedSegment*> slots;
    uint64 clock=0;
    uint64 bytes=0;
    ~SharedAnnPool(){
        for(auto* s:slots)if(s&&s->refcount==0)SharedSegmentDestroy(s);
        slots.clear();bytes=0;
    }
} g_sharedAnnPool;

static void SharedSegmentDestroy(LakeAnnSharedSegment* s)
{
    if(!s)return;
    pq_parquet_reader_close(&s->pq);
    std::free(s->shared_edges);
    std::free(s->graph);
    std::free(s);
}
static void SharedSegmentUnref(LakeAnnSharedSegment* s)
{
    if(!s)return;
    LakeAnnSharedSegment* destroy=NULL;
    (void)pthread_mutex_lock(&g_sharedAnnPool.mu);
    if(s->refcount>0)--s->refcount;
    if(s->refcount==0&&s->invalidated)destroy=s;
    (void)pthread_mutex_unlock(&g_sharedAnnPool.mu);
    if(destroy)SharedSegmentDestroy(destroy);
}
static void SharedBorrowRelease(std::shared_ptr<void> pointer)
{
    auto* e=(LakeAnnRescanEntry*)pointer.get();
    Drop(&e);
}
static const char* ArtifactFileName(int index,bool* requiredForPq)
{
    static const char* const names[5]={
        OG_LAKE_ANN_NODES_FILE,OG_LAKE_ANN_LOOKUP_FILE,
        LAKE_PQ_MODEL_FILE,LAKE_PQ_CODES_FILE,LAKE_PQ_MANIFEST_FILE
    };
    *requiredForPq=(index>=2);
    return names[index];
}
static bool CaptureArtifactStamps(const char* directory,bool hasPq,LakeAnnFileStamp out[5])
{
    if(!directory||!*directory)return false;
    for(int i=0;i<5;++i){
        bool pqOnly=false;const char* name=ArtifactFileName(i,&pqOnly);
        out[i]=LakeAnnFileStamp{};
        if(pqOnly&&!hasPq)continue;
        char path[MAXPGPATH];
        int written=snprintf(path,sizeof(path),"%s/%s",directory,name);
        if(written<=0||(size_t)written>=sizeof(path))return false;
        struct stat st={};
        if(stat(path,&st)!=0)return false;
        out[i].expected=true;
        out[i].dev=(uint64)st.st_dev;
        out[i].ino=(uint64)st.st_ino;
        out[i].size=(uint64)st.st_size;
        out[i].mtime_sec=(int64)st.st_mtim.tv_sec;
        out[i].mtime_nsec=(int64)st.st_mtim.tv_nsec;
        out[i].ctime_sec=(int64)st.st_ctim.tv_sec;
        out[i].ctime_nsec=(int64)st.st_ctim.tv_nsec;
    }
    return true;
}
static bool VerifyArtifactStamps(const char* directory,bool hasPq,const LakeAnnFileStamp expected[5])
{
    LakeAnnFileStamp current[5];
    if(!CaptureArtifactStamps(directory,hasPq,current))return false;
    return memcmp(current,expected,sizeof(current))==0;
}
static void SharedInvalidateSegment(LakeAnnSharedSegment* s)
{
    if(!s)return;
    bool retireNow=false;
    HOLD_INTERRUPTS();
    (void)pthread_mutex_lock(&g_sharedAnnPool.mu);
    for(size_t i=0;i<g_sharedAnnPool.slots.size();++i){
        if(g_sharedAnnPool.slots[i]==s){
            g_sharedAnnPool.bytes-=s->bytes;
            s->invalidated=true;
            g_sharedAnnPool.slots.erase(g_sharedAnnPool.slots.begin()+i);
            retireNow=(s->refcount==0);
            break;
        }
    }
    if(!s->invalidated)s->invalidated=true;
    (void)pthread_mutex_unlock(&g_sharedAnnPool.mu);
    RESUME_INTERRUPTS();
    if(retireNow)SharedSegmentDestroy(s);
}
static LakeAnnRescanEntry* SharedAttachEntry(LakeAnnRescanCache* c,LakeAnnSharedSegment* s,MemoryContext owner,bool isLoad)
{
    if(!s)return NULL;
    auto* e=(LakeAnnRescanEntry*)std::calloc(1,sizeof(LakeAnnRescanEntry));
    if(!e)return NULL;
    e->shared=s;e->ready=true;
    memcpy(e->key,s->key,sizeof(e->key));
    e->spec=s->spec;memcpy(e->graph_context,s->graph_context,65);e->spec.context_hash=e->graph_context;
    e->nodes=s->nodes;e->entries=s->entries;e->limits=s->limits;e->pq=s->pq;
    if(c&&!c->closed){
        int slot=-1;for(int i=0;i<LakeAnnCacheSlots;++i)if(!c->entries[i]){slot=i;break;}
        if(slot<0){slot=Oldest(c);if(slot<0){e->shared=NULL;std::free(e);return NULL;}Drop(&c->entries[slot]);++c->stats.evictions;if(c->observed)LakeTestAnnCacheEvent(LAKE_ANN_CACHE_EVICT,0);}
        c->entries[slot]=e;e->cache=c;e->stamp=++c->clock;
        if(isLoad)++c->stats.loads;else ++c->stats.hits;
        c->stats.peak=Max(c->stats.peak,c->stats.bytes+s->bytes);
        return e;
    }
    auto* cb=(MemoryContextCallback*)MemoryContextAlloc(owner,sizeof(MemoryContextCallback));
    new(cb)MemoryContextCallback();
    cb->func=SharedBorrowRelease;
    cb->arg=std::shared_ptr<void>(std::shared_ptr<void>(),e);
    MemoryContextRegisterResetCallback(owner,cb);
    return e;
}
bool LakeAnnSharedPoolEnabled(void)
{
    return !g_instance.attr.attr_common.enable_thread_pool&&
           !LakeTestPqDiagnosticArmed()&&
           !LakeTestAnnCacheObserved()&&
           !LakeTestAnnCacheDiagnosticPending();
}
LakeAnnRescanEntry* LakeAnnSharedAcquire(LakeAnnRescanCache* c,const LakeAnnRescanKey* key,bool hasPq,MemoryContext owner)
{
    if(!LakeAnnSharedPoolEnabled()||!key||!owner){
        if(c&&!c->closed)++c->stats.misses;
        return NULL;
    }
    unsigned char hash[SHA256_DIGEST_LENGTH];
    if(!Key(key,hash)){
        if(c&&!c->closed)++c->stats.misses;
        return NULL;
    }
    LakeAnnSharedSegment* hit=NULL;
    HOLD_INTERRUPTS();
    (void)pthread_mutex_lock(&g_sharedAnnPool.mu);
    for(auto* s:g_sharedAnnPool.slots){
        if(s&&!s->invalidated&&s->has_pq==hasPq&&memcmp(s->key,hash,sizeof(hash))==0){
            s->stamp=++g_sharedAnnPool.clock;
            ++s->refcount;
            hit=s;
            break;
        }
    }
    (void)pthread_mutex_unlock(&g_sharedAnnPool.mu);
    RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
    if(hit){
        if(!VerifyArtifactStamps(hit->directory,hit->has_pq,hit->stamps)){
            SharedInvalidateSegment(hit);
            SharedSegmentUnref(hit);
            hit=NULL;
        }
    }
    if(!hit){
        if(c&&!c->closed)++c->stats.misses;
        return NULL;
    }
    LakeAnnRescanEntry* wrapper=SharedAttachEntry(c,hit,owner,false);
    if(!wrapper){SharedSegmentUnref(hit);return NULL;}
    return wrapper;
}
void LakeAnnSharedInvalidate(const LakeAnnRescanKey* key)
{
    if(!key)return;
    unsigned char hash[SHA256_DIGEST_LENGTH];if(!Key(key,hash))return;
    std::vector<LakeAnnSharedSegment*> retire;
    HOLD_INTERRUPTS();
    (void)pthread_mutex_lock(&g_sharedAnnPool.mu);
    for(size_t i=0;i<g_sharedAnnPool.slots.size();){
        auto* s=g_sharedAnnPool.slots[i];
        if(s&&memcmp(s->key,hash,sizeof(hash))==0){
            g_sharedAnnPool.bytes-=s->bytes;
            s->invalidated=true;
            g_sharedAnnPool.slots.erase(g_sharedAnnPool.slots.begin()+i);
            if(s->refcount==0)retire.push_back(s);
        }else ++i;
    }
    (void)pthread_mutex_unlock(&g_sharedAnnPool.mu);
    RESUME_INTERRUPTS();
    for(auto* s:retire)SharedSegmentDestroy(s);
}
LakeAnnRescanEntry* LakeAnnSharedLoad(LakeAnnRescanCache* c,const LakeAnnRescanKey* key,OgLakeAnnReader* graph,const LakePqInput* pq,MemoryContext owner)
{
    if(!LakeAnnSharedPoolEnabled()||!key||!key->directory||!graph||!owner)return NULL;
    bool hasPq=pq!=NULL&&pq->descriptor.pq_root[0]!='\0';
    unsigned char hash[SHA256_DIGEST_LENGTH];if(!Key(key,hash))return NULL;
    const auto* spec=og_lake_ann_reader_spec(graph);
    if(!spec||spec->dimension>OG_LAKE_ANN_MAX_DIM||spec->node_count>OG_LAKE_ANN_MAX_PAGES*OG_LAKE_ANN_PAGE_ROWS)return NULL;
    if(hasPq&&(pq->descriptor.node_count!=spec->node_count||!pq->descriptor.chunks||spec->dimension%pq->descriptor.chunks))return NULL;
    if(strlen(key->directory)>=MAXPGPATH)return NULL;
    LakeAnnFileStamp stamps[5];
    if(!CaptureArtifactStamps(key->directory,hasPq,stamps))return NULL;
    uint64 n=spec->node_count;
    uint64 totalPostings=(uint64)Max((int64)0,spec->posting_count);
    uint64 nodes=Align(n*sizeof(OgLakeAnnNode)),entries=Align((uint64)spec->entry_count*sizeof(uint32));
    uint64 vectors=Align(n*spec->dimension*sizeof(float)),postings=Align(totalPostings*sizeof(OgLakeAnnAddress));
    uint64 graphBytes=nodes+entries+vectors+postings;
    if(graphBytes>LakeAnnSharedPoolLimit)return NULL;
    auto* s=(LakeAnnSharedSegment*)std::calloc(1,sizeof(LakeAnnSharedSegment));
    if(!s)return NULL;
    memcpy(s->key,hash,sizeof(hash));
    s->has_pq=hasPq;
    snprintf(s->directory,sizeof(s->directory),"%s",key->directory);
    memcpy(s->stamps,stamps,sizeof(stamps));
    s->graph=std::malloc(Max(graphBytes,UINT64_C(1)));
    auto* edgeVec=new (std::nothrow) std::vector<uint32>();
    auto* edgeOffsets=new (std::nothrow) std::vector<uint64>();
    if(!s->graph||!edgeVec||!edgeOffsets){delete edgeVec;delete edgeOffsets;SharedSegmentDestroy(s);return NULL;}
    char* p=(char*)s->graph;s->nodes=(OgLakeAnnNode*)p;p+=nodes;s->entries=(uint32*)p;p+=entries;
    float* v=(float*)p;p+=vectors;auto* addresses=(OgLakeAnnAddress*)p;
    s->spec=*spec;memcpy(s->graph_context,spec->context_hash,65);s->spec.context_hash=s->graph_context;
    LakeAnnSharedSegment* volatile chosen=NULL;
    LakeAnnSharedSegment* volatile ownPtr=s;
    PG_TRY();{
        edgeOffsets->reserve(n);
        uint32 count=0;uint64 postAt=0;
        for(uint32 id=0;id<spec->node_count;++id){const OgLakeAnnNode* node=NULL;OgLakeSdkError error;
            HOLD_INTERRUPTS();int code=og_lake_ann_reader_node(graph,id,&node,&error);RESUME_INTERRUPTS();LakeFileCheckStatus(code,&error);CHECK_FOR_INTERRUPTS();
            s->nodes[id]=*node;s->nodes[id].vector=v+(uint64)id*spec->dimension;
            memcpy((void*)s->nodes[id].vector,node->vector,spec->dimension*sizeof(float));
            edgeOffsets->push_back((uint64)edgeVec->size());
            if(node->neighbor_count)edgeVec->insert(edgeVec->end(),node->neighbors,node->neighbors+node->neighbor_count);
            if(postAt+node->posting_count>totalPostings)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("cached graph posting population exceeds spec")));
            s->nodes[id].postings=node->posting_count?(addresses+postAt):NULL;
            if(node->posting_count){
                memcpy((void*)s->nodes[id].postings,node->postings,node->posting_count*sizeof(OgLakeAnnAddress));
                postAt+=node->posting_count;
            }
            if(node->entry){if(count>=spec->entry_count)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("cached graph entry population differs")));s->entries[count++]=id;}
        }
        if(count!=spec->entry_count)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("cached graph entry population differs")));
        uint64 edgeBytes=Align((uint64)edgeVec->size()*sizeof(uint32));
        if(!edgeVec->empty()){
            s->shared_edges=(uint32*)std::malloc(edgeBytes);
            if(!s->shared_edges){delete edgeVec;delete edgeOffsets;SharedSegmentDestroy(ownPtr);ownPtr=NULL;return NULL;}
            memcpy(s->shared_edges,edgeVec->data(),edgeVec->size()*sizeof(uint32));
        }
        for(uint32 id=0;id<spec->node_count;++id){
            s->nodes[id].neighbors=s->nodes[id].neighbor_count?(s->shared_edges+(*edgeOffsets)[id]):NULL;
        }
        delete edgeVec;edgeVec=NULL;
        delete edgeOffsets;edgeOffsets=NULL;
        uint64 charge=graphBytes+edgeBytes+sizeof(*s);
        int code=OG_LAKE_SDK_OK;OgLakeSdkError error={};
        if(hasPq){
            PqParquetSpec ps={};ps.struct_size=sizeof(ps);ps.version=PQ_PARQUET_VERSION;ps.dimension=spec->dimension;
            ps.metric=spec->metric;ps.chunks=pq->descriptor.chunks;ps.training_rows=pq->descriptor.training_rows;
            ps.node_count=spec->node_count;ps.source_file_id=pq->source_file;ps.source_version_hash=pq->source_hash;ps.schema_hash=pq->schema_hash;
            OgLakeSdkLimits limits=LakeAnnPqLimits(spec->dimension,spec->node_count,pq->descriptor.chunks);
            char context[65];uint64 adapter=0,cpu=0,sdk=0;
            HOLD_INTERRUPTS();code=pq_parquet_graph_context(&ps,spec->context_hash,pq->descriptor.graph_root,context,&error);RESUME_INTERRUPTS();LakeFileCheckStatus(code,&error);ps.context_hash=context;
            HOLD_INTERRUPTS();code=pq_parquet_budget(&ps,&limits,&adapter,&cpu,&sdk,&error);RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
            if(code==OG_LAKE_SDK_OK){
                limits.footer_bytes=Min(limits.footer_bytes,(int64)sdk);s->limits=limits;
                PqParquetPaths paths={sizeof(paths),psprintf("%s/%s",pq->directory,LAKE_PQ_MODEL_FILE),psprintf("%s/%s",pq->directory,LAKE_PQ_CODES_FILE),psprintf("%s/%s",pq->directory,LAKE_PQ_MANIFEST_FILE)};
                HOLD_INTERRUPTS();code=pq_parquet_reader_open(LakeFileProvider(),&ps,&limits,&paths,pq->descriptor.pq_root,&s->pq,&error);RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
                if(code==OG_LAKE_SDK_OK){PqParquetWritten written={};written.struct_size=sizeof(written);PqCpuInfo model={},codes={};model.struct_size=sizeof(model);codes.struct_size=sizeof(codes);
                    HOLD_INTERRUPTS();code=pq_parquet_reader_info(s->pq,&written,&model,&codes,&error);RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
                    charge+=adapter+model.resident_bytes+codes.resident_bytes;
                }
            }
            Status(code,&error);
            if(code!=OG_LAKE_SDK_OK){SharedSegmentDestroy(ownPtr);ownPtr=NULL;return NULL;}
        }
        if(charge==0||charge>LakeAnnSharedPoolLimit||!VerifyArtifactStamps(s->directory,hasPq,stamps)){SharedSegmentDestroy(ownPtr);ownPtr=NULL;return NULL;}
        s->bytes=charge;
        std::vector<LakeAnnSharedSegment*> retire;
        HOLD_INTERRUPTS();
        (void)pthread_mutex_lock(&g_sharedAnnPool.mu);
        for(auto* cur:g_sharedAnnPool.slots){
            if(cur&&!cur->invalidated&&cur->has_pq==hasPq&&memcmp(cur->key,hash,sizeof(hash))==0){
                cur->stamp=++g_sharedAnnPool.clock;++cur->refcount;chosen=cur;break;
            }
        }
        if(!chosen){
            while(!g_sharedAnnPool.slots.empty()&&g_sharedAnnPool.bytes+charge>LakeAnnSharedPoolLimit){
                int oldest=-1;
                for(size_t i=0;i<g_sharedAnnPool.slots.size();++i){
                    auto* cur=g_sharedAnnPool.slots[i];
                    if(cur&&cur->refcount==0&&(oldest<0||cur->stamp<g_sharedAnnPool.slots[oldest]->stamp))oldest=(int)i;
                }
                if(oldest<0)break;
                auto* victim=g_sharedAnnPool.slots[oldest];
                g_sharedAnnPool.bytes-=victim->bytes;
                victim->invalidated=true;
                g_sharedAnnPool.slots.erase(g_sharedAnnPool.slots.begin()+oldest);
                retire.push_back(victim);
            }
            if(g_sharedAnnPool.bytes+charge<=LakeAnnSharedPoolLimit){
                s->stamp=++g_sharedAnnPool.clock;s->refcount=1;
                g_sharedAnnPool.bytes+=charge;g_sharedAnnPool.slots.push_back(s);
                chosen=s;ownPtr=NULL;
            }else{
                /* Pool is saturated by active pinned readers; lend standalone segment just for this statement. */
                s->refcount=1;s->invalidated=true;chosen=s;ownPtr=NULL;
            }
        }
        (void)pthread_mutex_unlock(&g_sharedAnnPool.mu);
        RESUME_INTERRUPTS();
        for(auto* dead:retire)SharedSegmentDestroy(dead);
        if(ownPtr){SharedSegmentDestroy(ownPtr);ownPtr=NULL;}
        LakeAnnRescanEntry* wrapper=SharedAttachEntry(c,chosen,owner,true);
        if(!wrapper){SharedSegmentUnref(chosen);chosen=NULL;return NULL;}
        chosen=NULL;
        return wrapper;
    }PG_CATCH();{
        delete edgeVec;delete edgeOffsets;
        if(ownPtr)SharedSegmentDestroy(ownPtr);
        if(chosen)SharedSegmentUnref(chosen);
        PG_RE_THROW();
    }PG_END_TRY();
}
#endif
