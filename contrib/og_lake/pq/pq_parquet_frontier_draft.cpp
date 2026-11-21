/* Isolated callback facade only. Native ownership/format2/publication hooks
 * deliberately remain uninstalled until the root's official adapter slice. */
#include "pq_parquet_frontier_draft.h"
#include <openssl/sha.h>
#include <cstring>
#include <memory>
#include <new>

namespace {
bool Hex(const char* p){if(!p||std::strlen(p)!=64)return false;for(int i=0;i<64;++i)if(!((p[i]>='0'&&p[i]<='9')||(p[i]>='a'&&p[i]<='f')))return false;return true;}
int Error(OgLakeSdkError* e,int code,const char* message)
{if(e){*e={};e->code=code;std::strncpy(e->message,message,sizeof(e->message)-1);}return code;}
void Number(SHA256_CTX& h,uint64_t n){unsigned char b[8];for(int i=7;i>=0;--i){b[i]=n&255;n>>=8;}SHA256_Update(&h,b,8);}
}
struct PqParquetFrontier {
    PqParquetReader* reader=nullptr;PqParquetQuery* query=nullptr;uint64_t nodes=0;bool disabled=false;
    ~PqParquetFrontier(){pq_parquet_query_close(&query);pq_parquet_reader_close(&reader);}
};
extern "C" int pq_parquet_graph_context(const PqParquetSpec* s,const char* graph,const char* manifest,char out[65],OgLakeSdkError* e)
{
    if(e)*e={};if(!s||s->struct_size!=sizeof(*s)||s->version!=1||!out||!Hex(graph)||!Hex(manifest)||!Hex(s->source_version_hash)||!Hex(s->schema_hash)||s->source_file_id<0||
        !s->dimension||s->dimension>PQ_CPU_MAX_DIM||!s->chunks||s->chunks>s->dimension||s->dimension%s->chunks||s->metric<1||s->metric>3||s->node_count>PQ_CPU_MAX_NODES)
        return Error(e,OG_LAKE_SDK_INVALID,"invalid exact graph/PQ context request");
    SHA256_CTX h;SHA256_Init(&h);const char domain[]="og_lake_native_pq_graph_context_v1";SHA256_Update(&h,domain,sizeof(domain)-1);
    for(auto p:{graph,manifest,s->source_version_hash,s->schema_hash})SHA256_Update(&h,p,64);
    Number(h,s->source_file_id);Number(h,s->node_count);Number(h,s->dimension);Number(h,s->chunks);Number(h,s->metric);Number(h,PQ_CPU_ALGORITHM_VERSION);
    unsigned char raw[32];SHA256_Final(raw,&h);const char digits[]="0123456789abcdef";char result[65]{};
    for(int i=0;i<32;++i){result[i*2]=digits[raw[i]>>4];result[i*2+1]=digits[raw[i]&15];}std::memcpy(out,result,65);return 0;
}
extern "C" int pq_parquet_frontier_open(const OgLakeParquetSdkApi* api,const PqParquetSpec* s,const OgLakeSdkLimits* limits,const PqParquetPaths* paths,const char* root,
    const char* graph,const char* manifest,const float* query,uint64_t count,PqParquetFrontier** out,OgLakeSdkError* e)
{
    if(e)*e={};if(!out||*out)return Error(e,OG_LAKE_SDK_INVALID,"occupied/missing PQ frontier output");
    char context[65];int code=pq_parquet_graph_context(s,graph,manifest,context,e);if(code)return code;
    if(!Hex(s->context_hash)||std::strcmp(context,s->context_hash))return Error(e,OG_LAKE_SDK_INVALID,"PQ context does not bind the exact verified graph/node order");
    try{auto p=std::unique_ptr<PqParquetFrontier>(new PqParquetFrontier);p->nodes=s->node_count;
        code=pq_parquet_reader_open(api,s,limits,paths,root,&p->reader,e);if(code)return code;
        code=pq_parquet_query_open(p->reader,limits,query,count,&p->query,e);if(code)return code;*out=p.release();return 0;
    }catch(const std::bad_alloc&){return Error(e,OG_LAKE_SDK_MEMORY,"PQ frontier handle allocation failed");}
    catch(...){return Error(e,OG_LAKE_SDK_STATE,"unexpected PQ frontier boundary exception");}
}
extern "C" int pq_parquet_frontier_distance(PqParquetFrontier* p,const OgLakeSdkLimits* limits,uint64_t node,float* out,OgLakeSdkError* e)
{
    if(e)*e={};if(!p||p->disabled||!p->reader||!p->query)return Error(e,OG_LAKE_SDK_STATE,"PQ frontier is disabled");
    if(!out||node>=p->nodes)return Error(e,OG_LAKE_SDK_INVALID,"PQ node is outside the exact dense graph");
    float value=0;int code=pq_parquet_distances(p->reader,p->query,limits,node,1,&value,1,e);if(!code)*out=value;return code;
}
extern "C" int pq_parquet_frontier_fallback(int status)
{
    if(status==0)return PQ_PARQUET_CONTINUE;
    if(status==OG_LAKE_SDK_CANCELLED)return PQ_PARQUET_ABORT_QUERY;
    if(status==OG_LAKE_SDK_INVALID||status==OG_LAKE_SDK_IO||status==OG_LAKE_SDK_MEMORY||status==OG_LAKE_SDK_UNSUPPORTED||status==OG_LAKE_SDK_STATE)return PQ_PARQUET_RESTART_FULL_GRAPH;
    return PQ_PARQUET_ABORT_QUERY;
}
extern "C" void pq_parquet_frontier_disable(PqParquetFrontier* p)
{if(p){pq_parquet_query_close(&p->query);pq_parquet_reader_close(&p->reader);p->disabled=true;}}
extern "C" void pq_parquet_frontier_close(PqParquetFrontier** p){if(p){delete *p;*p=nullptr;}}
