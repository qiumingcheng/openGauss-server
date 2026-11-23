/* Real standard LIST<FLOAT> codec, including borrowed nested C Data. */
#include "sdk/lake_parquet_sdk.h"
#include "vendor/arrow20/abi.h"
#include <dirent.h>
#include <dlfcn.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static unsigned long checks;
static void check(int yes,const char* name)
{++checks;if(!yes){fprintf(stderr,"list codec check failed: %s\n",name);exit(1);}}
static void ok(int code,OgLakeSdkError* e)
{++checks;if(code){fprintf(stderr,"list codec error %d: %s\n",code,e->message);exit(1);}}
static void ar(struct ArrowArray* a){a->release=NULL;}
static void sr(struct ArrowSchema* s){s->release=NULL;}
static int cancel(void* p){return *(int*)p;}
static int fds(void)
{int n=0;DIR* d=opendir("/proc/self/fd");check(d!=NULL,"fd inventory");while(readdir(d))++n;closedir(d);return n;}
static const uint32_t bits[]={0,0x80000000u,0x3f800000u,0xbf800000u,1,0x80000001u,
    0x007fffffu,0x00800000u,0x7f7fffffu,0xff7fffffu,0x7f800000u,0xff800000u,
    0x7fc12345u,0xffc01234u,0x3eaaaaabu,0xbeaaaaabu};
static int row_null(int64_t r){return r%17==16;}
static int length(int64_t r){return row_null(r)||r%7==6?0:3;}
static int child_null(int64_t r,int k){return r%13==12&&k==1;}
static int isnull(const struct ArrowArray* a,int64_t i)
{int64_t n=a->offset+i;return a->null_count!=0&&a->buffers[0]&&
    !(((const unsigned char*)a->buffers[0])[n/8]&(1u<<(n%8)));}

static void append(const OgLakeParquetSdkApi* api,OgLakeSdkWriter* writer,int64_t start,int count,int bad)
{
    enum {MAX=4093,OFFSET=2,CHILD_OFFSET=3};check(count<=MAX,"bounded batch input");
    int32_t ids[MAX],offsets[MAX+OFFSET+1];float values[MAX*3+CHILD_OFFSET];
    unsigned char validity[(MAX+OFFSET+7)/8],child_validity[(MAX*3+CHILD_OFFSET+7)/8];
    memset(validity,0,sizeof(validity));memset(child_validity,0,sizeof(child_validity));
    memset(offsets,0,sizeof(offsets));int n=0,nulls=0,child_nulls=0;
    for(int i=0;i<count;++i){int64_t r=start+i;ids[i]=(int32_t)r+1;
        if(row_null(r))++nulls;else validity[(i+OFFSET)/8]|=(unsigned char)(1u<<((i+OFFSET)%8));
        offsets[i+OFFSET]=n;
        for(int k=0;k<length(r);++k){memcpy(&values[n+CHILD_OFFSET],&bits[(r*3+k)%16],4);
            if(child_null(r,k))++child_nulls;else child_validity[(n+CHILD_OFFSET)/8]|=(unsigned char)(1u<<((n+CHILD_OFFSET)%8));++n;}
        offsets[i+OFFSET+1]=n;
    }
    if(bad==1)offsets[count+OFFSET]=n+1;
    const void* rb[]={NULL};const void* ib[]={NULL,ids};const void* lb[]={validity,offsets};const void* vb[]={child_validity,values};
    struct ArrowArray root={0},id={0},list={0},element={0};
    struct ArrowSchema schema={0},is={0},ls={0},es={0};
    element.length=n;element.offset=CHILD_OFFSET;element.null_count=child_nulls;element.n_buffers=2;element.buffers=vb;element.release=ar;
    es.format=bad==2?"g":"f";es.name="element";es.flags=(bad==3||bad==4)?0:2;es.release=sr;
    struct ArrowArray* lc[]={&element};struct ArrowSchema* lsc[]={&es};
    list.length=count;list.offset=OFFSET;list.null_count=nulls;list.n_buffers=2;list.buffers=lb;list.n_children=1;list.children=lc;list.release=ar;
    ls.format="+l";ls.name="embedding.湖";ls.flags=2;ls.n_children=1;ls.children=lsc;ls.release=sr;
    id.length=count;id.n_buffers=2;id.buffers=ib;id.release=ar;is.format="i";is.name="id";is.release=sr;
    struct ArrowArray* children[]={&id,&list};struct ArrowSchema* fields[]={&is,&ls};
    root.length=count;root.n_buffers=1;root.buffers=rb;root.n_children=2;root.children=children;root.release=ar;
    schema.format="+s";schema.n_children=2;schema.children=fields;schema.release=sr;
    OgLakeSdkError e={0};int code=api->writer_append(writer,&root,&schema,&e);
    if(bad>=1&&bad<=3){int expected=bad==2?OG_LAKE_SDK_UNSUPPORTED:OG_LAKE_SDK_INVALID;
        if(code!=expected)fprintf(stderr,"malformed mode %d returned %d: %s\n",bad,code,e.message);
        check(code==expected,"invalid or unsupported nested input rejected before publication");}else ok(code,&e);
    check(root.release&&schema.release&&id.release&&is.release&&list.release&&ls.release&&element.release&&es.release,
        "borrowed root/list/element callbacks remain owned by caller");
}

static void inspect(const OgLakeSdkBatch* b,int64_t i,int reference)
{
    const struct ArrowArray* v=b->array->children[0];const struct ArrowArray* id=b->array->children[1];
    int64_t r=b->original_positions[i];check(((const int32_t*)id->buffers[1])[id->offset+i]==r+1,"exact ID and physical position");
    check(strcmp(b->schema->children[0]->format,"+l")==0&&v->n_children==1&&
        strcmp(b->schema->children[0]->children[0]->format,"f")==0,"standard Arrow LIST/FLOAT structure");
    int absent=reference?r==2:row_null(r);check(isnull(v,i)==absent,"NULL list distinct from empty list");if(absent)return;
    const int32_t* offsets=v->buffers[1];int32_t begin=offsets[v->offset+i],end=offsets[v->offset+i+1];
    int want=reference?(r==1?0:r==0?3:2):length(r);check(begin>=0&&end-begin==want,"exact standard list lengths");
    const struct ArrowArray* c=v->children[0];
    for(int k=0;k<want;++k){int64_t pos=begin+k;int missing=reference?(r==3&&k==0):child_null(r,k);
        check(isnull(c,pos)==missing,"NULL elements remain distinct");if(missing)continue;
        uint32_t got;memcpy(&got,(const unsigned char*)c->buffers[1]+(c->offset+pos)*4,4);
        uint32_t wanted;
        if(reference){float value=r==0?(k==0?1.5f:k==1?0.f:2.25f):3.f;memcpy(&wanted,&value,4);}
        else wanted=bits[(r*3+k)%16];
        check(got==wanted,"lossless nested float bits including signed zero and NaN payload");
    }
}

static void read_file(const OgLakeParquetSdkApi* api,const char* path,const OgLakeSdkLimits* limits,int reference,int selection)
{
    OgLakeSdkError e={0};OgLakeSdkFooter* f=NULL;ok(api->footer_open(path,limits,&f,&e),&e);
    const OgLakeSdkFooterInfo* info=api->footer_info(f);int64_t rows=reference?4:8193;
    check(info->num_fields==2&&info->num_rows==rows,"real logical field/population count");
    check(info->fields[1].field_id==(reference?2:73)&&info->fields[1].type==OG_LAKE_SDK_FLOAT32_LIST,"LIST parent ID, not element ID");
    check(api->capabilities&OG_LAKE_SDK_LIST_FIELD_IDS,"explicit nested identity capability");
    check(info->struct_size>=OG_LAKE_SDK_FIELD_END(OgLakeSdkFooterInfo,list_fields)&&
        info->num_list_fields==1&&info->list_fields,"bounded optional footer identity suffix");
    check(info->list_fields[0].field_id==(reference?2:73)&&
        info->list_fields[0].element_field_id==(reference?3:9)&&info->list_fields[0].element_nullable==1,
        "exact parent/element IDs and physical element nullability");
    int groups=info->num_row_groups;int32_t selected[]={groups-1,0};int64_t expected_count=selection?
        info->row_groups[groups-1].num_rows+info->row_groups[0].num_rows:rows;
    int64_t first_count=info->row_groups[groups-1].num_rows,first_start=info->row_groups[groups-1].first_row;
    check(!selection||groups>1,"out-of-order row-group selection fixture");api->footer_close(&f);
    int64_t ids[]={reference?2:73,reference?1:11};OgLakeSdkReadOptions o={sizeof(o),path,*limits,257,OG_LAKE_SDK_BY_FIELD_ID,2,ids,
        selection?2:-1,selection?selected:NULL};
    OgLakeSdkReader* reader=NULL;ok(api->reader_open(&o,&reader,&e),&e);int64_t count=0;
    for(;;){OgLakeSdkBatch* b=NULL;int code=api->reader_next(reader,&b,&e);if(code==OG_LAKE_SDK_EOF)break;ok(code,&e);
        for(int64_t i=0;i<b->num_rows;++i){int64_t p=count+i,want=selection?(p<first_count?first_start+p:p-first_count):p;
            check(b->original_positions[i]==want,"source positions survive nested projection/group order");inspect(b,i,reference);}
        count+=b->num_rows;api->batch_release(&b);
    }
    check(count==expected_count,"every selected physical row exactly once");api->reader_close(&reader);
    o.num_row_groups=-1;o.row_groups=NULL;ok(api->reader_open(&o,&reader,&e),&e);OgLakeSdkBatch* retained=NULL;
    ok(api->reader_next(reader,&retained,&e),&e);api->reader_close(&reader);
    inspect(retained,0,reference);api->batch_release(&retained);check(!retained&&!reader,"nested buffers safely released after reader close");
    int64_t child_id=reference?3:9;o.num_field_ids=1;o.field_ids=&child_id;
    check(api->reader_open(&o,&reader,&e)==OG_LAKE_SDK_UNSUPPORTED&&!reader,"child ID cannot select a logical parent column");
}

int main(int argc,char** argv)
{
    check(argc==4||argc==5,"provider/output/reference arguments");
    void* lib=dlopen(argv[1],RTLD_NOW|RTLD_LOCAL);check(lib!=NULL,"load isolated real provider");
    OgLakeParquetSdkEntrypoint entry=(OgLakeParquetSdkEntrypoint)dlsym(lib,"og_lake_parquet_sdk_v1");check(entry!=NULL,"base entry");
    const OgLakeParquetSdkApi* api=entry();check(api->struct_size==sizeof(*api)&&api->abi_version==1,"base ABI unchanged");
    OgLakeSdkField fields[]={{11,OG_LAKE_SDK_INT32,0,"id"},{73,OG_LAKE_SDK_FLOAT32_LIST,1,"embedding.湖"}};
    OgLakeSdkLimits limits={sizeof(limits),64*1024*1024,16*1024*1024,NULL,NULL};OgLakeSdkError e={0};char path[4096];
    OgLakeSdkWriteOptions o={sizeof(o),path,limits,2,fields,OG_LAKE_SDK_ZSTD,1024};OgLakeSdkWriter* w=NULL;
    if(argc==5&&strcmp(argv[4],"prefix")==0){
        check(api->capabilities&OG_LAKE_SDK_FLOAT32_LIST_VALUES,"prior list-capable provider");
        check(!(api->capabilities&OG_LAKE_SDK_LIST_FIELD_IDS),"prior provider has no identity suffix capability");
        OgLakeSdkFooter* footer=NULL;ok(api->footer_open(argv[3],&limits,&footer,&e),&e);
        const OgLakeSdkFooterInfo* info=api->footer_info(footer);
        check(info->struct_size>=OG_LAKE_SDK_FIELD_END(OgLakeSdkFooterInfo,object_version)&&
            info->struct_size<OG_LAKE_SDK_FIELD_END(OgLakeSdkFooterInfo,list_fields),"new consumer recognizes old footer prefix without reading its suffix");
        check(info->num_fields==2&&info->fields[1].field_id==2,"prior footer root array remains readable");
        api->footer_close(&footer);printf("{\"status\":\"pass\",\"checks\":%lu}\n",checks);return 0;
    }
    if(argc==5&&strcmp(argv[4],"noids")==0){
        check(api->capabilities&OG_LAKE_SDK_LIST_FIELD_IDS,"missing-ID fixture uses the new capability");
        OgLakeSdkFooter* footer=NULL;ok(api->footer_open(argv[3],&limits,&footer,&e),&e);
        const OgLakeSdkFooterInfo* info=api->footer_info(footer);
        check(info->struct_size>=OG_LAKE_SDK_FIELD_END(OgLakeSdkFooterInfo,list_fields)&&info->num_list_fields==1,
            "no-ID nested fixture returns explicit metadata");
        check(info->fields[1].field_id==-1&&info->list_fields[0].field_id==-1&&info->list_fields[0].element_field_id==-1,
            "missing parent/element IDs are never invented from ordinals");
        api->footer_close(&footer);printf("{\"status\":\"pass\",\"checks\":%lu}\n",checks);return 0;
    }
    if(argc==5){check(!(api->capabilities&OG_LAKE_SDK_FLOAT32_LIST_VALUES)&&!dlsym(lib,"og_lake_parquet_sdk_v4"),"old provider lacks nested capability/ABI");
        snprintf(path,sizeof(path),"%s/old.parquet",argv[2]);check(api->writer_open(&o,&w,&e)==OG_LAKE_SDK_UNSUPPORTED&&!w,"old provider rejects list field code");
        check(access(path,F_OK)!=0,"old provider creates no list artifact");printf("{\"status\":\"pass\",\"checks\":%lu}\n",checks);return 0;}
    check(api->capabilities&OG_LAKE_SDK_FLOAT32_LIST_VALUES,"list capability explicit");
    OgLakeParquetNestedEntrypoint nested=(OgLakeParquetNestedEntrypoint)dlsym(lib,"og_lake_parquet_sdk_v4");check(nested!=NULL,"optional nested ABI");
    const OgLakeParquetNestedApi* v4=nested();check(v4->base==api&&v4->abi_version==4&&v4->struct_size==sizeof(*v4)&&v4->capabilities&OG_LAKE_SDK_WRITE_FLOAT32_LIST,"exact optional API identity");
    OgLakeSdkListField list={73,9,1};int before=fds();
    for(int codec=0;codec<3;++codec){snprintf(path,sizeof(path),"%s/list_%d.parquet",argv[2],codec);o.compression=(uint32_t)codec;
        ok(v4->writer_open(&o,1,&list,&w,&e),&e);append(api,w,0,4093,0);append(api,w,4093,4093,0);append(api,w,8186,7,0);
        OgLakeSdkWrittenFile done={0};done.struct_size=sizeof(done);ok(api->writer_finish(w,&done,&e),&e);
        check(done.local_fsync_complete&&done.num_rows==8193,"durable standard nested output");ok(api->writer_close(&w,&e),&e);
        read_file(api,path,&limits,0,0);read_file(api,path,&limits,0,1);
    }
    read_file(api,argv[3],&limits,1,0);
    snprintf(path,sizeof(path),"%s/required-elements.parquet",argv[2]);
    OgLakeSdkListField required_success={73,9,0};ok(v4->writer_open(&o,1,&required_success,&w,&e),&e);
    append(api,w,0,1,4);OgLakeSdkWrittenFile required_file={0};required_file.struct_size=sizeof(required_file);
    ok(api->writer_finish(w,&required_file,&e),&e);ok(api->writer_close(&w,&e),&e);
    OgLakeSdkFooter* required_footer=NULL;ok(api->footer_open(path,&limits,&required_footer,&e),&e);
    const OgLakeSdkFooterInfo* required_info=api->footer_info(required_footer);
    check(required_info->struct_size>=OG_LAKE_SDK_FIELD_END(OgLakeSdkFooterInfo,list_fields)&&
        required_info->num_list_fields==1&&required_info->list_fields[0].element_nullable==0,
        "physical required elements are not reported as nullable");api->footer_close(&required_footer);
    snprintf(path,sizeof(path),"%s/reject.parquet",argv[2]);
    check(api->writer_open(&o,&w,&e)==OG_LAKE_SDK_UNSUPPORTED&&!w&&access(path,F_OK)!=0,"ABI1 cannot invent a child ID");
    OgLakeSdkListField collision={73,11,1};check(v4->writer_open(&o,1,&collision,&w,&e)==OG_LAKE_SDK_INVALID&&!w&&access(path,F_OK)!=0,"root/child ID collision rejected");
    OgLakeSdkListField orphan={74,9,1};check(v4->writer_open(&o,1,&orphan,&w,&e)==OG_LAKE_SDK_UNSUPPORTED&&!w&&access(path,F_OK)!=0,"orphan descriptor rejected");
    OgLakeSdkListField duplicate[]={list,list};check(v4->writer_open(&o,2,duplicate,&w,&e)==OG_LAKE_SDK_INVALID&&!w,"duplicate list descriptor rejected");
    for(int bad=1;bad<=2;++bad){ok(v4->writer_open(&o,1,&list,&w,&e),&e);append(api,w,0,30,bad);ok(api->writer_close(&w,&e),&e);
        check(access(path,F_OK)!=0,"malformed nested candidate discarded");}
    OgLakeSdkListField required={73,9,0};ok(v4->writer_open(&o,1,&required,&w,&e),&e);append(api,w,0,30,3);
    ok(api->writer_close(&w,&e),&e);check(access(path,F_OK)!=0,"NULL forbidden element aborts the unpublished candidate");
    check(v4->writer_open(&o,-1,&list,&w,&e)==OG_LAKE_SDK_INVALID&&!w,"negative descriptor count refused");
    check(v4->writer_open(&o,1,NULL,&w,&e)==OG_LAKE_SDK_INVALID&&!w,"missing descriptor vector refused");
    int stop=1;o.limits.should_cancel=cancel;o.limits.cancel_context=&stop;
    check(v4->writer_open(&o,1,&list,&w,&e)==OG_LAKE_SDK_CANCELLED&&!w&&access(path,F_OK)!=0,"nested writer cancellation creates no candidate");
    check(fds()==before,"all owned nested descriptors released");
    printf("{\"status\":\"pass\",\"checks\":%lu,\"files\":3,\"rows\":8193}\n",checks);return 0;
}
