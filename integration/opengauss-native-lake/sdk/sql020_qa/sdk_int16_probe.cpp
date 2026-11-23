/* Prepared standalone SDK C consumer. Not a Native backend test. */
#include "lake_parquet_sdk.h"
#include "vendor/arrow20/abi.h"
#include <cstdint>
#include <cstring>
#include <dlfcn.h>
#include <iostream>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <vector>
static void Check(bool x,const char* s){if(!x)throw std::runtime_error(s);}
static void ArrayRelease(ArrowArray* a){a->release=nullptr;}
static void SchemaRelease(ArrowSchema* s){s->release=nullptr;}
int main(int argc,char** argv)
{
    if(argc<3||argc>4){std::cerr<<"provider.so /absolute/new-owned.parquet [--expect-missing]\n";return 2;}
    void* library=nullptr;const OgLakeParquetSdkApi* api=nullptr;
    const OgLakeParquetLifecycleApi* life=nullptr;OgLakeSdkWriter* writer=nullptr;
    OgLakeSdkReader* reader=nullptr;OgLakeSdkFooter* footer=nullptr;OgLakeSdkBatch* batch=nullptr;
    OgLakeSdkError error{};bool keep=false,passed=false;uint64_t checked=0;
    try{
        Check(argv[2][0]=='/',"absolute new path required");struct stat st{};
        Check(::lstat(argv[2],&st)!=0,"refuse an existing path");
        library=::dlopen(argv[1],RTLD_NOW|RTLD_LOCAL);Check(library,"cannot load exact provider");
        auto entry=reinterpret_cast<OgLakeParquetSdkEntrypoint>(::dlsym(library,"og_lake_parquet_sdk_v1"));
        Check(entry,"missing stable C ABI1 entry");api=entry();Check(api&&api->abi_version==1&&api->struct_size>=sizeof(*api),"invalid API1");
        const bool missing=(api->capabilities&OG_LAKE_SDK_INT16_VALUES)==0;
        if(argc==4){Check(std::strcmp(argv[3],"--expect-missing")==0&&missing,"old provider must explicitly lack capability");
            Check(::lstat(argv[2],&st)!=0,"capability refusal created no output");
            std::cout<<"{\"status\":\"pass_old_capability_refusal\",\"file_calls\":0}\n";::dlclose(library);return 0;}
        Check(!missing,"INT16 not advertised; caller refuses before SDK file operations");
        auto lifecycle=reinterpret_cast<OgLakeParquetLifecycleEntrypoint>(::dlsym(library,"og_lake_parquet_sdk_v2"));
        Check(lifecycle,"missing candidate cleanup API");life=lifecycle();Check(life&&life->base==api&&life->writer_abort,"invalid lifecycle API");
        constexpr int64_t rows=65536,pad=5;std::vector<int16_t> data(rows+pad);
        std::vector<uint8_t> valid((rows+pad+7)/8,255);int64_t nulls=0;
        for(int64_t i=0;i<rows;++i){data[i+pad]=static_cast<int16_t>(i-32768);
            if(i%17==0){valid[(i+pad)/8]&=static_cast<uint8_t>(~(1u<<((i+pad)%8)));++nulls;}}
        ArrowArray root{},values{},nullable{};ArrowSchema schema{},valueSchema{},nullableSchema{};
        const void* rootBuffers[]={nullptr};const void* valueBuffers[]={nullptr,data.data()};
        const void* nullableBuffers[]={valid.data(),data.data()};ArrowArray* children[]={&values,&nullable};
        ArrowSchema* fields[]={&valueSchema,&nullableSchema};
        root.length=rows;root.n_buffers=1;root.buffers=rootBuffers;root.n_children=2;root.children=children;root.release=ArrayRelease;
        values.length=nullable.length=rows;values.offset=nullable.offset=pad;values.n_buffers=nullable.n_buffers=2;
        values.buffers=valueBuffers;nullable.buffers=nullableBuffers;nullable.null_count=nulls;values.release=nullable.release=ArrayRelease;
        schema.format="+s";schema.n_children=2;schema.children=fields;schema.release=SchemaRelease;
        valueSchema.format=nullableSchema.format="s";valueSchema.name="whole";nullableSchema.name="maybe";
        nullableSchema.flags=ARROW_FLAG_NULLABLE;valueSchema.release=nullableSchema.release=SchemaRelease;
        OgLakeSdkField columns[]={{73,OG_LAKE_SDK_INT16,0,"whole"},{9,OG_LAKE_SDK_INT16,1,"maybe"}};
        OgLakeSdkWriteOptions options{};options.struct_size=sizeof(options);options.path=argv[2];options.num_fields=2;options.fields=columns;
        options.compression=OG_LAKE_SDK_ZSTD;options.row_group_rows=8192;options.limits.struct_size=sizeof(options.limits);
        options.limits.memory_bytes=64*1024*1024;options.limits.footer_bytes=8*1024*1024;
        Check(api->writer_open(&options,&writer,&error)==OG_LAKE_SDK_OK,error.message);
        Check(api->writer_append(writer,&root,&schema,&error)==OG_LAKE_SDK_OK,error.message);
        Check(root.release==ArrayRelease&&schema.release==SchemaRelease&&values.release==ArrayRelease,"borrowed callbacks changed");
        OgLakeSdkWrittenFile written{};written.struct_size=sizeof(written);
        Check(api->writer_finish(writer,&written,&error)==OG_LAKE_SDK_OK,error.message);
        Check(written.num_rows==rows&&written.file_bytes>0&&written.local_fsync_complete==1,"bad durable file receipt");
        Check(api->footer_open(argv[2],&options.limits,&footer,&error)==OG_LAKE_SDK_OK,error.message);
        const auto* info=api->footer_info(footer);Check(info&&info->num_rows==rows&&info->num_fields==2,"invalid real footer");
        Check(info->fields[0].field_id==73&&info->fields[1].field_id==9&&info->fields[0].type==OG_LAKE_SDK_INT16&&info->fields[1].type==OG_LAKE_SDK_INT16,"actual signed16 IDs/types changed");
        int64_t projection[]={9,73};OgLakeSdkReadOptions read{};read.struct_size=sizeof(read);read.path=argv[2];read.limits=options.limits;
        read.batch_rows=1000;read.projection_kind=OG_LAKE_SDK_BY_FIELD_ID;read.num_field_ids=2;read.field_ids=projection;read.num_row_groups=-1;
        Check(api->reader_open(&read,&reader,&error)==OG_LAKE_SDK_OK,error.message);int64_t seen=0;
        for(;;){int code=api->reader_next(reader,&batch,&error);if(code==OG_LAKE_SDK_EOF)break;Check(code==OG_LAKE_SDK_OK,error.message);
            Check(batch&&batch->array&&batch->schema&&batch->original_positions&&batch->array->n_children==2&&batch->array->offset==0,"bad actual batch");
            for(int64_t row=0;row<batch->num_rows;++row){const int64_t pos=batch->original_positions[row];Check(pos==seen+row,"original positions changed");
                for(int c=0;c<2;++c){const auto* array=batch->array->children[c];const auto* type=batch->schema->children[c];
                    Check(array&&type&&std::strcmp(type->format,"s")==0&&array->n_buffers==2&&array->buffers&&array->buffers[1],"real Arrow output is not signed16");
                    int64_t index=array->offset+row;bool null=array->buffers[0]&&((static_cast<const uint8_t*>(array->buffers[0])[index/8]>>(index%8))&1)==0;
                    Check(null==(c==0&&pos%17==0),"SQL NULL bitmap changed");
                    if(!null){Check(static_cast<const int16_t*>(array->buffers[1])[index]==pos-32768,"signed16 full-domain value changed");}
                    ++checked;
                }
            }
            seen+=batch->num_rows;api->batch_release(&batch);
        }
        Check(seen==rows,"incomplete reader");OgLakeSdkIoStats stats{};api->reader_stats(reader,&stats);
        Check(stats.peak_pool_bytes<=options.limits.memory_bytes,"reader exceeded declared pool");keep=true;passed=true;
    }catch(const std::exception& e){std::cerr<<e.what()<<"\n";}
    if(api){if(batch)api->batch_release(&batch);if(reader)api->reader_close(&reader);if(footer)api->footer_close(&footer);
        if(writer){int code=keep?api->writer_close(&writer,&error):(life?life->writer_abort(&writer,&error):api->writer_close(&writer,&error));
            if(code!=OG_LAKE_SDK_OK){passed=false;std::cerr<<"cleanup: "<<error.message<<"\n";}}}
    if(library)::dlclose(library);
    std::cout<<"{\"status\":\""<<(passed?"pass":"fail")<<"\",\"checked_cells\":"<<checked<<",\"file_retained_for_independent_reader\":"<<(keep?"true":"false")<<"}\n";
    return passed?0:1;
}
