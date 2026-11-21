/* Apache Arrow20 file provider. Database APIs are deliberately absent. */
#include "lake_parquet_sdk.h"
#include "lake_parquet_fd_sdk.h"
#include "lake_temporal_codec.h"
#include <arrow/api.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/extension_type.h>
#include <arrow/extension/json.h>
#include <arrow/io/api.h>
#include <arrow/util/key_value_metadata.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>
#include <parquet/file_writer.h>
#include <parquet/metadata.h>
#include <parquet/schema.h>
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <climits>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <sys/stat.h>
#include <vector>

namespace {
struct Failure : std::runtime_error {
    int code;
    int system_errno;
    Failure(int c,const char* text,int number=0):std::runtime_error(text),code(c),system_errno(number) {}
};
void SetError(OgLakeSdkError* error,int code,const char* message,int system_errno=0) noexcept
{
    if(!error)return;
    std::memset(error,0,sizeof(*error));error->code=code;error->system_errno=system_errno;
    if(message)std::strncpy(error->message,message,sizeof(error->message)-1);
}
void Require(bool condition,const char* message,int code=OG_LAKE_SDK_INVALID)
{
    if(!condition)throw Failure(code,message);
}
void Check(const arrow::Status& status)
{
    if(status.ok())return;
    int code=status.IsOutOfMemory()?OG_LAKE_SDK_MEMORY:
        status.IsCancelled()?OG_LAKE_SDK_CANCELLED:
        status.IsNotImplemented()?OG_LAKE_SDK_UNSUPPORTED:
        status.IsInvalid()?OG_LAKE_SDK_INVALID:OG_LAKE_SDK_IO;
    throw Failure(code,status.message().c_str());
}
template<typename T>T Take(arrow::Result<T> result)
{
    Check(result.status());return std::move(result).ValueOrDie();
}
template<typename Function>int Guard(OgLakeSdkError* error,Function function) noexcept
{
    SetError(error,OG_LAKE_SDK_OK,nullptr);
    try{return function();}
    catch(const Failure& failure){SetError(error,failure.code,failure.what(),failure.system_errno);return failure.code;}
    catch(const std::bad_alloc&){SetError(error,OG_LAKE_SDK_MEMORY,"file provider allocation failed");return OG_LAKE_SDK_MEMORY;}
    catch(const std::exception& failure){SetError(error,OG_LAKE_SDK_IO,failure.what());return OG_LAKE_SDK_IO;}
    catch(...){SetError(error,OG_LAKE_SDK_STATE,"unknown file provider exception");return OG_LAKE_SDK_STATE;}
}
OgLakeSdkLimits Limits(const OgLakeSdkLimits* input)
{
    Require(input&&input->struct_size>=sizeof(*input),"invalid file provider limits ABI");
    OgLakeSdkLimits limits=*input;
    if(limits.memory_bytes==0)limits.memory_bytes=64*1024*1024;
    if(limits.footer_bytes==0)limits.footer_bytes=16*1024*1024;
    Require(limits.memory_bytes>0&&limits.footer_bytes>0&&limits.footer_bytes<=limits.memory_bytes,
        "invalid file provider memory/footer limit");
    return limits;
}
bool Cancelled(const OgLakeSdkLimits& limits)
{
    return limits.should_cancel&&limits.should_cancel(limits.cancel_context)!=0;
}
void Poll(const OgLakeSdkLimits& limits)
{
    Require(!Cancelled(limits),"file provider operation cancelled",OG_LAKE_SDK_CANCELLED);
}

/* Charges Arrow buffers, including temporary old+new reallocation space.
 * Thrift metadata has separate serialized-size and container/string bounds. */
class BudgetPool final : public arrow::MemoryPool {
    arrow::MemoryPool* underlying_=arrow::system_memory_pool();
    int64_t limit_;
    std::atomic<int64_t> current_{0},peak_{0},total_{0},allocations_{0};
    bool Reserve(int64_t bytes)
    {
        if(bytes<0||bytes>limit_)return false;
        int64_t value=current_.load();
        do{if(value>limit_-bytes)return false;}
        while(!current_.compare_exchange_weak(value,value+bytes));
        int64_t high=peak_.load();
        while(high<value+bytes&&!peak_.compare_exchange_weak(high,value+bytes)){}
        return true;
    }
public:
    explicit BudgetPool(int64_t limit):limit_(limit){}
    arrow::Status Allocate(int64_t bytes,int64_t alignment,uint8_t** out) override
    {
        if(!Reserve(bytes))return arrow::Status::OutOfMemory("file provider Arrow pool budget exceeded");
        auto status=underlying_->Allocate(bytes,alignment,out);
        if(!status.ok())current_.fetch_sub(bytes);
        else{total_.fetch_add(bytes);allocations_.fetch_add(1);}
        return status;
    }
    arrow::Status Reallocate(int64_t old_bytes,int64_t new_bytes,int64_t alignment,uint8_t** out) override
    {
        if(old_bytes<0||!Reserve(new_bytes))return arrow::Status::OutOfMemory("file provider reallocation budget exceeded");
        auto status=underlying_->Reallocate(old_bytes,new_bytes,alignment,out);
        current_.fetch_sub(status.ok()?old_bytes:new_bytes);
        if(status.ok()){total_.fetch_add(new_bytes);allocations_.fetch_add(1);}
        return status;
    }
    void Free(uint8_t* buffer,int64_t bytes,int64_t alignment) override
    {
        underlying_->Free(buffer,bytes,alignment);current_.fetch_sub(bytes);
    }
    int64_t bytes_allocated() const override{return current_.load();}
    int64_t max_memory() const override{return peak_.load();}
    int64_t total_bytes_allocated() const override{return total_.load();}
    int64_t num_allocations() const override{return allocations_.load();}
    std::string backend_name() const override{return "og_lake_bounded_arrow_buffers";}
};

bool SameFile(const struct stat& left,const struct stat& right)
{
    return left.st_dev==right.st_dev&&left.st_ino==right.st_ino&&left.st_size==right.st_size&&
        left.st_mtim.tv_sec==right.st_mtim.tv_sec&&left.st_mtim.tv_nsec==right.st_mtim.tv_nsec&&
        left.st_ctim.tv_sec==right.st_ctim.tv_sec&&left.st_ctim.tv_nsec==right.st_ctim.tv_nsec;
}
class RangeFile final : public arrow::io::RandomAccessFile {
    int fd_=-1;
    int64_t position_=0;
    struct stat identity_{};
    OgLakeSdkLimits limits_;
    std::shared_ptr<BudgetPool> pool_;
public:
    uint64_t read_calls=0,bytes_read=0;
    RangeFile(const char* path,OgLakeSdkLimits limits,std::shared_ptr<BudgetPool> pool):limits_(limits),pool_(std::move(pool))
    {
        Require(path&&path[0]=='/'&&std::strlen(path)<PATH_MAX,"file provider requires a bounded absolute local path");
        Poll(limits_);fd_=::open(path,O_RDONLY|O_CLOEXEC|O_NOFOLLOW);
        if(fd_<0)throw Failure(OG_LAKE_SDK_IO,"cannot open local Parquet object",errno);
        if(::fstat(fd_,&identity_)!=0||!S_ISREG(identity_.st_mode)){
            int number=errno;::close(fd_);fd_=-1;throw Failure(OG_LAKE_SDK_IO,"Parquet object is not a regular local file",number);
        }
    }
    RangeFile(int source,OgLakeSdkLimits limits,std::shared_ptr<BudgetPool> pool):limits_(limits),pool_(std::move(pool))
    {
        Require(source>=0,"invalid borrowed Parquet descriptor");
        Poll(limits_);
        do{fd_=::fcntl(source,F_DUPFD_CLOEXEC,0);}while(fd_<0&&errno==EINTR);
        if(fd_<0)throw Failure(OG_LAKE_SDK_IO,"cannot duplicate Parquet descriptor",errno);
        try {
            const int flags=::fcntl(fd_,F_GETFL);
            if(flags<0)throw Failure(OG_LAKE_SDK_IO,"cannot inspect Parquet descriptor permissions",errno);
            Require((flags&O_ACCMODE)==O_RDONLY&&(flags&O_PATH)==0,"Parquet descriptor must be readable and read-only");
            if(::fstat(fd_,&identity_)!=0)throw Failure(OG_LAKE_SDK_IO,"cannot inspect Parquet descriptor identity",errno);
            Require(S_ISREG(identity_.st_mode),"Parquet descriptor is not a regular file");
            Poll(limits_);
        } catch(...) {::close(fd_);fd_=-1;throw;}
    }
    ~RangeFile() override{(void)Close();}
    arrow::Status Close() override
    {
        if(fd_>=0){int fd=fd_;fd_=-1;(void)::close(fd);}return arrow::Status::OK();
    }
    bool closed() const override{return fd_<0;}
    arrow::Result<int64_t> Tell() const override{return position_;}
    arrow::Status Seek(int64_t position) override
    {
        if(position<0)return arrow::Status::Invalid("negative local seek");
        position_=position;return arrow::Status::OK();
    }
    arrow::Result<int64_t> GetSize() override{return identity_.st_size;}
    std::string ObjectVersion() const
    {
        return "local-stat-v1:"+std::to_string((uint64_t)identity_.st_dev)+":"+
            std::to_string((uint64_t)identity_.st_ino)+":"+std::to_string(identity_.st_size)+":"+
            std::to_string(identity_.st_mtim.tv_sec)+":"+std::to_string(identity_.st_mtim.tv_nsec)+":"+
            std::to_string(identity_.st_ctim.tv_sec)+":"+std::to_string(identity_.st_ctim.tv_nsec);
    }
    arrow::Result<int64_t> ReadAt(int64_t position,int64_t bytes,void* output) override
    {
        if(fd_<0)return arrow::Status::Invalid("local Parquet reader is closed");
        if(position<0||bytes<0||(bytes&&!output))return arrow::Status::Invalid("invalid local range read");
        if(Cancelled(limits_))return arrow::Status::Cancelled("local Parquet read cancelled");
        struct stat current;
        if(::fstat(fd_,&current)!=0||!SameFile(identity_,current))return arrow::Status::IOError("immutable Parquet object changed");
        int64_t wanted=position>=identity_.st_size?0:std::min<int64_t>(bytes,identity_.st_size-position);
        int64_t done=0;
        while(done<wanted){
            if(Cancelled(limits_))return arrow::Status::Cancelled("local Parquet read cancelled");
            int64_t chunk=std::min<int64_t>(65536,wanted-done);++read_calls;
            ssize_t count=::pread(fd_,static_cast<char*>(output)+done,static_cast<size_t>(chunk),position+done);
            if(count<0){if(errno==EINTR)continue;return arrow::Status::IOError("local Parquet pread failed");}
            if(count==0)return arrow::Status::IOError("immutable Parquet object was truncated");
            done+=count;bytes_read+=static_cast<uint64_t>(count);
        }
        return done;
    }
    arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position,int64_t bytes) override
    {
        if(position<0||bytes<0)return arrow::Status::Invalid("invalid local buffered range");
        int64_t wanted=position>=identity_.st_size?0:std::min<int64_t>(bytes,identity_.st_size-position);
        ARROW_ASSIGN_OR_RAISE(auto buffer,arrow::AllocateResizableBuffer(wanted,pool_.get()));
        ARROW_ASSIGN_OR_RAISE(auto count,ReadAt(position,wanted,buffer->mutable_data()));
        if(count!=wanted)return arrow::Status::IOError("incomplete local Parquet range");
        return std::shared_ptr<arrow::Buffer>(std::move(buffer));
    }
    arrow::Result<int64_t> Read(int64_t bytes,void* output) override
    {
        ARROW_ASSIGN_OR_RAISE(auto count,ReadAt(position_,bytes,output));position_+=count;return count;
    }
    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t bytes) override
    {
        ARROW_ASSIGN_OR_RAISE(auto buffer,ReadAt(position_,bytes));position_+=buffer->size();return buffer;
    }
};
uint32_t FieldType(const std::shared_ptr<arrow::DataType>& type)
{
    switch(type->id()){
        case arrow::Type::BOOL:return OG_LAKE_SDK_BOOL;
        case arrow::Type::INT16:return OG_LAKE_SDK_INT16;
        case arrow::Type::INT32:return OG_LAKE_SDK_INT32;
        case arrow::Type::INT64:return OG_LAKE_SDK_INT64;
        case arrow::Type::FLOAT:return OG_LAKE_SDK_FLOAT32;
        case arrow::Type::DOUBLE:return OG_LAKE_SDK_FLOAT64;
        case arrow::Type::DATE32:return OG_LAKE_SDK_DATE32;
        case arrow::Type::TIMESTAMP:{
            const auto& timestamp=static_cast<const arrow::TimestampType&>(*type);
            if(timestamp.unit()!=arrow::TimeUnit::MICRO)return OG_LAKE_SDK_OTHER;
            if(timestamp.timezone().empty())return OG_LAKE_SDK_TIMESTAMP_US;
            return timestamp.timezone()=="UTC"?OG_LAKE_SDK_TIMESTAMPTZ_US:OG_LAKE_SDK_OTHER;
        }
        case arrow::Type::LIST:
            return type->field(0)->type()->id()==arrow::Type::FLOAT?
                OG_LAKE_SDK_FLOAT32_LIST:OG_LAKE_SDK_OTHER;
        case arrow::Type::STRING:case arrow::Type::LARGE_STRING:return OG_LAKE_SDK_UTF8;
        case arrow::Type::BINARY:case arrow::Type::LARGE_BINARY:return OG_LAKE_SDK_BINARY;
        default:return OG_LAKE_SDK_OTHER;
    }
}
/* Fixed16 alone is deliberately not classified by FieldType: UUID meaning
 * requires the independent physical/logical footer proof below. Canonical
 * arrow.uuid metadata may restore an extension, but cannot grant that proof. */
bool UuidStorageType(const std::shared_ptr<arrow::DataType>& type)
{
    const arrow::DataType* storage=type.get();
    if(storage->id()==arrow::Type::EXTENSION){
        const auto& extension=static_cast<const arrow::ExtensionType&>(*storage);
        if(extension.extension_name()!="arrow.uuid")return false;
        storage=extension.storage_type().get();
    }
    return storage->id()==arrow::Type::FIXED_SIZE_BINARY&&
        static_cast<const arrow::FixedSizeBinaryType&>(*storage).byte_width()==16;
}
/* Only the standard UTF8 storage is admitted for JSON in this slice. */
bool JsonStorageType(const std::shared_ptr<arrow::DataType>& type)
{
    const arrow::DataType* storage=type.get();
    if(storage->id()==arrow::Type::EXTENSION){
        const auto& extension=static_cast<const arrow::ExtensionType&>(*storage);
        if(extension.extension_name()!="arrow.json")return false;
        storage=extension.storage_type().get();
    }
    return storage->id()==arrow::Type::STRING;
}
/* Do not infer decimal semantics from ARROW:schema alone. Decimal256
 * restored over a physical precision<=38 remains outside this codec. */
bool Decimal128Parameters(const std::shared_ptr<arrow::DataType>& type,int* precision,int* scale)
{
    if(type->id()!=arrow::Type::DECIMAL128)return false;
    const auto& decimal=static_cast<const arrow::Decimal128Type&>(*type);
    const int p=decimal.precision(),s=decimal.scale();
    if(p<1||p>38||s<0||s>p)return false;
    *precision=p;*scale=s;return true;
}
bool Decimal128Physical(const std::shared_ptr<arrow::DataType>& type,
    const parquet::ColumnDescriptor* column)
{
    int p=0,s=0;const auto& logical=column->logical_type();
    if(!Decimal128Parameters(type,&p,&s)||!logical||!logical->is_decimal())return false;
    const auto& decimal=static_cast<const parquet::DecimalLogicalType&>(*logical);
    if(decimal.precision()!=p||decimal.scale()!=s)return false;
    switch(column->physical_type()){
        case parquet::Type::INT32:return p<=9;
        case parquet::Type::INT64:return p<=18;
        case parquet::Type::FIXED_LEN_BYTE_ARRAY:{
            /* floor(log10(2^(8*n-1)-1)), the standard signed FLBA capacity. */
            static const int capacity[17]={0,2,4,6,9,11,14,16,18,21,23,26,28,31,33,35,38};
            int n=column->type_length();return n>=1&&n<=16&&p<=capacity[n];
        }
        default:return false;
    }
}
uint32_t ParquetFieldType(const std::shared_ptr<arrow::DataType>& type,
    const parquet::ColumnDescriptor* column)
{
    const uint32_t mapped=FieldType(type);
    const auto& logical=column->logical_type();
    if((logical&&logical->is_decimal())||type->id()==arrow::Type::DECIMAL128||
       type->id()==arrow::Type::DECIMAL32||type->id()==arrow::Type::DECIMAL64||
       type->id()==arrow::Type::DECIMAL256)
        return Decimal128Physical(type,column)?static_cast<uint32_t>(OG_LAKE_SDK_DECIMAL128):
            static_cast<uint32_t>(OG_LAKE_SDK_OTHER);
    if((logical&&logical->is_JSON())||
       (type->id()==arrow::Type::EXTENSION&&
        static_cast<const arrow::ExtensionType&>(*type).extension_name()=="arrow.json"))
        return logical&&logical->is_JSON()&&
            column->physical_type()==parquet::Type::BYTE_ARRAY&&JsonStorageType(type)?
            static_cast<uint32_t>(OG_LAKE_SDK_JSON):static_cast<uint32_t>(OG_LAKE_SDK_OTHER);
    if((logical&&logical->is_UUID())||
       (type->id()==arrow::Type::EXTENSION&&
        static_cast<const arrow::ExtensionType&>(*type).extension_name()=="arrow.uuid"))
        return logical&&logical->is_UUID()&&
            column->physical_type()==parquet::Type::FIXED_LEN_BYTE_ARRAY&&
            column->type_length()==16&&UuidStorageType(type)?
            static_cast<uint32_t>(OG_LAKE_SDK_UUID):static_cast<uint32_t>(OG_LAKE_SDK_OTHER);
    /* Restored ARROW:schema metadata must not disguise a DATE as a plain int,
     * change UTC semantics or reinterpret an INT96/nanosecond physical field.
     * Keep unsupported fields inspectable but reject their value projection. */
    if(mapped==OG_LAKE_SDK_DATE32||(logical&&logical->is_date()))
        return mapped==OG_LAKE_SDK_DATE32&&column->physical_type()==parquet::Type::INT32&&
            logical&&logical->is_date()?mapped:static_cast<uint32_t>(OG_LAKE_SDK_OTHER);
    if(mapped==OG_LAKE_SDK_TIMESTAMP_US||mapped==OG_LAKE_SDK_TIMESTAMPTZ_US||
       (logical&&logical->is_timestamp())) {
        if((mapped!=OG_LAKE_SDK_TIMESTAMP_US&&mapped!=OG_LAKE_SDK_TIMESTAMPTZ_US)||
           column->physical_type()!=parquet::Type::INT64||!logical||!logical->is_timestamp())return OG_LAKE_SDK_OTHER;
        const auto& timestamp=static_cast<const parquet::TimestampLogicalType&>(*logical);
        return timestamp.time_unit()==parquet::LogicalType::TimeUnit::MICROS&&
            timestamp.is_adjusted_to_utc()==(mapped==OG_LAKE_SDK_TIMESTAMPTZ_US)?mapped:static_cast<uint32_t>(OG_LAKE_SDK_OTHER);
    }
    return mapped;
}
void ValidateDecimal128Values(const std::shared_ptr<arrow::Array>& values,
    int precision,int scale,const OgLakeSdkLimits& limits)
{
    Poll(limits);int p=0,s=0;
    Require(Decimal128Parameters(values->type(),&p,&s)&&p==precision&&s==scale,
        "decimal128 Arrow precision/scale differs from physical declaration",OG_LAKE_SDK_UNSUPPORTED);
    const auto& data=*values->data();
    Require(data.offset>=0&&data.length>=0&&data.buffers.size()==2&&
        data.offset<=INT64_MAX-data.length&&data.offset+data.length<=INT64_MAX/16,
        "decimal128 Arrow buffer shape or byte offset is invalid");
    const int64_t bytes=data.length==0?0:(data.offset+data.length)*16;
    Require(bytes==0||(data.buffers[1]&&data.buffers[1]->size()>=bytes),
        "decimal128 data buffer is shorter than its declared rows");
    /* ValidateFull proves Arrow structure. Its pinned decimal Abs-based
     * precision check does not reject INT128_MIN: negating MIN wraps to MIN.
     * Compare signed coefficients directly against the representable +/-max;
     * never take Abs or negate an untrusted coefficient. */
    Check(values->ValidateFull());
    const arrow::BasicDecimal128 maximum=arrow::BasicDecimal128::GetMaxValue(precision);
    arrow::BasicDecimal128 minimum=maximum;minimum.Negate();
    for(int64_t row=0;row<data.length;++row){
        if((row&1023)==0)Poll(limits);
        if(values->IsNull(row))continue;
        const arrow::BasicDecimal128 coefficient(data.buffers[1]->data()+16*(data.offset+row));
        Require(!(coefficient<minimum)&&!(maximum<coefficient),
            "decimal128 coefficient exceeds declared precision");
    }
    Poll(limits);
}
void ValidateUuidValues(const std::shared_ptr<arrow::Array>& values,const OgLakeSdkLimits& limits)
{
    Poll(limits);
    Require(UuidStorageType(values->type()),"UUID requires Arrow fixed-size binary16",OG_LAKE_SDK_UNSUPPORTED);
    auto storage=values;
    if(values->type_id()==arrow::Type::EXTENSION)
        storage=static_cast<const arrow::ExtensionArray&>(*values).storage();
    Require(storage->length()==values->length()&&storage->offset()==values->offset(),
        "UUID extension storage offset/length differs");
    const auto& data=*storage->data();
    Require(data.offset>=0&&data.length>=0&&data.buffers.size()==2&&
        data.offset<=INT64_MAX-data.length&&data.offset+data.length<=INT64_MAX/16,
        "UUID Arrow buffer shape or byte offset is invalid");
    Check(storage->ValidateFull());
    int64_t bytes=data.length==0?0:(data.offset+data.length)*16;
    Require(bytes==0||(data.buffers[1]&&data.buffers[1]->size()>=bytes),
        "UUID Arrow data buffer is shorter than its rows");
    /* Every16-byte value is valid. The C producer owns actual input capacity;
     * ImportRecordBatch/ValidateFull prove structure, not inaccessible memory. */
}
void ValidateJsonValues(const std::shared_ptr<arrow::Array>& values,const OgLakeSdkLimits& limits)
{
    Poll(limits);
    Require(JsonStorageType(values->type()),"JSON requires Arrow UTF8 storage",OG_LAKE_SDK_UNSUPPORTED);
    auto storage=values;
    if(values->type_id()==arrow::Type::EXTENSION)
        storage=static_cast<const arrow::ExtensionArray&>(*values).storage();
    Require(storage->length()==values->length()&&storage->offset()==values->offset(),
        "JSON extension storage offset/length differs");
    const auto& data=*storage->data();
    Require(data.offset>=0&&data.length>=0&&data.buffers.size()==3&&
        data.offset<=INT32_MAX-1&&data.length<=INT32_MAX-1-data.offset,
        "JSON Arrow buffer shape or offsets index is invalid");
    Check(storage->ValidateFull()); /* Includes all StringArray UTF8 and offset checks. */
    const auto& strings=static_cast<const arrow::StringArray&>(*storage);
    int64_t bytes=0;
    for(int64_t row=0;row<strings.length();++row){
        if((row&1023)==0)Poll(limits);
        if(strings.IsNull(row))continue;
        auto value=strings.GetView(row);
        Require(static_cast<uint64_t>(value.size())<=static_cast<uint64_t>(limits.memory_bytes-bytes),
            "JSON borrowed/decoded value bytes exceed provider budget",OG_LAKE_SDK_MEMORY);
        bytes+=static_cast<int64_t>(value.size());
        Require(value.empty()||std::memchr(value.data(),0,value.size())==nullptr,
            "JSON transport refuses raw NUL bytes");
        /* Do not parse, canonicalize, decode escapes or convert numeric text.
         * The host must invoke strict json_in with can_ignore=false. */
    }
}
void ValidateTemporalValues(const std::shared_ptr<arrow::RecordBatch>& batch,const OgLakeSdkLimits& limits)
{
    for(int column=0;column<batch->num_columns();++column) {
        const auto& values=batch->column(column);
        uint32_t type=FieldType(values->type());
        if(type!=OG_LAKE_SDK_DATE32&&type!=OG_LAKE_SDK_TIMESTAMP_US&&type!=OG_LAKE_SDK_TIMESTAMPTZ_US)continue;
        for(int64_t row=0;row<values->length();++row) {
            if((row&1023)==0)Poll(limits);
            if(values->IsNull(row))continue;
            bool finite=type==OG_LAKE_SDK_DATE32?
                OgLakeTemporalFiniteDate32(static_cast<const arrow::Date32Array&>(*values).Value(row)):
                OgLakeTemporalFiniteMicros(static_cast<const arrow::TimestampArray&>(*values).Value(row));
            Require(finite,"temporal values require finite DATE32 or microsecond timestamps",OG_LAKE_SDK_UNSUPPORTED);
        }
    }
}
} // namespace

struct OgLakeSdkFooter {
    std::shared_ptr<BudgetPool> pool;
    std::shared_ptr<RangeFile> file;
    std::unique_ptr<parquet::arrow::FileReader> reader;
    std::shared_ptr<arrow::Schema> schema;
    std::shared_ptr<parquet::FileMetaData> metadata;
    std::vector<std::string> names;
    std::vector<OgLakeSdkField> fields;
    std::vector<OgLakeSdkListField> list_fields;
    std::vector<std::vector<OgLakeSdkColumnChunk>> chunks;
    std::vector<OgLakeSdkRowGroup> groups;
    std::string object_version;
    OgLakeSdkFooterInfo info{};
};

namespace {
std::unique_ptr<OgLakeSdkFooter> OpenFooter(const char* path,const OgLakeSdkLimits& limits,
    const int* source_fd=nullptr,const char* expected_version=nullptr)
{
    auto footer=std::make_unique<OgLakeSdkFooter>();
    footer->pool=std::make_shared<BudgetPool>(limits.memory_bytes);
    footer->file=source_fd?std::make_shared<RangeFile>(*source_fd,limits,footer->pool):
        std::make_shared<RangeFile>(path,limits,footer->pool);
    if(expected_version)
        Require(footer->file->ObjectVersion()==expected_version,"Parquet descriptor does not match the inspected object",OG_LAKE_SDK_STATE);
    int64_t size=Take(footer->file->GetSize());
    Require(size>=12,"Parquet object is too short");
    auto head=Take(footer->file->ReadAt(0,4));auto tail=Take(footer->file->ReadAt(size-8,8));
    Require(std::memcmp(head->data(),"PAR1",4)==0&&std::memcmp(tail->data()+4,"PAR1",4)==0,
        "unsupported or malformed Parquet magic");
    const uint8_t* p=tail->data();
    uint64_t metadata_bytes=static_cast<uint64_t>(p[0])|(static_cast<uint64_t>(p[1])<<8)|
        (static_cast<uint64_t>(p[2])<<16)|(static_cast<uint64_t>(p[3])<<24);
    Require(metadata_bytes<=static_cast<uint64_t>(limits.footer_bytes)&&metadata_bytes<=static_cast<uint64_t>(size-12),
        "Parquet footer exceeds file or metadata limit");
    parquet::ReaderProperties properties(footer->pool.get());
    properties.enable_buffered_stream();properties.set_buffer_size(65536);
    properties.set_thrift_string_size_limit(static_cast<int32_t>(std::min<int64_t>(limits.footer_bytes,1048576)));
    properties.set_thrift_container_size_limit(16384);properties.set_page_checksum_verification(true);
    parquet::ArrowReaderProperties arrow_properties;
    arrow_properties.set_use_threads(false);arrow_properties.set_pre_buffer(false);
    parquet::arrow::FileReaderBuilder builder;
    Check(builder.Open(footer->file,properties));builder.memory_pool(footer->pool.get());builder.properties(arrow_properties);
    Check(builder.Build(&footer->reader));Check(footer->reader->GetSchema(&footer->schema));
    footer->metadata=footer->reader->parquet_reader()->metadata();
    int columns=footer->metadata->num_columns(),groups=footer->metadata->num_row_groups();
    Require(columns>0&&columns<=4096&&groups>=0&&groups<=16384,"Parquet schema/row-group count exceeds provider limits");
    Require(footer->schema->num_fields()==columns,
        "only one physical leaf per top-level Parquet field is admitted",OG_LAKE_SDK_UNSUPPORTED);
    footer->names.reserve(columns);footer->fields.resize(columns);
    std::set<int64_t> known_ids;
    for(int column=0;column<columns;++column){
        auto field=footer->schema->field(column);
        Require(field->type()->num_fields()==0||FieldType(field->type())==OG_LAKE_SDK_FLOAT32_LIST,
            "nested Parquet field is not admitted",OG_LAKE_SDK_UNSUPPORTED);
        /* A LIST's logical ID belongs to the root group, not its FLOAT leaf.
         * The admitted one-leaf layouts retain exact top-level/leaf ordinal
         * correspondence for chunk statistics and projected range reads. */
        int64_t id=footer->metadata->schema()->group_node()->field(column)->field_id();
        if(id>=0)Require(known_ids.insert(id).second,"duplicate Parquet field_id");
        if(FieldType(field->type())==OG_LAKE_SDK_FLOAT32_LIST){
            int64_t child_id=footer->metadata->schema()->Column(column)->schema_node()->field_id();
            if(child_id>=0)Require(known_ids.insert(child_id).second,"duplicate Parquet list element field_id");
            footer->list_fields.push_back({id,child_id,field->type()->field(0)->nullable()?1u:0u});
        }
        footer->names.push_back(field->name());
        uint32_t type=ParquetFieldType(field->type(),footer->metadata->schema()->Column(column));
        /* Unknown Arrow extensions may deserialize as plain storage while
         * retaining their metadata. Physical JSON does not authorize those
         * extra semantics or export a noncanonical extension to a consumer. */
        if((type==OG_LAKE_SDK_JSON||type==OG_LAKE_SDK_DECIMAL128)&&field->metadata())
            for(int i=0;i<field->metadata()->size();++i)
                if(field->metadata()->key(i)==arrow::kExtensionTypeKeyName&&
                   (type==OG_LAKE_SDK_DECIMAL128||
                    field->metadata()->value(i)!="arrow.json"))type=OG_LAKE_SDK_OTHER;
        footer->fields[column]={id,type,field->nullable()?1u:0u,nullptr};
    }
    for(int column=0;column<columns;++column)footer->fields[column].name=footer->names[column].c_str();
    footer->chunks.resize(groups);footer->groups.resize(groups);int64_t first=0;
    for(int group=0;group<groups;++group){
        auto metadata=footer->metadata->RowGroup(group);int64_t rows=metadata->num_rows();
        Require(rows>=0&&rows<=INT64_MAX-first&&metadata->num_columns()==columns,"invalid Parquet row-group population");
        footer->chunks[group].resize(columns);
        for(int column=0;column<columns;++column){
            auto chunk=metadata->ColumnChunk(column);
            Require(chunk->file_path().empty(),"external Parquet column chunks are not admitted",OG_LAKE_SDK_UNSUPPORTED);
            int64_t offset=chunk->data_page_offset();
            if(chunk->dictionary_page_offset()>0)offset=std::min(offset,chunk->dictionary_page_offset());
            int64_t compressed=chunk->total_compressed_size(),uncompressed=chunk->total_uncompressed_size();
            Require(offset>=0&&compressed>=0&&uncompressed>=0&&offset<=size&&compressed<=size-offset,
                "Parquet column chunk exceeds its immutable object");
            footer->chunks[group][column]={offset,compressed,uncompressed};
        }
        footer->groups[group]={first,rows,footer->chunks[group].data()};first+=rows;
    }
    Require(first==footer->metadata->num_rows(),"Parquet footer row populations disagree");
    Require(footer->metadata->size()>0&&footer->metadata->size()<=size-8,"invalid original serialized Parquet footer length");
    footer->object_version=footer->file->ObjectVersion();
    footer->info={sizeof(OgLakeSdkFooterInfo),columns,groups,first,size,footer->fields.data(),footer->groups.data(),
        static_cast<int64_t>(footer->metadata->size()),footer->object_version.c_str(),
        static_cast<int32_t>(footer->list_fields.size()),footer->list_fields.data()};
    Poll(limits);return footer;
}
int FooterDecimal(const OgLakeSdkFooter* footer,int32_t ordinal,
    OgLakeSdkDecimalField* output,OgLakeSdkError* error)
{
    return Guard(error,[&]{
        Require(output&&output->struct_size>=sizeof(*output),"invalid decimal parameter output ABI");
        *output={sizeof(*output),-1,0,0};
        Require(footer&&ordinal>=0&&ordinal<footer->info.num_fields,"decimal field ordinal is out of range");
        Require(footer->fields[ordinal].type==OG_LAKE_SDK_DECIMAL128,
            "field has no admitted physical decimal128 parameters",OG_LAKE_SDK_UNSUPPORTED);
        int p=0,s=0;
        Require(Decimal128Parameters(footer->schema->field(ordinal)->type(),&p,&s),
            "physical decimal parameters are unavailable",OG_LAKE_SDK_UNSUPPORTED);
        *output={sizeof(*output),ordinal,p,s};return OG_LAKE_SDK_OK;
    });
}
int FooterOpen(const char* path,const OgLakeSdkLimits* input,OgLakeSdkFooter** output,OgLakeSdkError* error)
{
    if(output)*output=nullptr;
    return Guard(error,[&]{Require(output,"footer output pointer is required");*output=OpenFooter(path,Limits(input)).release();return OG_LAKE_SDK_OK;});
}
int FooterOpenFd(int fd,const OgLakeSdkLimits* input,OgLakeSdkFooter** output,OgLakeSdkError* error)
{
    if(output)*output=nullptr;
    return Guard(error,[&]{Require(output,"footer output pointer is required");
        *output=OpenFooter(nullptr,Limits(input),&fd).release();return OG_LAKE_SDK_OK;});
}
const OgLakeSdkFooterInfo* FooterInfo(const OgLakeSdkFooter* footer){return footer?&footer->info:nullptr;}
void FooterClose(OgLakeSdkFooter** footer) noexcept
{
    if(footer){delete *footer;*footer=nullptr;}
}
struct BatchLease {std::atomic<bool> outstanding{false};};
struct BatchOwner {
    std::shared_ptr<BudgetPool> pool;
    std::shared_ptr<BatchLease> lease;
    std::unique_ptr<arrow::Buffer> positions;
    ArrowArray array{};
    ArrowSchema schema{};
    OgLakeSdkBatch view{};
    std::string object_version;
    ~BatchOwner(){
        if(array.release)array.release(&array);
        if(schema.release)schema.release(&schema);
        if(lease)lease->outstanding.store(false);
    }
};
} // namespace

struct OgLakeSdkReader {
    OgLakeSdkLimits limits{};
    std::unique_ptr<OgLakeSdkFooter> footer;
    std::unique_ptr<arrow::RecordBatchReader> batches;
    std::shared_ptr<arrow::Schema> projected_schema;
    std::shared_ptr<BatchLease> lease=std::make_shared<BatchLease>();
    std::vector<int> selected_groups;
    std::vector<int> selected_fields;
    std::thread::id owner=std::this_thread::get_id();
    int64_t batch_rows=0,decode_batch_rows=0,remaining=0,group_offset=0;
    size_t group_index=0;
    uint64_t decoded_rows=0;
    bool failed=false;
};

namespace {
int ReaderOpenImpl(const OgLakeSdkReadOptions* options,OgLakeSdkReader** output,OgLakeSdkError* error,
    const int* source_fd,const char* expected_version)
{
    if(output)*output=nullptr;
    return Guard(error,[&]{
        Require(output&&options&&options->struct_size>=sizeof(*options),"invalid reader options/output ABI");
        if(source_fd)
            Require(!options->path&&expected_version&&expected_version[0]&&::strnlen(expected_version,256)<256,
                "descriptor reader requires a null path and the bounded footer object version");
        Require(options->batch_rows>0&&options->batch_rows<=1048576,"invalid Parquet batch row limit");
        Require(options->num_field_ids>=-1&&options->num_row_groups>=-1,"invalid Parquet projection/selection count");
        Require(options->projection_kind==OG_LAKE_SDK_BY_FIELD_ID||options->projection_kind==OG_LAKE_SDK_BY_ORDINAL,
            "unknown Parquet projection mode");
        Require(options->num_field_ids<=0||options->field_ids,"missing Parquet projection vector");
        Require(options->num_row_groups<=0||options->row_groups,"missing Parquet row-group vector");
        auto reader=std::make_unique<OgLakeSdkReader>();reader->limits=Limits(&options->limits);
        reader->batch_rows=options->batch_rows;reader->decode_batch_rows=options->batch_rows;
        reader->footer=OpenFooter(options->path,reader->limits,source_fd,expected_version);
        auto& footer=*reader->footer;std::set<int> seen;
        int num_fields=options->num_field_ids<0?footer.info.num_fields:options->num_field_ids;
        Require(num_fields<=footer.info.num_fields,"too many projected Parquet fields");
        std::vector<std::shared_ptr<arrow::Field>> fields;
        for(int i=0;i<num_fields;++i){
            int column=-1;
            if(options->num_field_ids<0)column=i;
            else if(options->projection_kind==OG_LAKE_SDK_BY_ORDINAL){
                Require(options->field_ids[i]>=0&&options->field_ids[i]<footer.info.num_fields,"Parquet ordinal is out of range");
                column=static_cast<int>(options->field_ids[i]);
            }else{
                Require(options->field_ids[i]>=0&&options->field_ids[i]<=INT32_MAX,"invalid requested Parquet field_id");
                for(int n=0;n<footer.info.num_fields;++n)if(footer.fields[n].field_id==options->field_ids[i])column=n;
                Require(column>=0,"requested Parquet field_id is absent",OG_LAKE_SDK_UNSUPPORTED);
            }
            if(options->projection_kind==OG_LAKE_SDK_BY_FIELD_ID)
                Require(footer.fields[column].field_id>=0,"Parquet field_id requires explicit host name mapping",OG_LAKE_SDK_UNSUPPORTED);
            Require(seen.insert(column).second,"duplicate projected Parquet field");
            Require(footer.fields[column].type!=OG_LAKE_SDK_OTHER,"Parquet field type is not admitted",OG_LAKE_SDK_UNSUPPORTED);
            reader->selected_fields.push_back(column);fields.push_back(footer.schema->field(column));
        }
        reader->projected_schema=arrow::schema(std::move(fields));seen.clear();
        int group_count=options->num_row_groups<0?footer.info.num_row_groups:options->num_row_groups;
        Require(group_count<=footer.info.num_row_groups,"too many selected Parquet row groups");
        for(int i=0;i<group_count;++i){
            int group=options->num_row_groups<0?i:options->row_groups[i];
            Require(group>=0&&group<footer.info.num_row_groups&&seen.insert(group).second,"invalid or duplicate Parquet row group");
            reader->selected_groups.push_back(group);
            Require(footer.groups[group].num_rows<=INT64_MAX-reader->remaining,"selected row count overflow");
            reader->remaining+=footer.groups[group].num_rows;
        }
        footer.reader->set_use_threads(false);footer.reader->set_batch_size(reader->batch_rows);
        *output=reader.release();return OG_LAKE_SDK_OK;
    });
}
int ReaderOpen(const OgLakeSdkReadOptions* options,OgLakeSdkReader** output,OgLakeSdkError* error)
{
    return ReaderOpenImpl(options,output,error,nullptr,nullptr);
}
int ReaderOpenFd(int fd,const char* version,const OgLakeSdkReadOptions* options,
    OgLakeSdkReader** output,OgLakeSdkError* error)
{
    return ReaderOpenImpl(options,output,error,&fd,version);
}
/* Reopening a RecordBatchReader creates fresh per-column/page decoder state.
 * Replay consumes only an immutable group's already-delivered prefix. A batch
 * straddling that exact prefix is sliced, never dropped or delivered twice. */
std::shared_ptr<arrow::RecordBatch> ReadCurrentGroup(OgLakeSdkReader* reader)
{
    if(!reader->batches){
        Poll(reader->limits);
        reader->footer->reader->set_batch_size(reader->decode_batch_rows);
        reader->batches=Take(reader->footer->reader->GetRecordBatchReader(
            std::vector<int>{reader->selected_groups[reader->group_index]},reader->selected_fields));
        int64_t skip=reader->group_offset,replayed=0;
        const int64_t group_rows=reader->footer->groups[reader->selected_groups[reader->group_index]].num_rows;
        while(skip>0){
            Poll(reader->limits);std::shared_ptr<arrow::RecordBatch> discarded;
            do{Check(reader->batches->ReadNext(&discarded));}while(discarded&&discarded->num_rows()==0);
            Require(discarded&&discarded->num_rows()>0&&discarded->num_rows()<=reader->decode_batch_rows&&
                discarded->num_rows()<=group_rows-replayed&&discarded->schema()->Equals(*reader->projected_schema,false),
                "Parquet retry cannot replay its delivered prefix",OG_LAKE_SDK_IO);
            if(discarded->num_rows()>skip){
                return discarded->Slice(skip);
            }
            skip-=discarded->num_rows();replayed+=discarded->num_rows();
        }
    }
    Poll(reader->limits);std::shared_ptr<arrow::RecordBatch> values;
    do{Check(reader->batches->ReadNext(&values));}while(values&&values->num_rows()==0);
    Require(values!=nullptr,"Parquet reader ended before the footer population",OG_LAKE_SDK_IO);
    return values;
}
int ReaderNext(OgLakeSdkReader* reader,OgLakeSdkBatch** output,OgLakeSdkError* error)
{
    if(output)*output=nullptr;
    return Guard(error,[&]{
        Require(reader&&output,"reader and batch output are required");
        Require(reader->owner==std::this_thread::get_id(),"Parquet reader belongs to another thread",OG_LAKE_SDK_STATE);
        Require(!reader->failed,"Parquet reader cannot be reused after an error",OG_LAKE_SDK_STATE);
        Require(!reader->lease->outstanding.load(),"release the preceding Parquet batch before reading again",OG_LAKE_SDK_STATE);
        reader->failed=true;Poll(reader->limits);
        if(reader->remaining==0){reader->batches.reset();reader->failed=false;return OG_LAKE_SDK_EOF;}
        while(reader->group_index<reader->selected_groups.size()&&
            reader->group_offset==reader->footer->groups[reader->selected_groups[reader->group_index]].num_rows){
            Poll(reader->limits);reader->batches.reset();++reader->group_index;reader->group_offset=0;
        }
        Require(reader->group_index<reader->selected_groups.size(),
            "Parquet reader exceeded selected row groups",OG_LAKE_SDK_IO);
        for(;;){
            try{
                std::shared_ptr<arrow::RecordBatch> values;
                if(reader->selected_fields.empty())values=arrow::RecordBatch::Make(reader->projected_schema,
                    std::min(reader->decode_batch_rows,reader->remaining),std::vector<std::shared_ptr<arrow::Array>>{});
                else values=ReadCurrentGroup(reader);
                int64_t count=values->num_rows();
                Require(count>0&&count<=reader->remaining&&count<=reader->batch_rows,"Parquet batch violates row bounds",OG_LAKE_SDK_IO);
                if(!reader->selected_fields.empty())
                    Require(count<=reader->footer->groups[reader->selected_groups[reader->group_index]].num_rows-reader->group_offset,
                        "Parquet batch crossed the selected physical row group",OG_LAKE_SDK_IO);
                Require(values->schema()->Equals(*reader->projected_schema,false),"Parquet projection schema/order changed",OG_LAKE_SDK_IO);
                ValidateTemporalValues(values,reader->limits);
                for(int column=0;column<values->num_columns();++column)
                    if(reader->footer->fields[reader->selected_fields[column]].type==OG_LAKE_SDK_UUID)
                        ValidateUuidValues(values->column(column),reader->limits);
                    else if(reader->footer->fields[reader->selected_fields[column]].type==OG_LAKE_SDK_JSON)
                        ValidateJsonValues(values->column(column),reader->limits);
                    else if(reader->footer->fields[reader->selected_fields[column]].type==OG_LAKE_SDK_DECIMAL128){
                        int p=0,s=0;
                        Require(Decimal128Parameters(reader->projected_schema->field(column)->type(),&p,&s),
                            "projected decimal128 parameters are unavailable",OG_LAKE_SDK_IO);
                        ValidateDecimal128Values(values->column(column),p,s,reader->limits);
                    }
                auto batch=std::make_unique<BatchOwner>();batch->pool=reader->footer->pool;
                batch->positions=Take(arrow::AllocateBuffer(count*sizeof(int64_t),batch->pool.get()));
                auto* positions=reinterpret_cast<int64_t*>(batch->positions->mutable_data());
                /* Commit cursor changes only after all allocation/export work.
                 * A memory retry must begin at the last delivered row. */
                size_t next_group=reader->group_index;int64_t next_offset=reader->group_offset;
                for(int64_t row=0;row<count;++row){
                    while(next_group<reader->selected_groups.size()&&
                        next_offset==reader->footer->groups[reader->selected_groups[next_group]].num_rows){
                        Poll(reader->limits);++next_group;next_offset=0;
                    }
                    Require(next_group<reader->selected_groups.size(),"Parquet position stream exceeded selected row groups",OG_LAKE_SDK_IO);
                    const auto& group=reader->footer->groups[reader->selected_groups[next_group]];
                    positions[row]=group.first_row+next_offset++;
                }
                Check(arrow::ExportRecordBatch(*values,&batch->array,&batch->schema));
                batch->object_version=reader->footer->object_version;
                batch->view={sizeof(OgLakeSdkBatch),count,&batch->array,&batch->schema,positions,batch.get(),batch->object_version.c_str()};
                batch->lease=reader->lease;reader->lease->outstanding.store(true);
                reader->group_index=next_group;reader->group_offset=next_offset;reader->remaining-=count;
                if(!reader->selected_fields.empty())reader->decoded_rows+=count;
                reader->failed=false;*output=&batch.release()->view;return OG_LAKE_SDK_OK;
            }catch(const Failure& failure){
                /* Attempt-local values/BatchOwner have been destroyed before
                 * this handler. Drop every decoder/table holding old buffers. */
                reader->batches.reset();
                if(failure.code!=OG_LAKE_SDK_MEMORY||reader->decode_batch_rows<=1)throw;
                int64_t previous=reader->decode_batch_rows;
                reader->decode_batch_rows=std::max<int64_t>(1,previous/2);
                Poll(reader->limits);
            }
        }
    });
}
void BatchRelease(OgLakeSdkBatch** batch) noexcept
{
    if(batch&&*batch){auto* owner=static_cast<BatchOwner*>((*batch)->private_data);*batch=nullptr;delete owner;}
}
void ReaderStats(const OgLakeSdkReader* reader,OgLakeSdkIoStats* stats) noexcept
{
    if(!stats)return;
    std::memset(stats,0,sizeof(*stats));if(!reader)return;
    stats->read_calls=reader->footer->file->read_calls;stats->bytes_read=reader->footer->file->bytes_read;
    stats->decoded_rows=reader->decoded_rows;stats->pool_bytes=reader->footer->pool->bytes_allocated();
    stats->peak_pool_bytes=reader->footer->pool->max_memory();
}
void ReaderClose(OgLakeSdkReader** reader) noexcept
{
    if(reader){delete *reader;*reader=nullptr;}
}

/* The parent directory descriptor and created inode identify the candidate.
 * Abort never removes a path that has since been replaced by another object. */
class LocalOutput final : public arrow::io::OutputStream {
    int parent_=-1,fd_=-1,anchor_=-1;
    std::string name_;
    struct stat identity_{};
    int64_t position_=0;
    OgLakeSdkLimits limits_;
    bool aborted_=false,retained_=false,created_=false;
    arrow::Status EntryIsOurs() const
    {
        struct stat current;
        if(::fstatat(parent_,name_.c_str(),&current,AT_SYMLINK_NOFOLLOW)!=0)
            return arrow::Status::IOError("cannot verify candidate Parquet directory entry");
        if(!S_ISREG(current.st_mode)||current.st_dev!=identity_.st_dev||current.st_ino!=identity_.st_ino)
            return arrow::Status::IOError("candidate Parquet directory entry was replaced");
        return arrow::Status::OK();
    }
public:
    LocalOutput(const char* path,OgLakeSdkLimits limits):limits_(limits)
    {
        Require(path&&path[0]=='/'&&std::strlen(path)<PATH_MAX,"writer requires a bounded absolute local path");
        std::string full(path);auto slash=full.find_last_of('/');name_=full.substr(slash+1);
        Require(!name_.empty()&&name_!="."&&name_!=".."&&name_.size()<=NAME_MAX,"invalid candidate Parquet file name");
        std::string directory=slash==0?"/":full.substr(0,slash);
        Poll(limits_);parent_=::open(directory.c_str(),O_RDONLY|O_DIRECTORY|O_CLOEXEC|O_NOFOLLOW);
        if(parent_<0)throw Failure(OG_LAKE_SDK_IO,"cannot open candidate Parquet parent directory",errno);
        fd_=::openat(parent_,name_.c_str(),O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC|O_NOFOLLOW,0600);
        if(fd_<0){int number=errno;::close(parent_);parent_=-1;throw Failure(OG_LAKE_SDK_IO,"cannot exclusively create candidate Parquet object",number);}
        if(::fstat(fd_,&identity_)!=0){
            /* Identity is unknown: leave the path to the host orphan tracker
             * rather than risk unlinking an externally replaced object. */
            int number=errno;::close(fd_);fd_=-1;
            ::close(parent_);parent_=-1;throw Failure(OG_LAKE_SDK_IO,"cannot identify candidate Parquet object",number);
        }
        created_=true;
    }
    ~LocalOutput() override
    {
        try{if(!retained_)(void)Discard();else(void)Close();}catch(...){}
        if(anchor_>=0)(void)::close(anchor_);
        if(parent_>=0)(void)::close(parent_);
    }
    arrow::Status Close() override
    {
        if(fd_<0)return arrow::Status::OK();
        int descriptor=fd_;fd_=-1;
        if(::close(descriptor)!=0)return arrow::Status::IOError("candidate Parquet close failed");
        return arrow::Status::OK();
    }
    bool closed() const override{return fd_<0;}
    arrow::Result<int64_t> Tell() const override{return position_;}
    arrow::Status Write(const void* data,int64_t count) override
    {
        if(aborted_||fd_<0)return arrow::Status::IOError("candidate Parquet output is not writable");
        if(count<0||(count&&!data)||count>INT64_MAX-position_)
            return arrow::Status::Invalid("invalid candidate Parquet write");
        int64_t done=0;
        while(done<count){
            if(Cancelled(limits_))return arrow::Status::Cancelled("candidate Parquet write cancelled");
            ssize_t written=::write(fd_,static_cast<const char*>(data)+done,
                static_cast<size_t>(std::min<int64_t>(65536,count-done)));
            if(written<0){if(errno==EINTR)continue;return arrow::Status::IOError("candidate Parquet write failed");}
            if(written==0)return arrow::Status::IOError("candidate Parquet write made no progress");
            done+=written;position_+=written;
        }
        return arrow::Status::OK();
    }
    void Stop() noexcept{aborted_=true;}
    arrow::Status Discard(bool discard_retained=false)
    {
        Stop();auto close_status=Close();
        if(!created_||(retained_&&!discard_retained))return close_status;
        auto entry=EntryIsOurs();if(!entry.ok())return entry;
        if(::unlinkat(parent_,name_.c_str(),0)!=0)
            return arrow::Status::IOError("cannot remove unfinished candidate Parquet object");
        created_=false;retained_=false;
        /* Persist removal before the caller forgets its candidate ownership. */
        int result;do{result=::fsync(parent_);}while(result!=0&&errno==EINTR);
        if(result!=0)return arrow::Status::IOError("cannot fsync candidate Parquet removal");
        return close_status;
    }
    int64_t FinishDurably()
    {
        Require(!aborted_&&fd_>=0,"candidate Parquet output is not finishable",OG_LAKE_SDK_STATE);
        Poll(limits_);Check(EntryIsOurs());
        struct stat current;
        if(::fstat(fd_,&current)!=0||current.st_size!=position_)
            throw Failure(OG_LAKE_SDK_IO,"candidate Parquet physical size disagrees with write position",errno);
        int result;do{Poll(limits_);result=::fsync(fd_);}while(result!=0&&errno==EINTR);
        if(result!=0)throw Failure(OG_LAKE_SDK_IO,"candidate Parquet data fsync failed",errno);
        /* Keep the completed inode alive until the caller resolves its
         * catalog transaction, so unlink/recreate cannot recycle its identity. */
        anchor_=::fcntl(fd_,F_DUPFD_CLOEXEC,0);
        if(anchor_<0)throw Failure(OG_LAKE_SDK_IO,"cannot retain candidate Parquet identity",errno);
        Check(Close());
        do{Poll(limits_);result=::fsync(parent_);}while(result!=0&&errno==EINTR);
        if(result!=0)throw Failure(OG_LAKE_SDK_IO,"candidate Parquet directory fsync failed",errno);
        Check(EntryIsOurs());retained_=true;return position_;
    }
};

void BorrowedArrayRelease(ArrowArray* value){value->release=nullptr;}
void BorrowedSchemaRelease(ArrowSchema* value){value->release=nullptr;}
/* Import consumes only these shells. Neither buffers nor caller callbacks are
 * retained; WriteTable completes before the borrowed shells leave this scope. */
struct BorrowedNode {
    ArrowArray array{};
    ArrowSchema schema{};
    std::vector<std::unique_ptr<BorrowedNode>> children;
    std::vector<ArrowArray*> column_pointers;
    std::vector<ArrowSchema*> field_pointers;
    BorrowedNode(const ArrowArray* input,const ArrowSchema* input_schema,int depth,int count=0)
    {
        Require(input&&input_schema&&input->release&&input_schema->release&&
            input->length>=0&&input->offset>=0&&input_schema->format&&
            !input->dictionary&&!input_schema->dictionary,"invalid borrowed Arrow input");
        int child_count=0;
        if(depth==0){
            Require(input->offset==0&&input->null_count==0&&std::strcmp(input_schema->format,"+s")==0,
                "invalid Arrow batch root");child_count=count;
        }else if(depth==1&&std::strcmp(input_schema->format,"+l")==0)child_count=1;
        else Require(depth==1||(depth==2&&std::strcmp(input_schema->format,"f")==0),
            "only one-level float32 lists are admitted",OG_LAKE_SDK_UNSUPPORTED);
        Require(input->n_children==child_count&&input_schema->n_children==child_count&&
            (child_count==0||(input->children&&input_schema->children)),
            "nested or inconsistent Arrow writer input",OG_LAKE_SDK_UNSUPPORTED);
        array=*input;schema=*input_schema;children.reserve(child_count);
        column_pointers.resize(child_count);field_pointers.resize(child_count);
        for(int i=0;i<child_count;++i){
            auto node=std::make_unique<BorrowedNode>(input->children[i],input_schema->children[i],depth+1);
            if(depth==0)Require(node->array.length==input->length,"inconsistent Arrow column length");
            column_pointers[i]=&node->array;field_pointers[i]=&node->schema;children.push_back(std::move(node));
        }
        array.children=column_pointers.data();array.release=BorrowedArrayRelease;array.private_data=nullptr;
        schema.children=field_pointers.data();schema.release=BorrowedSchemaRelease;schema.private_data=nullptr;
    }
};
void ValidateBorrowedUuid(const ArrowArray* array,const ArrowSchema* schema)
{
    Require(array&&schema&&schema->format&&std::strcmp(schema->format,"w:16")==0&&
        array->n_buffers==2&&array->buffers&&array->n_children==0&&schema->n_children==0&&
        array->offset>=0&&array->length>=0&&array->null_count>=-1&&array->null_count<=array->length&&
        array->offset<=INT64_MAX-array->length&&array->offset+array->length<=INT64_MAX/16&&
        (array->length==0||array->buffers[1])&&
        (array->null_count<=0||array->buffers[0]),
        "UUID borrowed Arrow input requires bounded w:16, validity and data buffers");
    /* C Data exposes no independent byte-capacity field. As for existing
     * primitive types, the producer must own the declared valid buffer range. */
}
void ValidateBorrowedDecimal(const ArrowArray* array,const ArrowSchema* schema,
    const std::shared_ptr<arrow::DataType>& expected)
{
    int p=0,s=0;
    Require(Decimal128Parameters(expected,&p,&s),"missing writer decimal128 declaration");
    const std::string format="d:"+std::to_string(p)+","+std::to_string(s);
    Require(array&&schema&&schema->format&&
        (format==schema->format||format+",128"==schema->format)&&
        array->n_buffers==2&&array->buffers&&
        array->n_children==0&&schema->n_children==0&&
        array->offset>=0&&array->length>=0&&array->null_count>=-1&&array->null_count<=array->length&&
        array->offset<=INT64_MAX-array->length&&array->offset+array->length<=INT64_MAX/16&&
        (array->length==0||array->buffers[1])&&(array->null_count<=0||array->buffers[0]),
        "decimal128 borrowed input requires exact p/s, width128, validity and bounded data");
    /* Producer owns actual capacities. Reject parameter/width/offset mismatch
     * before ImportRecordBatch can use the borrowed coefficient range. */
}
void ValidateBorrowedJson(const ArrowArray* array,const ArrowSchema* schema)
{
    Require(array&&schema&&schema->format&&std::strcmp(schema->format,"u")==0&&
        array->n_buffers==3&&array->buffers&&array->n_children==0&&schema->n_children==0&&
        array->offset>=0&&array->length>=0&&array->null_count>=-1&&array->null_count<=array->length&&
        array->offset<=INT32_MAX-1&&array->length<=INT32_MAX-1-array->offset&&
        (array->length==0||array->buffers[1])&&
        (array->null_count<=0||array->buffers[0]),
        "JSON borrowed input requires bounded u, validity, int32 offsets and data buffers");
    /* As for all original C Data types, the producer guarantees actual buffer
     * capacity. No pointer arithmetic occurs before the bounded shape check. */
}
std::shared_ptr<arrow::RecordBatch> NormalizeJsonInputBatch(
    const std::shared_ptr<arrow::RecordBatch>& batch,const std::vector<int>& json_columns)
{
    auto fields=batch->schema()->fields();auto arrays=batch->columns();
    for(int column:json_columns){
        const auto& field=fields[column];const auto& values=arrays[column];
        if(field->metadata())for(int i=0;i<field->metadata()->size();++i)
            if(field->metadata()->key(i)==arrow::kExtensionTypeKeyName)
                Require(field->metadata()->value(i)=="arrow.json","JSON input has a different Arrow extension");
        Require(JsonStorageType(values->type()),"JSON input is not UTF8 or canonical arrow.json/UTF8");
        auto storage=values;
        if(values->type_id()==arrow::Type::EXTENSION)
            storage=static_cast<const arrow::ExtensionArray&>(*values).storage();
        Require(storage->length()==values->length()&&storage->offset()==values->offset(),
            "JSON input extension storage offset/length differs");
        auto type=arrow::extension::json(arrow::utf8());
        fields[column]=field->WithType(type);
        arrays[column]=arrow::ExtensionType::WrapArray(type,storage);
    }
    return arrow::RecordBatch::Make(arrow::schema(std::move(fields),batch->schema()->metadata()),
        batch->num_rows(),std::move(arrays));
}
std::shared_ptr<arrow::RecordBatch> NormalizeUuidInputBatch(
    const std::shared_ptr<arrow::RecordBatch>& batch,const std::vector<int>& uuid_columns)
{
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for(int column:uuid_columns){
        const auto& field=batch->schema()->field(column);const auto& values=batch->column(column);
        if(field->metadata())for(int i=0;i<field->metadata()->size();++i)
            if(field->metadata()->key(i)==arrow::kExtensionTypeKeyName)
                Require(field->metadata()->value(i)=="arrow.uuid","UUID input has a different Arrow extension");
        if(values->type_id()!=arrow::Type::EXTENSION)continue;
        Require(UuidStorageType(values->type()),"UUID input extension is not canonical fixed16");
        const auto& storage=static_cast<const arrow::ExtensionArray&>(*values).storage();
        Require(storage->length()==values->length()&&storage->offset()==values->offset(),
            "UUID input extension storage offset/length differs");
        if(arrays.empty()){
            fields=batch->schema()->fields();arrays=batch->columns();
        }
        /* Official ExtensionArray storage is a temporary shared-buffer view.
         * Names/nullability/field metadata remain intact for the original
         * schema check; no other column/type or metadata is normalized. */
        fields[column]=field->WithType(storage->type());arrays[column]=storage;
    }
    if(arrays.empty())return batch;
    return arrow::RecordBatch::Make(arrow::schema(std::move(fields),batch->schema()->metadata()),
        batch->num_rows(),std::move(arrays));
}
std::shared_ptr<arrow::DataType> WriterType(uint32_t type)
{
    switch(type){
        case OG_LAKE_SDK_BOOL:return arrow::boolean();
        case OG_LAKE_SDK_INT16:return arrow::int16();
        case OG_LAKE_SDK_INT32:return arrow::int32();
        case OG_LAKE_SDK_INT64:return arrow::int64();
        case OG_LAKE_SDK_FLOAT32:return arrow::float32();
        case OG_LAKE_SDK_FLOAT64:return arrow::float64();
        case OG_LAKE_SDK_DATE32:return arrow::date32();
        case OG_LAKE_SDK_TIMESTAMP_US:return arrow::timestamp(arrow::TimeUnit::MICRO);
        case OG_LAKE_SDK_TIMESTAMPTZ_US:return arrow::timestamp(arrow::TimeUnit::MICRO,"UTC");
        case OG_LAKE_SDK_UTF8:return arrow::utf8();
        case OG_LAKE_SDK_BINARY:return arrow::binary();
        case OG_LAKE_SDK_UUID:return arrow::fixed_size_binary(16);
        case OG_LAKE_SDK_JSON:return arrow::extension::json(arrow::utf8());
        default:throw Failure(OG_LAKE_SDK_UNSUPPORTED,"Parquet writer field type is not admitted");
    }
}
parquet::Compression::type WriterCompression(uint32_t compression)
{
    switch(compression){
        case OG_LAKE_SDK_UNCOMPRESSED:return parquet::Compression::UNCOMPRESSED;
        case OG_LAKE_SDK_ZSTD:return parquet::Compression::ZSTD;
        case OG_LAKE_SDK_SNAPPY:return parquet::Compression::SNAPPY;
        case OG_LAKE_SDK_GZIP:return parquet::Compression::GZIP;
        case OG_LAKE_SDK_LZ4:return parquet::Compression::LZ4;
        default:throw Failure(OG_LAKE_SDK_UNSUPPORTED,"Parquet writer compression is not admitted");
    }
}
} // namespace

struct OgLakeSdkWriter {
    OgLakeSdkLimits limits{};
    std::shared_ptr<BudgetPool> pool;
    std::shared_ptr<arrow::Schema> schema;
    std::shared_ptr<LocalOutput> output;
    std::unique_ptr<parquet::arrow::FileWriter> writer;
    std::vector<int> uuid_columns; /* Internal writer state, never an ABI field. */
    std::vector<int> json_columns; /* Physical JSON columns; the public ABI is unchanged. */
    std::vector<int> decimal_columns; /* Only ABI5 supplies p/s through its sidecar. */
    std::thread::id owner=std::this_thread::get_id();
    int64_t row_group_rows=0,rows=0,bytes=0;
    bool failed=false,finished=false;
    ~OgLakeSdkWriter(){if(output&&!finished)output->Stop();}
};

namespace {
std::unique_ptr<parquet::arrow::FileWriter> OpenUuidWriter(
    const std::shared_ptr<arrow::Schema>& schema,arrow::MemoryPool* pool,
    const std::shared_ptr<LocalOutput>& output,
    const std::shared_ptr<parquet::WriterProperties>& properties,
    const std::shared_ptr<parquet::ArrowWriterProperties>& arrow_properties,
    const std::vector<int>& uuid_columns)
{
    Require(!uuid_columns.empty()&&!arrow_properties->store_schema(),
        "UUID writer requires the provider's original non-extension schema properties",OG_LAKE_SDK_UNSUPPORTED);
    std::shared_ptr<parquet::SchemaDescriptor> automatic;
    Check(parquet::arrow::ToParquetSchema(schema.get(),*properties,*arrow_properties,&automatic));
    auto root=std::static_pointer_cast<parquet::schema::GroupNode>(automatic->schema_root());
    Require(root->field_count()==schema->num_fields(),"UUID writer top-level schema changed");
    parquet::schema::NodeVector fields;fields.reserve(root->field_count());
    size_t next=0;
    for(int column=0;column<root->field_count();++column){
        const auto& old=root->field(column);
        if(next<uuid_columns.size()&&uuid_columns[next]==column){
            Require(old->is_primitive()&&UuidStorageType(schema->field(column)->type()),
                "UUID writer field is not one fixed16 primitive");
            const auto& primitive=static_cast<const parquet::schema::PrimitiveNode&>(*old);
            Require(primitive.physical_type()==parquet::Type::FIXED_LEN_BYTE_ARRAY&&
                primitive.type_length()==16&&old->field_id()>=0,
                "UUID writer primitive lost its width or field_id");
            fields.push_back(parquet::schema::PrimitiveNode::Make(old->name(),old->repetition(),
                parquet::LogicalType::UUID(),parquet::Type::FIXED_LEN_BYTE_ARRAY,16,old->field_id()));
            ++next;
        }else fields.push_back(old); /* Other types, LIST children and IDs are identical nodes. */
    }
    Require(next==uuid_columns.size(),"UUID writer field selection differs from the declared schema");
    /* Arrow20 rejects a LogicalType::None object in the GroupNode overload;
     * preserve its original root through the proven converted-type overload. */
    auto physical=std::static_pointer_cast<parquet::schema::GroupNode>(
        parquet::schema::GroupNode::Make(root->name(),root->repetition(),fields,
            root->converted_type(),root->field_id()));
    auto underlying=parquet::ParquetFileWriter::Open(output,physical,properties);
    std::unique_ptr<parquet::arrow::FileWriter> writer;
    Check(parquet::arrow::FileWriter::Make(pool,std::move(underlying),schema,arrow_properties,&writer));
    return writer;
}
void ValidateUuidWrittenSchema(const std::shared_ptr<parquet::FileMetaData>& metadata,
    const std::shared_ptr<arrow::Schema>& schema,const std::vector<int>& uuid_columns)
{
    Require(metadata&&metadata->num_columns()==schema->num_fields()&&
        metadata->schema()->group_node()->field_count()==schema->num_fields(),
        "UUID writer footer schema population changed",OG_LAKE_SDK_IO);
    const auto* root=metadata->schema()->group_node();
    for(int column:uuid_columns){
        const auto& field=schema->field(column);const auto& node=root->field(column);
        Require(node->is_primitive(),"UUID writer footer field is nested",OG_LAKE_SDK_IO);
        const auto& primitive=static_cast<const parquet::schema::PrimitiveNode&>(*node);
        const auto& logical=primitive.logical_type();
        Require(primitive.physical_type()==parquet::Type::FIXED_LEN_BYTE_ARRAY&&
            primitive.type_length()==16&&logical&&logical->is_UUID()&&
            node->name()==field->name()&&node->is_optional()==field->nullable()&&
            field->metadata()&&
            std::to_string(node->field_id())==Take(field->metadata()->Get("PARQUET:field_id")),
            "UUID writer footer lost logical UUID, width16 or exact field identity",OG_LAKE_SDK_IO);
    }
}
void ValidateJsonWrittenSchema(const std::shared_ptr<parquet::FileMetaData>& metadata,
    const std::shared_ptr<arrow::Schema>& schema,const std::vector<int>& json_columns)
{
    Require(metadata&&metadata->num_columns()==schema->num_fields()&&
        metadata->schema()->group_node()->field_count()==schema->num_fields(),
        "JSON writer footer schema population changed",OG_LAKE_SDK_IO);
    const auto* root=metadata->schema()->group_node();
    for(int column:json_columns){
        const auto& field=schema->field(column);const auto& node=root->field(column);
        Require(node->is_primitive(),"JSON writer footer field is nested",OG_LAKE_SDK_IO);
        const auto& primitive=static_cast<const parquet::schema::PrimitiveNode&>(*node);
        const auto& logical=primitive.logical_type();
        Require(primitive.physical_type()==parquet::Type::BYTE_ARRAY&&logical&&logical->is_JSON()&&
            node->name()==field->name()&&node->is_optional()==field->nullable()&&field->metadata()&&
            std::to_string(node->field_id())==Take(field->metadata()->Get("PARQUET:field_id")),
            "JSON writer footer lost physical JSON or exact field identity",OG_LAKE_SDK_IO);
    }
}
void ValidateDecimalWrittenSchema(const std::shared_ptr<parquet::FileMetaData>& metadata,
    const std::shared_ptr<arrow::Schema>& schema,const std::vector<int>& decimal_columns)
{
    Require(metadata&&metadata->num_columns()==schema->num_fields()&&
        metadata->schema()->group_node()->field_count()==schema->num_fields(),
        "decimal128 writer footer schema population changed",OG_LAKE_SDK_IO);
    const auto* root=metadata->schema()->group_node();
    for(int column:decimal_columns){
        const auto& field=schema->field(column);const auto& node=root->field(column);
        Require(node->is_primitive()&&Decimal128Physical(field->type(),metadata->schema()->Column(column))&&
            node->name()==field->name()&&node->is_optional()==field->nullable()&&field->metadata()&&
            std::to_string(node->field_id())==Take(field->metadata()->Get("PARQUET:field_id")),
            "decimal128 writer footer lost DECIMAL parameters or field identity",OG_LAKE_SDK_IO);
    }
}
int WriterOpenParameterized(const OgLakeSdkWriteOptions* options,int32_t num_lists,const OgLakeSdkListField* lists,
    int32_t num_decimals,const OgLakeSdkDecimalField* decimals,OgLakeSdkWriter** output,OgLakeSdkError* error)
{
    if(output)*output=nullptr;
    return Guard(error,[&]{
        Require(options&&output&&options->struct_size>=sizeof(*options),"invalid writer options/output ABI");
        Require(options->num_fields>0&&options->num_fields<=4096&&options->fields,
            "invalid Parquet writer field vector");
        Require(num_lists>=0&&num_lists<=options->num_fields&&(num_lists==0||lists),
            "invalid Parquet writer list descriptors");
        Require(num_decimals>=0&&num_decimals<=options->num_fields&&(num_decimals==0||decimals),
            "invalid Parquet writer decimal descriptors");
        std::set<int32_t> decimal_ordinals;
        for(int n=0;n<num_decimals;++n){
            const auto& decimal=decimals[n];
            Require(decimal.struct_size>=sizeof(decimal)&&decimal.field_ordinal>=0&&
                decimal.field_ordinal<options->num_fields&&decimal_ordinals.insert(decimal.field_ordinal).second&&
                decimal.precision>=1&&decimal.precision<=38&&decimal.scale>=0&&decimal.scale<=decimal.precision&&
                options->fields[decimal.field_ordinal].type==OG_LAKE_SDK_DECIMAL128,
                "invalid, duplicate or unmatched decimal writer descriptor");
        }
        Require(options->row_group_rows>0&&options->row_group_rows<=1048576,"invalid Parquet writer row-group limit");
        auto writer=std::make_unique<OgLakeSdkWriter>();writer->limits=Limits(&options->limits);
        writer->pool=std::make_shared<BudgetPool>(writer->limits.memory_bytes);
        writer->row_group_rows=options->row_group_rows;
        std::set<int64_t> ids;std::set<std::string> names;std::vector<std::shared_ptr<arrow::Field>> fields;
        std::set<int64_t> list_ids;int matched_lists=0;
        for(int i=0;i<num_lists;++i)
            Require(list_ids.insert(lists[i].field_id).second,"duplicate list descriptor");
        for(int i=0;i<options->num_fields;++i){
            const auto& field=options->fields[i];
            Require(field.field_id>=0&&field.field_id<=INT32_MAX&&ids.insert(field.field_id).second,
                "invalid or duplicate Parquet writer field_id");
            Require(field.name&&std::strlen(field.name)>0&&std::strlen(field.name)<=1048576&&
                names.insert(field.name).second&&field.nullable<=1,"invalid or duplicate Parquet writer field name");
            auto metadata=arrow::key_value_metadata({"PARQUET:field_id"},{std::to_string(field.field_id)});
            std::shared_ptr<arrow::DataType> type;
            if(field.type==OG_LAKE_SDK_FLOAT32_LIST){
                const OgLakeSdkListField* child=nullptr;
                for(int n=0;n<num_lists;++n)if(lists[n].field_id==field.field_id)child=&lists[n];
                Require(child!=nullptr,"list writer requires the optional nested schema ABI",OG_LAKE_SDK_UNSUPPORTED);
                Require(child->element_field_id>=0&&child->element_field_id<=INT32_MAX&&
                    ids.insert(child->element_field_id).second&&child->element_nullable<=1,
                    "invalid or duplicate list element field_id/nullability");
                auto child_metadata=arrow::key_value_metadata({"PARQUET:field_id"},{std::to_string(child->element_field_id)});
                type=arrow::list(arrow::field("element",arrow::float32(),child->element_nullable!=0,child_metadata));
                ++matched_lists;
            }else if(field.type==OG_LAKE_SDK_DECIMAL128){
                const OgLakeSdkDecimalField* decimal=nullptr;
                for(int n=0;n<num_decimals;++n)if(decimals[n].field_ordinal==i)decimal=&decimals[n];
                Require(decimal,"decimal writer requires exact optional ABI5 parameters",OG_LAKE_SDK_UNSUPPORTED);
                type=arrow::decimal128(decimal->precision,decimal->scale);
                writer->decimal_columns.push_back(i);
            }else type=WriterType(field.type);
            if(field.type==OG_LAKE_SDK_UUID)writer->uuid_columns.push_back(i);
            if(field.type==OG_LAKE_SDK_JSON)writer->json_columns.push_back(i);
            fields.push_back(arrow::field(field.name,type,field.nullable!=0,metadata));
        }
        Require(matched_lists==num_lists,"list descriptor does not match a list field");
        auto codec=WriterCompression(options->compression);writer->schema=arrow::schema(std::move(fields));
        parquet::WriterProperties::Builder properties;
        properties.memory_pool(writer->pool.get())->compression(codec)->write_batch_size(1024)->
            max_row_group_length(writer->row_group_rows)->data_pagesize(65536)->enable_page_checksum();
        /* Plain encoding preserves signed zeros and NaN payload bits.
         * The selected lossless compression remains enabled. */
        for(int i=0;i<options->num_fields;++i)
            if(options->fields[i].type==OG_LAKE_SDK_FLOAT32||options->fields[i].type==OG_LAKE_SDK_FLOAT64)
                properties.disable_dictionary(options->fields[i].name);
            else if(options->fields[i].type==OG_LAKE_SDK_FLOAT32_LIST)
                properties.disable_dictionary(std::string(options->fields[i].name)+".list.element");
        parquet::ArrowWriterProperties::Builder arrow_properties;arrow_properties.set_use_threads(false);
        writer->output=std::make_shared<LocalOutput>(options->path,writer->limits);
        if(!writer->uuid_columns.empty())
            writer->writer=OpenUuidWriter(writer->schema,writer->pool.get(),writer->output,
                properties.build(),arrow_properties.build(),writer->uuid_columns);
        else writer->writer=Take(parquet::arrow::FileWriter::Open(*writer->schema,writer->pool.get(),writer->output,
            properties.build(),arrow_properties.build()));
        *output=writer.release();return OG_LAKE_SDK_OK;
    });
}
int WriterOpenNested(const OgLakeSdkWriteOptions* options,int32_t num_lists,const OgLakeSdkListField* lists,
    OgLakeSdkWriter** output,OgLakeSdkError* error)
{
    return WriterOpenParameterized(options,num_lists,lists,0,nullptr,output,error);
}
int WriterOpen(const OgLakeSdkWriteOptions* options,OgLakeSdkWriter** output,OgLakeSdkError* error)
{
    return WriterOpenNested(options,0,nullptr,output,error);
}
int WriterAppend(OgLakeSdkWriter* writer,const ArrowArray* array,const ArrowSchema* schema,OgLakeSdkError* error)
{
    return Guard(error,[&]{
        Require(writer&&writer->owner==std::this_thread::get_id(),"Parquet writer belongs to another thread",OG_LAKE_SDK_STATE);
        Require(!writer->failed&&!writer->finished,"Parquet writer cannot append in this state",OG_LAKE_SDK_STATE);
        writer->failed=true;Poll(writer->limits);
        BorrowedNode borrowed(array,schema,0,writer->schema->num_fields());
        for(int column:writer->uuid_columns)
            ValidateBorrowedUuid(&borrowed.children[column]->array,&borrowed.children[column]->schema);
        for(int column:writer->json_columns)
            ValidateBorrowedJson(&borrowed.children[column]->array,&borrowed.children[column]->schema);
        for(int column:writer->decimal_columns)
            ValidateBorrowedDecimal(&borrowed.children[column]->array,&borrowed.children[column]->schema,
                writer->schema->field(column)->type());
        auto batch=Take(arrow::ImportRecordBatch(&borrowed.array,&borrowed.schema));
        if(!writer->uuid_columns.empty())batch=NormalizeUuidInputBatch(batch,writer->uuid_columns);
        if(!writer->json_columns.empty())batch=NormalizeJsonInputBatch(batch,writer->json_columns);
        for(int column:writer->decimal_columns){
            const auto& field=batch->schema()->field(column);
            if(field->metadata())for(int n=0;n<field->metadata()->size();++n)
                Require(field->metadata()->key(n)!=arrow::kExtensionTypeKeyName,
                    "decimal128 input has unsupported Arrow extension metadata");
        }
        Require(batch->schema()->Equals(*writer->schema,false),"Arrow writer input schema does not match declared fields");
        Check(batch->ValidateFull());
        ValidateTemporalValues(batch,writer->limits);
        for(int column:writer->decimal_columns){
            int p=0,s=0;
            Require(Decimal128Parameters(writer->schema->field(column)->type(),&p,&s),
                "decimal128 writer has no exact parameter declaration");
            ValidateDecimal128Values(batch->column(column),p,s,writer->limits);
        }
        for(int column:writer->uuid_columns)ValidateUuidValues(batch->column(column),writer->limits);
        for(int column:writer->json_columns)ValidateJsonValues(batch->column(column),writer->limits);
        for(int i=0;i<batch->num_columns();++i){
            Require(writer->schema->field(i)->nullable()||batch->column(i)->null_count()==0,
                "NULL violates the declared Parquet field");
            if(writer->schema->field(i)->type()->id()==arrow::Type::LIST){
                auto values=std::static_pointer_cast<arrow::ListArray>(batch->column(i))->values();
                Require(writer->schema->field(i)->type()->field(0)->nullable()||values->null_count()==0,
                    "NULL violates the declared Parquet list element");
            }
        }
        Require(batch->num_rows()<=INT64_MAX-writer->rows,"Parquet writer row population overflow");
        if(batch->num_rows()>0){
            auto table=Take(arrow::Table::FromRecordBatches(writer->schema,{batch}));
            Check(writer->writer->WriteTable(*table,writer->row_group_rows));
        }
        Poll(writer->limits);writer->rows+=batch->num_rows();writer->failed=false;return OG_LAKE_SDK_OK;
    });
}
int WriterFinish(OgLakeSdkWriter* writer,OgLakeSdkWrittenFile* result,OgLakeSdkError* error)
{
    return Guard(error,[&]{
        Require(writer&&result&&result->struct_size>=sizeof(*result),"invalid writer finish output ABI");
        *result={sizeof(*result),0,0,0};
        Require(writer->owner==std::this_thread::get_id(),"Parquet writer belongs to another thread",OG_LAKE_SDK_STATE);
        Require(!writer->failed&&!writer->finished,"Parquet writer cannot finish in this state",OG_LAKE_SDK_STATE);
        writer->failed=true;Poll(writer->limits);Check(writer->writer->Close());
        Require(writer->writer->metadata()&&writer->writer->metadata()->num_rows()==writer->rows,
            "Parquet writer footer row population disagrees",OG_LAKE_SDK_IO);
        if(!writer->uuid_columns.empty())
            ValidateUuidWrittenSchema(writer->writer->metadata(),writer->schema,writer->uuid_columns);
        if(!writer->json_columns.empty())
            ValidateJsonWrittenSchema(writer->writer->metadata(),writer->schema,writer->json_columns);
        if(!writer->decimal_columns.empty())
            ValidateDecimalWrittenSchema(writer->writer->metadata(),writer->schema,writer->decimal_columns);
        writer->writer.reset();writer->bytes=writer->output->FinishDurably();
        writer->finished=true;writer->failed=false;
        *result={sizeof(*result),1,writer->rows,writer->bytes};return OG_LAKE_SDK_OK;
    });
}
int WriterClose(OgLakeSdkWriter** handle,OgLakeSdkError* error)
{
    return Guard(error,[&]{
        if(!handle||!*handle)return OG_LAKE_SDK_OK;
        std::unique_ptr<OgLakeSdkWriter> writer(*handle);*handle=nullptr;
        if(!writer->finished){
            if(writer->output)writer->output->Stop();
            writer->writer.reset();
            if(writer->output)Check(writer->output->Discard());
        }
        return OG_LAKE_SDK_OK;
    });
}
/* An outer catalog abort must also discard already-fsynced candidates.
 * The retained output object still carries its original ownership identity;
 * Discard preserves an unrelated replacement at the same pathname. */
int WriterAbort(OgLakeSdkWriter** handle,OgLakeSdkError* error)
{
    return Guard(error,[&]{
        if(!handle||!*handle)return OG_LAKE_SDK_OK;
        std::unique_ptr<OgLakeSdkWriter> writer(*handle);*handle=nullptr;
        if(writer->output)writer->output->Stop();
        writer->writer.reset();
        if(writer->output)Check(writer->output->Discard(true));
        return OG_LAKE_SDK_OK;
    });
}

/* Walk from / with anchored directory descriptors, rejecting every symlink
 * component. URI reuse is forbidden by the host retirement contract. */
class DeleteDirectory {
    int descriptor_;
public:
    explicit DeleteDirectory(int fd):descriptor_(fd){}
    ~DeleteDirectory(){if(descriptor_>=0)(void)::close(descriptor_);}
    DeleteDirectory(const DeleteDirectory&)=delete;
    DeleteDirectory& operator=(const DeleteDirectory&)=delete;
    int descriptor() const{return descriptor_;}
    void replace(int fd){int old=descriptor_;descriptor_=fd;if(old>=0)(void)::close(old);}
};
std::vector<std::string> DeleteComponents(const char* path,bool root)
{
    Require(path&&::strnlen(path,PATH_MAX)<PATH_MAX,"retired file path exceeds the local bound");
    std::string value(path);
    if(root){
        Require(value.size()>1&&value[0]=='/',"cleanup requires a non-root absolute data directory");
        value.erase(0,1);
        if(!value.empty()&&value.back()=='/')value.pop_back();
    }else Require(!value.empty()&&value[0]!='/',"cleanup requires a relative object path");
    std::vector<std::string> parts;
    size_t start=0;
    for(;;){
        size_t end=value.find('/',start);
        std::string part=value.substr(start,end==std::string::npos?end:end-start);
        Require(!part.empty()&&part!="."&&part!=".."&&part.size()<=NAME_MAX,"invalid retired file path component");
        parts.push_back(std::move(part));
        if(end==std::string::npos)break;
        start=end+1;
    }
    return parts;
}
void DeleteDescend(DeleteDirectory& parent,const std::string& component,const OgLakeSdkLimits& limits)
{
    int fd;
    do{Poll(limits);fd=::openat(parent.descriptor(),component.c_str(),O_RDONLY|O_DIRECTORY|O_CLOEXEC|O_NOFOLLOW);}
    while(fd<0&&errno==EINTR);
    if(fd<0)throw Failure(OG_LAKE_SDK_IO,"cannot anchor retired file parent directory",errno);
    parent.replace(fd);
}
int DeleteRetiredFile(const char* root,const char* path,const OgLakeSdkLimits* input,
                     uint32_t* removed,OgLakeSdkError* error)
{
    if(removed)*removed=0;
    return Guard(error,[&]{
        Require(removed!=nullptr,"missing retired file deletion result");
        OgLakeSdkLimits limits=Limits(input);
        auto directories=DeleteComponents(root,true);
        auto components=DeleteComponents(path,false);
        const std::string& name=components.back();
        Require(name.size()>8&&name.compare(name.size()-8,8,".parquet")==0,"cleanup only accepts Parquet object names");
        Poll(limits);
        int anchor;
        do{anchor=::open("/",O_RDONLY|O_DIRECTORY|O_CLOEXEC);}while(anchor<0&&errno==EINTR);
        if(anchor<0)throw Failure(OG_LAKE_SDK_IO,"cannot anchor local cleanup root",errno);
        DeleteDirectory parent(anchor);
        for(const auto& part:directories)DeleteDescend(parent,part,limits);
        for(size_t i=0;i+1<components.size();++i)DeleteDescend(parent,components[i],limits);
        struct stat identity;
        int status;
        do{Poll(limits);status=::fstatat(parent.descriptor(),name.c_str(),&identity,AT_SYMLINK_NOFOLLOW);}
        while(status!=0&&errno==EINTR);
        if(status!=0&&errno!=ENOENT)throw Failure(OG_LAKE_SDK_IO,"cannot inspect retired local file",errno);
        if(status==0){
            Require(S_ISREG(identity.st_mode),"retired object is not a regular local file");
            do{Poll(limits);status=::unlinkat(parent.descriptor(),name.c_str(),0);}while(status!=0&&errno==EINTR);
            if(status!=0&&errno!=ENOENT)throw Failure(OG_LAKE_SDK_IO,"cannot delete retired local file",errno);
            if(status==0)*removed=1;
        }
        do{Poll(limits);status=::fsync(parent.descriptor());}while(status!=0&&errno==EINTR);
        if(status!=0)throw Failure(OG_LAKE_SDK_IO,"cannot sync retired file directory",errno);
        return OG_LAKE_SDK_OK;
    });
}

} // namespace

/* Only the C entrypoint is exported. The native executor never links to Arrow
 * C++ objects; callers must inspect capabilities before using optional slots. */
extern "C" __attribute__((visibility("default")))
const OgLakeParquetSdkApi* og_lake_parquet_sdk_v1(void)
{
    static const OgLakeParquetSdkApi api={
        OG_LAKE_PARQUET_ABI,sizeof(OgLakeParquetSdkApi),"apache_arrow_parquet","20.0.0",
        OG_LAKE_SDK_LOCAL_FILES|OG_LAKE_SDK_FIELD_IDS|OG_LAKE_SDK_ORIGINAL_POSITIONS|
            OG_LAKE_SDK_IO_STATS|OG_LAKE_SDK_POOL_BUDGET|OG_LAKE_SDK_POLL_CANCEL|
            OG_LAKE_SDK_WRITE_ORDER|OG_LAKE_SDK_WRITER|OG_LAKE_SDK_FOOTER_BYTES|OG_LAKE_SDK_OBJECT_VERSION|
            OG_LAKE_SDK_FLOAT_VALUES|OG_LAKE_SDK_FLOAT32_LIST_VALUES|OG_LAKE_SDK_LIST_FIELD_IDS|OG_LAKE_SDK_BINARY_VALUES|OG_LAKE_SDK_INT16_VALUES|
            OG_LAKE_SDK_DATE32_VALUES|OG_LAKE_SDK_TIMESTAMP_US_VALUES|OG_LAKE_SDK_TIMESTAMPTZ_US_VALUES|OG_LAKE_SDK_UUID_VALUES|OG_LAKE_SDK_JSON_VALUES|OG_LAKE_SDK_DECIMAL128_VALUES,
        FooterOpen,FooterInfo,FooterClose,ReaderOpen,ReaderNext,BatchRelease,
        ReaderStats,ReaderClose,WriterOpen,WriterAppend,WriterFinish,WriterClose
    };
    return &api;
}

extern "C" __attribute__((visibility("default")))
const OgLakeParquetLifecycleApi* og_lake_parquet_sdk_v2(void)
{
    static const OgLakeParquetLifecycleApi api={OG_LAKE_PARQUET_LIFECYCLE_ABI,
        sizeof(OgLakeParquetLifecycleApi),og_lake_parquet_sdk_v1(),OG_LAKE_SDK_ABORT_FINISHED,WriterAbort};
    return &api;
}


extern "C" __attribute__((visibility("default")))
const OgLakeParquetMaintenanceApi* og_lake_parquet_sdk_v3(void)
{
    static const OgLakeParquetMaintenanceApi api={OG_LAKE_PARQUET_MAINTENANCE_ABI,
        sizeof(OgLakeParquetMaintenanceApi),og_lake_parquet_sdk_v1(),OG_LAKE_SDK_LOCAL_DURABLE_DELETE,DeleteRetiredFile};
    return &api;
}

extern "C" __attribute__((visibility("default")))
const OgLakeParquetNestedApi* og_lake_parquet_sdk_v4(void)
{
    static const OgLakeParquetNestedApi api={OG_LAKE_PARQUET_NESTED_ABI,
        sizeof(OgLakeParquetNestedApi),og_lake_parquet_sdk_v1(),OG_LAKE_SDK_WRITE_FLOAT32_LIST,WriterOpenNested};
    return &api;
}

extern "C" __attribute__((visibility("default")))
const OgLakeParquetDecimalApi* og_lake_parquet_sdk_v5(void)
{
    static const OgLakeParquetDecimalApi api={OG_LAKE_PARQUET_DECIMAL_ABI,
        sizeof(OgLakeParquetDecimalApi),og_lake_parquet_sdk_v1(),OG_LAKE_SDK_DECIMAL_PARAMETERS,
        FooterDecimal,WriterOpenParameterized};
    return &api;
}

extern "C" __attribute__((visibility("default")))
const OgLakeParquetFdApi* og_lake_parquet_sdk_v6(void)
{
    static const OgLakeParquetFdApi api={OG_LAKE_PARQUET_FD_ABI,
        sizeof(OgLakeParquetFdApi),og_lake_parquet_sdk_v1(),OG_LAKE_SDK_READONLY_FD,FooterOpenFd,ReaderOpenFd};
    return &api;
}
