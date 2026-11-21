/* Native file-provider boundary. No C++ or database objects cross this ABI. */
#ifndef OG_LAKE_PARQUET_SDK_H
#define OG_LAKE_PARQUET_SDK_H
#include <stddef.h>
#include <stdint.h>
#define OG_LAKE_SDK_FIELD_END(type,member) (offsetof(type,member)+sizeof(((type*)0)->member))
#ifdef __cplusplus
extern "C" {
#endif

#define OG_LAKE_PARQUET_ABI 1u
#define OG_LAKE_SDK_ERROR_BYTES 512
struct ArrowArray;
struct ArrowSchema;
typedef struct OgLakeSdkFooter OgLakeSdkFooter;
typedef struct OgLakeSdkReader OgLakeSdkReader;
typedef struct OgLakeSdkWriter OgLakeSdkWriter;

enum OgLakeSdkStatus {
    OG_LAKE_SDK_OK=0, OG_LAKE_SDK_EOF=1, OG_LAKE_SDK_INVALID=2,
    OG_LAKE_SDK_IO=3, OG_LAKE_SDK_MEMORY=4, OG_LAKE_SDK_CANCELLED=5,
    OG_LAKE_SDK_UNSUPPORTED=6, OG_LAKE_SDK_STATE=7
};
enum OgLakeSdkType {
    OG_LAKE_SDK_BOOL=1, OG_LAKE_SDK_INT32=2, OG_LAKE_SDK_INT64=3,
    OG_LAKE_SDK_UTF8=4, OG_LAKE_SDK_FLOAT32=5, OG_LAKE_SDK_FLOAT64=6,
    OG_LAKE_SDK_FLOAT32_LIST=7, OG_LAKE_SDK_BINARY=8,
    OG_LAKE_SDK_INT16=9,
    /* Finite temporal values use the Unix1970 epoch, never host2000 datums.
     * The three types are distinct from int32/int64 even at equal width. */
    OG_LAKE_SDK_DATE32=10, OG_LAKE_SDK_TIMESTAMP_US=11,
    OG_LAKE_SDK_TIMESTAMPTZ_US=12,
    /* Standard UUID, not text/BINARY or a host-endian integer pair. */
    OG_LAKE_SDK_UUID=13,
    /* Physical Parquet JSON, transported as exact UTF8 text (not JSONB). */
    OG_LAKE_SDK_JSON=14,
    /* Parameters are available through optional ABI5, never Field suffixes. */
    OG_LAKE_SDK_DECIMAL128=15,
    OG_LAKE_SDK_OTHER=255
};
enum OgLakeSdkCompression {
    OG_LAKE_SDK_UNCOMPRESSED=0, OG_LAKE_SDK_ZSTD=1, OG_LAKE_SDK_SNAPPY=2,
    OG_LAKE_SDK_GZIP=3, OG_LAKE_SDK_LZ4=4
};
enum OgLakeSdkProjection {
    OG_LAKE_SDK_BY_FIELD_ID=0,
    OG_LAKE_SDK_BY_ORDINAL=1 /* Only after the host resolves a standard mapping. */
};
typedef struct OgLakeSdkError {
    int32_t code;
    int32_t system_errno;
    char message[OG_LAKE_SDK_ERROR_BYTES]; /* Caller-owned, always terminated. */
} OgLakeSdkError;

/* Polling only: callback must not allocate through or raise ERROR in the host.
 * Local synchronous providers invoke it only on the calling thread. */
typedef int (*OgLakeSdkShouldCancel)(void* context);
typedef struct OgLakeSdkLimits {
    uint32_t struct_size;
    int64_t memory_bytes;
    int64_t footer_bytes;
    OgLakeSdkShouldCancel should_cancel;
    void* cancel_context;
} OgLakeSdkLimits;
typedef struct OgLakeSdkField {
    int64_t field_id; /* Exact Parquet field_id; absent ID is -1. */
    uint32_t type;
    uint32_t nullable;
    const char* name; /* Owned by footer/writer until its close. */
} OgLakeSdkField;
typedef struct OgLakeSdkListField {
    int64_t field_id;
    int64_t element_field_id;
    uint32_t element_nullable;
} OgLakeSdkListField;
typedef struct OgLakeSdkColumnChunk {
    int64_t offset;
    int64_t compressed_bytes;
    int64_t uncompressed_bytes;
} OgLakeSdkColumnChunk;
typedef struct OgLakeSdkRowGroup {
    int64_t first_row;
    int64_t num_rows;
    const OgLakeSdkColumnChunk* columns;
} OgLakeSdkRowGroup;
typedef struct OgLakeSdkFooterInfo {
    uint32_t struct_size;
    int32_t num_fields;
    int32_t num_row_groups;
    int64_t num_rows;
    int64_t file_bytes;
    const OgLakeSdkField* fields;
    const OgLakeSdkRowGroup* row_groups;
    /* Optional ABI1 suffix, guarded by struct_size and FOOTER_BYTES.
     * Original serialized Thrift footer length; excludes trailing length/magic. */
    int64_t footer_bytes;
    /* Optional ABI1 suffix, guarded by struct_size and OBJECT_VERSION.
     * Opaque immutable-object version identity, not a content checksum.
     * Owned by the footer until footer_close. */
    const char* object_version;
    /* Optional ABI1 suffix, guarded by struct_size and LIST_FIELD_IDS.
     * Logical LIST parent/element identities; field-array stride is unchanged.
     * IDs may be -1 when absent. Owned by the footer until footer_close. */
    int32_t num_list_fields;
    const OgLakeSdkListField* list_fields;
} OgLakeSdkFooterInfo;
typedef struct OgLakeSdkIoStats {
    uint64_t read_calls;
    uint64_t bytes_read;
    uint64_t decoded_rows;
    int64_t pool_bytes;
    int64_t peak_pool_bytes;
} OgLakeSdkIoStats;
typedef struct OgLakeSdkReadOptions {
    uint32_t struct_size;
    const char* path;
    OgLakeSdkLimits limits;
    int64_t batch_rows;
    uint32_t projection_kind;
    int32_t num_field_ids; /* -1=all, 0=none, positive=exact projection. */
    const int64_t* field_ids;
    int32_t num_row_groups; /* -1=all, 0=empty selection. */
    const int32_t* row_groups;
} OgLakeSdkReadOptions;
typedef struct OgLakeSdkBatch {
    uint32_t struct_size;
    int64_t num_rows;
    struct ArrowArray* array;
    struct ArrowSchema* schema;
    const int64_t* original_positions; /* Absolute, before any host filtering. */
    void* private_data;
    /* Optional ABI1 suffix with the exact reader object's version identity.
     * Owned by this batch, including after reader_close. */
    const char* object_version;
} OgLakeSdkBatch;
typedef struct OgLakeSdkWriteOptions {
    uint32_t struct_size;
    const char* path; /* Absolute new path. Existing objects never overwritten. */
    OgLakeSdkLimits limits;
    int32_t num_fields;
    const OgLakeSdkField* fields;
    uint32_t compression;
    int64_t row_group_rows;
} OgLakeSdkWriteOptions;
typedef struct OgLakeSdkWrittenFile {
    uint32_t struct_size;
    uint32_t local_fsync_complete; /* Local data+directory, not cluster ACK. */
    int64_t num_rows;
    int64_t file_bytes;
} OgLakeSdkWrittenFile;
enum OgLakeSdkCapability {
    OG_LAKE_SDK_LOCAL_FILES=1u<<0, OG_LAKE_SDK_FIELD_IDS=1u<<1,
    OG_LAKE_SDK_ORIGINAL_POSITIONS=1u<<2, OG_LAKE_SDK_WRITE_ORDER=1u<<3,
    OG_LAKE_SDK_IO_STATS=1u<<4, OG_LAKE_SDK_POOL_BUDGET=1u<<5,
    OG_LAKE_SDK_POLL_CANCEL=1u<<6, OG_LAKE_SDK_WRITER=1u<<7,
    OG_LAKE_SDK_FOOTER_BYTES=1u<<8, OG_LAKE_SDK_OBJECT_VERSION=1u<<9,
    /* Flat IEEE float32/float64 fields, Arrow f/g, preserving value bits.
     * This does not admit nested/list vectors or native lake SQL types. */
    OG_LAKE_SDK_FLOAT_VALUES=1u<<10,
    /* Standard one-level LIST<FLOAT32>, Arrow +l/f. Parent field_id is
     * returned in the unchanged footer field array. Writer child IDs are
     * supplied by optional ABI4; ABI1 writer_open rejects list fields. */
    OG_LAKE_SDK_FLOAT32_LIST_VALUES=1u<<11,
    OG_LAKE_SDK_LIST_FIELD_IDS=1u<<12,
    /* Opaque bytes, Arrow z/Z and Parquet BYTE_ARRAY without UTF8 annotation.
     * BINARY writer input is Arrow z. Inspect this bit before requesting type8;
     * existing struct layouts and UTF8 validation remain unchanged. This is a
     * transport codec, not a native lake SQL bytea or index-format contract. */
    OG_LAKE_SDK_BINARY_VALUES=1u<<13,
    /* Signed int16, Arrow s, Parquet INT32 with signed16 logical annotation.
     * Type9 requires this capability; all ABI structs/tables are unchanged. */
    OG_LAKE_SDK_INT16_VALUES=1u<<14,
    /* Independent opt-in ABI1 codecs; every existing struct/table is unchanged.
     * DATE32: signed days since1970-01-01, Arrow tdD, Parquet INT32/DATE.
     * TIMESTAMP_US: civil microseconds since1970, no session-zone conversion;
     * Arrow tsu: and Parquet INT64/TIMESTAMP(MICROS,isAdjustedToUTC=false).
     * TIMESTAMPTZ_US: UTC microseconds since1970; Arrow tsu:UTC and Parquet
     * INT64/TIMESTAMP(MICROS,isAdjustedToUTC=true). Named Arrow timezones,
     * other timestamp units, DATE64 and INT96 are not admitted by these bits.
     * MIN, -MAX and MAX temporal wire sentinels are rejected; these capabilities
     * do not promise infinities, all host ranges, SQL types or partition rules.
     * A consumer must check the corresponding bit before requesting types10..12.
     */
    OG_LAKE_SDK_DATE32_VALUES=1u<<15,
    OG_LAKE_SDK_TIMESTAMP_US_VALUES=1u<<16,
    OG_LAKE_SDK_TIMESTAMPTZ_US_VALUES=1u<<17,
    /* UUID requires this opt-in codec bit. Parquet is UUID logical annotation
     * on FIXED_LEN_BYTE_ARRAY(16); bare fixed16/BINARY/UTF8 is not UUID.
     * The existing batch ABI carries Arrow w:16 with exactly validity + data
     * buffers. NonNULL row i occupies 16 bytes at data+16*(offset+i), in
     * canonical/network hex order; no byte swap or UUID-version restriction.
     * Input buffers must have the valid capacity required by Arrow C Data;
     * writer_append borrows them only for the call. Returned batch ownership,
     * row positions and release-after-reader-close rules are unchanged.
     * No row-value struct, field stride or API-table layout is appended. */
    OG_LAKE_SDK_UUID_VALUES=1ull<<18,
    /* Physical BYTE_ARRAY/JSON is required independently of Arrow metadata;
     * ordinary UTF8 is type4, never type14. Arrow C Data uses u with validity,
     * int32 offsets and UTF8 bytes; canonical arrow.json(utf8()) metadata may
     * accompany reader batches. Footer type14 is the semantic authority.
     * writer_append accepts u or canonical arrow.json/utf8, borrowing buffers
     * only during the call and preserving every byte, whitespace and key.
     * SQL NULL is distinct from nonNULL text "null". Raw NUL, malformed UTF8,
     * offset overflow and per-JSON-column byte-budget overflow are rejected.
     * Borrowed batches are caller-owned: this codec does not impose one
     * aggregate budget on all borrowed columns. The original global pool
     * still bounds provider-owned allocations; host builders have separate
     * combined limits. This is a
     * transport codec: strict complete JSON document syntax is checked by
     * the host's json input function, never by a converting SDK JSON parser.
     * No public struct, API table, field stride or ABI version changes. */
    OG_LAKE_SDK_JSON_VALUES=1ull<<19,
    /* Standard finite decimal(p,s): 1<=p<=38 and 0<=s<=p. Consumers must
     * also obtain ABI5 and its parameter capability, including for empty and
     * all-NULL fields. Arrow d:p,s (or explicit ,128) carries signed native-
     * endian 16-byte two's-complement coefficients; no float conversion or
     * scale rounding. Physical Parquet DECIMAL and matching Arrow parameters
     * are independently required. This slice reads INT32(p<=9), INT64(p<=18)
     * and sufficiently wide FLBA(1..16). BYTE_ARRAY and decimal32/64/256 are
     * inspectable as OTHER and rejected on value projection.
     * ABI1/4 writer_open cannot guess decimal parameters and rejects type15.
     * All original public structures and array strides remain unchanged. */
    OG_LAKE_SDK_DECIMAL128_VALUES=1ull<<20
};

/* All returned errors are explicit. EOF is distinct from a zero-row batch.
 * Close/release are synchronous, safe for NULL and partial initialization.
 * Reader owns at most one outstanding batch; release it before reading again.
 * An outstanding batch retains its buffers and pool after reader_close and must
 * still be released. Reader close stops I/O; no asynchronous callback survives.
 * writer_append borrows Arrow input only for that call and preserves row order;
 * it must not steal input release callbacks or retain borrowed buffers.
 * Finish closes and fsyncs output; a failure cannot be published. writer_close
 * tries to discard an unfinished object, retaining a successfully finished one;
 * cleanup I/O failure is returned so the host can retain its orphan record.
 * No function may throw C++ exceptions or call the database error mechanism. */
typedef struct OgLakeParquetSdkApi {
    uint32_t abi_version;
    uint32_t struct_size;
    const char* provider_name;
    const char* provider_version;
    uint64_t capabilities;
    int (*footer_open)(const char*,const OgLakeSdkLimits*,OgLakeSdkFooter**,OgLakeSdkError*);
    const OgLakeSdkFooterInfo* (*footer_info)(const OgLakeSdkFooter*);
    void (*footer_close)(OgLakeSdkFooter**);
    int (*reader_open)(const OgLakeSdkReadOptions*,OgLakeSdkReader**,OgLakeSdkError*);
    int (*reader_next)(OgLakeSdkReader*,OgLakeSdkBatch**,OgLakeSdkError*);
    void (*batch_release)(OgLakeSdkBatch**);
    void (*reader_stats)(const OgLakeSdkReader*,OgLakeSdkIoStats*);
    void (*reader_close)(OgLakeSdkReader**);
    /* Writer entries are NULL unless OG_LAKE_SDK_WRITER is advertised. */
    int (*writer_open)(const OgLakeSdkWriteOptions*,OgLakeSdkWriter**,OgLakeSdkError*);
    int (*writer_append)(OgLakeSdkWriter*,const struct ArrowArray*,const struct ArrowSchema*,OgLakeSdkError*);
    int (*writer_finish)(OgLakeSdkWriter*,OgLakeSdkWrittenFile*,OgLakeSdkError*);
    int (*writer_close)(OgLakeSdkWriter**,OgLakeSdkError*);
} OgLakeParquetSdkApi;
typedef const OgLakeParquetSdkApi* (*OgLakeParquetSdkEntrypoint)(void);
const OgLakeParquetSdkApi* og_lake_parquet_sdk_v1(void);

/* Optional lifecycle ABI: v1 stays unchanged for existing consumers.
 * Abort is only for a caller-owned candidate which was never published, or
 * whose catalog transaction has certainly aborted. Finished files remain
 * intact under v1 writer_close; only this explicit abort discards them.
 * It consumes the handle on success/error and reports cleanup I/O failures. */
#define OG_LAKE_PARQUET_LIFECYCLE_ABI 2u
#define OG_LAKE_SDK_ABORT_FINISHED (1ull<<0)
typedef struct OgLakeParquetLifecycleApi {
    uint32_t abi_version;
    uint32_t struct_size;
    const OgLakeParquetSdkApi* base;
    uint64_t capabilities;
    int (*writer_abort)(OgLakeSdkWriter**,OgLakeSdkError*);
} OgLakeParquetLifecycleApi;
typedef const OgLakeParquetLifecycleApi* (*OgLakeParquetLifecycleEntrypoint)(void);
const OgLakeParquetLifecycleApi* og_lake_parquet_sdk_v2(void);

/* Optional synchronous maintenance ABI. The host proves catalog retirement,
 * snapshot/backup safety and exclusive protection before calling. This API
 * does no catalog work. Local deletion rejects aliases and nonregular files;
 * success includes a durable parent-directory sync. Missing files are an
 * idempotent success. On any error the host must retain its pending row. */
#define OG_LAKE_PARQUET_MAINTENANCE_ABI 3u
#define OG_LAKE_SDK_LOCAL_DURABLE_DELETE (1ull<<0)
typedef struct OgLakeParquetMaintenanceApi {
    uint32_t abi_version;
    uint32_t struct_size;
    const OgLakeParquetSdkApi* base;
    uint64_t capabilities;
    int (*delete_file)(const char* root,const char* relative_path,
        const OgLakeSdkLimits*,uint32_t* removed,OgLakeSdkError*);
} OgLakeParquetMaintenanceApi;
typedef const OgLakeParquetMaintenanceApi* (*OgLakeParquetMaintenanceEntrypoint)(void);
const OgLakeParquetMaintenanceApi* og_lake_parquet_sdk_v3(void);

/* Optional nested-schema writer ABI. ABI1/2/3 layouts and field-array stride
 * stay unchanged. LIST child IDs come from the standard catalog, never from
 * parent_id+1 or an invented fixed-dimension Parquet encoding. This codec
 * accepts empty/NULL lists and NULL elements; vector(n) validation is the
 * host's separate responsibility. Child name is the standard "element". */
#define OG_LAKE_PARQUET_NESTED_ABI 4u
#define OG_LAKE_SDK_WRITE_FLOAT32_LIST (1ull<<0)
typedef struct OgLakeParquetNestedApi {
    uint32_t abi_version;
    uint32_t struct_size;
    const OgLakeParquetSdkApi* base;
    uint64_t capabilities;
    int (*writer_open)(const OgLakeSdkWriteOptions*,int32_t num_lists,
        const OgLakeSdkListField*,OgLakeSdkWriter**,OgLakeSdkError*);
} OgLakeParquetNestedApi;
typedef const OgLakeParquetNestedApi* (*OgLakeParquetNestedEntrypoint)(void);
const OgLakeParquetNestedApi* og_lake_parquet_sdk_v4(void);


/* Optional parameterized-decimal ABI. Ordinals refer to the unchanged
 * FooterInfo.fields vector, including fields without Parquet IDs (-1).
 * Getter output is caller-owned, with struct_size initialized by the caller.
 * writer_open requires exactly one descriptor for each decimal field, none
 * for other fields, plus the original independent LIST descriptors. Neither
 * sidecar survives the call; writer_append/finish/close use the base API and
 * all original cancellation, candidate, thread and ownership rules apply. */
#define OG_LAKE_PARQUET_DECIMAL_ABI 5u
#define OG_LAKE_SDK_DECIMAL_PARAMETERS (1ull<<0)
typedef struct OgLakeSdkDecimalField {
    uint32_t struct_size;
    int32_t field_ordinal;
    int32_t precision;
    int32_t scale;
} OgLakeSdkDecimalField;
typedef struct OgLakeParquetDecimalApi {
    uint32_t abi_version;
    uint32_t struct_size;
    const OgLakeParquetSdkApi* base;
    uint64_t capabilities;
    int (*footer_decimal)(const OgLakeSdkFooter*,int32_t field_ordinal,
        OgLakeSdkDecimalField*,OgLakeSdkError*);
    int (*writer_open)(const OgLakeSdkWriteOptions*,int32_t num_lists,
        const OgLakeSdkListField*,int32_t num_decimals,
        const OgLakeSdkDecimalField*,OgLakeSdkWriter**,OgLakeSdkError*);
} OgLakeParquetDecimalApi;
typedef const OgLakeParquetDecimalApi* (*OgLakeParquetDecimalEntrypoint)(void);
const OgLakeParquetDecimalApi* og_lake_parquet_sdk_v5(void);

#ifdef __cplusplus
}
#endif
#endif
