/* JSON is validated text, never JSONB or a floating-point intermediate. */
#ifndef OG_LAKE_JSON_H
#define OG_LAKE_JSON_H
#include "postgres.h"
#include "lake_binary.h"
#include "fmgr.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/json.h"

extern bool LakeJsonBusinessVersionEnabled(void);

/* Existing JSON datums are owned varlena values. Detach before freeing an
 * inline tuple, with the same pre-detoast row budget as BYTEA. */
static inline Datum LakeCopyJsonDatum(Datum value, Size* rowBytes)
{
    return LakeCopyBinaryDatum(value, rowBytes);
}

/* External Parquet JSON needs the host's strict syntax check. Reject embedded
 * NUL before any C string operation; escaped "\u0000" remains ordinary JSON
 * source text. DirectFunctionCall1Coll(...,false) cannot ignore bad input and
 * replace it with JSON null. json_in preserves spaces and duplicate keys. */
static inline Datum LakeJsonFromUtf8Bytes(const char* bytes, Size length,
    Size* rowBytes)
{
    if ((!bytes && length) || length > (Size)MaxAllocSize - VARHDRSZ - 1)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("native lake JSON input length is invalid")));
    if (length && memchr(bytes, 0, length))
        ereport(ERROR, (errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
            errmsg("native lake JSON contains a zero byte")));
    const char* source = length ? bytes : "";
    /* Check the whole row's remaining budget before conversion can allocate.
     * Charge the actual converted payload once, below. */
    Size projectedBytes = rowBytes ? *rowBytes : LAKE_BINARY_ROW_BYTES;
    LakeReserveBinaryBytes(length, &projectedBytes);
    (void)pg_verify_mbstr(PG_UTF8, source, (int)length, false);
    char* converted = pg_any_to_server(source, (int)length, PG_UTF8);
    Size convertedLength = converted == source ? length : strlen(converted);
    LakeReserveBinaryBytes(convertedLength, rowBytes);
    char* terminated = (char*)palloc(convertedLength + 1);
    if (convertedLength) {
        errno_t rc = memcpy_s(terminated, convertedLength + 1, converted,
            convertedLength);
        securec_check(rc, "", "");
    }
    terminated[convertedLength] = '\0';
    Datum result = DirectFunctionCall1Coll(json_in, InvalidOid,
        CStringGetDatum(terminated), false);
    pfree(terminated);
    if (converted != source) pfree(converted);
    return result;
}

/* DuckLake's PostgreSQL metadata backend maps JSON's VARCHAR storage to
 * BYTEA. It contains raw UTF8 JSON bytes, not bytea's printable escape syntax.
 * Check raw size before detoasting an external/compressed inline value. */
static inline Datum LakeJsonReadInline(Datum value, Size* rowBytes)
{
    Size raw = toast_raw_datum_size(value);
    if (raw < VARHDRSZ)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("native lake inline JSON length is invalid")));
    Size projectedBytes = rowBytes ? *rowBytes : LAKE_BINARY_ROW_BYTES;
    LakeReserveBinaryBytes(raw - VARHDRSZ, &projectedBytes);
    struct varlena* bytes = PG_DETOAST_DATUM_PACKED(value);
    Datum result = LakeJsonFromUtf8Bytes(VARDATA_ANY(bytes),
        VARSIZE_ANY_EXHDR(bytes), rowBytes);
    if ((void*)bytes != DatumGetPointer(value)) pfree(bytes);
    return result;
}
#endif
