/* Fixed16 host UUID ownership; no varlena/TOAST, integer swap or new row ID. */
#ifndef OG_LAKE_UUID_H
#define OG_LAKE_UUID_H
#include "postgres.h"
#include "utils/memutils.h"
#include "utils/uuid.h"

static_assert(UUID_LEN==16&&sizeof(pg_uuid_t)==16,"native UUID must match the SDK UUID16 contract");

extern bool LakeUuidBusinessVersionEnabled(void);
static const Size LAKE_UUID_ROW_BYTES=64UL*1024UL*1024UL;
static inline Datum LakeUuidFromBytes(const void* bytes,Size* rowBytes=NULL)
{
    if(bytes==NULL)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake UUID value buffer is missing")));
    if(rowBytes){
        if(*rowBytes>LAKE_UUID_ROW_BYTES||UUID_LEN>LAKE_UUID_ROW_BYTES-*rowBytes)
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake UUID row exceeds its 64 MiB byte limit")));
        *rowBytes+=UUID_LEN;
    }
    pg_uuid_t* copy=(pg_uuid_t*)palloc(UUID_LEN);
    errno_t rc=memcpy_s(copy->data,UUID_LEN,bytes,UUID_LEN);securec_check(rc,"","");
    return UUIDPGetDatum(copy);
}
static inline Datum LakeCopyUuidDatum(Datum value,Size* rowBytes=NULL)
{
    pg_uuid_t* source=DatumGetUUIDP(value);
    return LakeUuidFromBytes(source?source->data:NULL,rowBytes);
}
/* Statistics are untrusted directory strings. A failed parse means unknown;
 * never call uuid_in/ERROR, locale functions or an executable expression here. */
static inline bool LakeUuidTryStatsBytes(const char* text,pg_uuid_t* value)
{
    if(!text||!value||strnlen(text,37)!=36)return false;
    int byte=0,high=-1;
    for(int i=0;i<36;++i){
        if(i==8||i==13||i==18||i==23){if(text[i]!='-')return false;continue;}
        unsigned char c=(unsigned char)text[i];
        int digit=c>='0'&&c<='9'?c-'0':c>='a'&&c<='f'?c-'a'+10:-1;
        if(digit<0)return false;
        if(high<0)high=digit;
        else{value->data[byte++]=(unsigned char)((high<<4)|digit);high=-1;}
    }
    return byte==UUID_LEN&&high<0;
}
#endif
