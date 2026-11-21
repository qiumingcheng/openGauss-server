/* DuckLake's PostgreSQL metadata backend stores temporal inline columns as
 * VARCHAR. The business tuple remains a native Datum; this boundary is text,
 * whereas Parquet/Arrow uses the separate checked Unix-epoch codec.
 *
 * Based on pinned DuckLake ToSQLString/InlinedDataInsert and DuckDB
 * DateToStringCast/StringFromTimestamp/ICU CastOperation conventions:
 * YYYY-MM-DD [ (BC)] [HH:MM:SS[.ffffff][+HH[:MM]]]. BC belongs to the date
 * component. Timestamp TZ always requires an explicit numeric offset.
 * Write UTC +00; read both small-batch +00 and ICU bulk numeric offsets.
 * No session DateStyle/TimeZone, naked cast or user input function is used.
 */
#ifndef OG_LAKE_TEMPORAL_TEXT_H
#define OG_LAKE_TEMPORAL_TEXT_H
#include "postgres.h"
#include "lake_temporal.h"
#include "executor/tuptable.h"
#include "access/tuptoaster.h"
#include "utils/builtins.h"
#include "utils/datetime.h"

static inline void LakeTemporalTextError(void)
{
    ereport(ERROR,(errcode(ERRCODE_INVALID_DATETIME_FORMAT),
        errmsg("native lake inline temporal value is not a supported DuckLake ISO value")));
}
static inline int LakeTemporalTextDigits(const char** cursor,const char* end,int minimum,int maximum)
{
    const char* p=*cursor;int value=0,count=0;
    while(p<end&&*p>='0'&&*p<='9'&&count<maximum){value=value*10+(*p++-'0');++count;}
    if(count<minimum)LakeTemporalTextError();
    *cursor=p;return value;
}
static inline void LakeTemporalTextTake(const char** cursor,const char* end,char expected)
{
    if(*cursor==end||**cursor!=expected)LakeTemporalTextError();
    ++*cursor;
}
static inline DateADT LakeTemporalTextDate(const char** cursor,const char* end)
{
    const char* p=*cursor;
    int year=LakeTemporalTextDigits(&p,end,4,7);
    LakeTemporalTextTake(&p,end,'-');int month=LakeTemporalTextDigits(&p,end,2,2);
    LakeTemporalTextTake(&p,end,'-');int day=LakeTemporalTextDigits(&p,end,2,2);
    if(year==0||month<1||month>12||day<1||day>31)LakeTemporalTextError();
    if(end-p>=5&&memcmp(p," (BC)",5)==0){year=1-year;p+=5;}
    if(!IS_VALID_JULIAN(year,month,day))
        ereport(ERROR,(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),errmsg("native lake inline date is outside the host calendar range")));
    int julian=date2j(year,month,day),checkYear,checkMonth,checkDay;
    if(julian<DATETIME_MIN_JULIAN||julian>=DATE_END_JULIAN)
        ereport(ERROR,(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),errmsg("native lake inline date is outside the host calendar range")));
    j2date(julian,&checkYear,&checkMonth,&checkDay);
    if(checkYear!=year||checkMonth!=month||checkDay!=day)LakeTemporalTextError();
    DateADT value=(DateADT)(julian-POSTGRES_EPOCH_JDATE);
    (void)LakeTemporalDateToUnix(value);*cursor=p;return value;
}
static inline Datum LakeTemporalParseText(const char* text,int length,Oid type)
{
    const char* p=text;const char* end=text+length;
    if(length<10||length>48||memchr(text,0,length)!=NULL)LakeTemporalTextError();
    DateADT date=LakeTemporalTextDate(&p,end);
    if(type==DATEOID){if(p!=end)LakeTemporalTextError();return DateADTGetDatum(date);}
    if(type!=TIMESTAMPOID&&type!=TIMESTAMPTZOID)
        ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("non-temporal type passed to inline temporal decoder")));
#ifndef HAVE_INT64_TIMESTAMP
    ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake timestamp transport requires integer datetimes")));
    return (Datum)0;
#else
    LakeTemporalTextTake(&p,end,' ');int hour=LakeTemporalTextDigits(&p,end,2,2);
    LakeTemporalTextTake(&p,end,':');int minute=LakeTemporalTextDigits(&p,end,2,2);
    LakeTemporalTextTake(&p,end,':');int second=LakeTemporalTextDigits(&p,end,2,2);
    if(hour>23||minute>59||second>59)LakeTemporalTextError();
    int micros=0;
    if(p<end&&*p=='.'){
        ++p;const char* start=p;micros=LakeTemporalTextDigits(&p,end,1,6);
        int digits=(int)(p-start);while(digits++<6)micros*=10;
        // A seventh digit is rejected, never silently truncated or rounded.
        if(p<end&&*p>='0'&&*p<='9')LakeTemporalTextError();
    }
    int64 offset=0;
    if(type==TIMESTAMPTZOID){
        if(p==end||(*p!='+'&&*p!='-'))LakeTemporalTextError();
        bool negative=*p++=='-';int hours=LakeTemporalTextDigits(&p,end,2,2),minutes=0;
        if(p<end&&*p==':'){++p;minutes=LakeTemporalTextDigits(&p,end,2,2);}
        if(hours>23||minutes>59)LakeTemporalTextError();
        offset=((int64)hours*60+minutes)*INT64CONST(60000000);
        if(negative)offset=-offset;
    }
    if(p!=end)LakeTemporalTextError();
    // tm2timestamp multiplies before checking overflow on this host branch.
    // Check each operation here before performing it, then enforce host/wire
    // finite range after explicit offset normalization.
    const int64 dayMicros=INT64CONST(86400000000);
    if((int64)date>PG_INT64_MAX/dayMicros||(int64)date<PG_INT64_MIN/dayMicros)
        ereport(ERROR,(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),errmsg("native lake inline timestamp overflows microsecond transport")));
    int64 value=(int64)date*dayMicros;
    int64 clock=((int64)hour*3600+minute*60+second)*INT64CONST(1000000)+micros;
    if(value>PG_INT64_MAX-clock)
        ereport(ERROR,(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),errmsg("native lake inline timestamp overflows microsecond transport")));
    value+=clock;
    if((offset>0&&value<PG_INT64_MIN+offset)||(offset<0&&value>PG_INT64_MAX+offset))
        ereport(ERROR,(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),errmsg("native lake inline timestamp offset overflows microsecond transport")));
    value-=offset;
    (void)LakeTemporalTimestampToUnix((Timestamp)value);
    return type==TIMESTAMPTZOID?TimestampTzGetDatum((TimestampTz)value):TimestampGetDatum((Timestamp)value);
#endif
}
static inline Datum LakeTemporalReadText(Datum input,Oid type)
{
    // Bound detoasting before materializing external inline values.
    Size raw=toast_raw_datum_size(input);
    if(raw<VARHDRSZ+10||raw>VARHDRSZ+48)LakeTemporalTextError();
    struct varlena* packed=PG_DETOAST_DATUM_PACKED(input);
    Datum result=LakeTemporalParseText(VARDATA_ANY(packed),VARSIZE_ANY_EXHDR(packed),type);
    if((Pointer)packed!=DatumGetPointer(input))pfree(packed);
    return result;
}
static inline char* LakeTemporalFormatDate(int year,int month,int day)
{
    bool bc=year<=0;if(bc)year=1-year;
    return psprintf("%04d-%02d-%02d%s",year,month,day,bc?" (BC)":"");
}
static inline Datum LakeTemporalWriteText(Datum input,Oid type)
{
    LakeTemporalValidateDatum(type,input);
    char* text=NULL;
    if(type==DATEOID){
        int year,month,day;
        j2date(DatumGetDateADT(input)+POSTGRES_EPOCH_JDATE,&year,&month,&day);
        text=LakeTemporalFormatDate(year,month,day);
    }else{
        if(type!=TIMESTAMPOID&&type!=TIMESTAMPTZOID)
            ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("non-temporal type passed to inline temporal encoder")));
#ifndef HAVE_INT64_TIMESTAMP
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake timestamp transport requires integer datetimes")));
#else
        Timestamp value=type==TIMESTAMPTZOID?DatumGetTimestampTz(input):DatumGetTimestamp(input);
        struct pg_tm tm={};fsec_t fraction=0;
        // tzp=NULL takes the host's calendar-only path; never session timezone.
        if(timestamp2tm(value,NULL,&tm,&fraction,NULL,NULL)!=0)
            ereport(ERROR,(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),errmsg("native lake timestamp cannot be formatted as DuckLake ISO text")));
        char* date=LakeTemporalFormatDate(tm.tm_year,tm.tm_mon,tm.tm_mday);
        char* part=NULL;
        if(fraction){
            part=psprintf(".%06d",(int)fraction);int end=(int)strlen(part)-1;
            while(end>0&&part[end]=='0')part[end--]='\0';
        }
        text=psprintf("%s %02d:%02d:%02d%s%s",date,tm.tm_hour,tm.tm_min,tm.tm_sec,part?part:"",type==TIMESTAMPTZOID?"+00":"");
        pfree(date);if(part)pfree(part);
#endif
    }
    Datum output=CStringGetTextDatum(text);pfree(text);return output;
}
#endif
