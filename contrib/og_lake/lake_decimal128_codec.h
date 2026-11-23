/* Exact numeric(p,s) <-> Arrow decimal128 coefficient bridge.
 * Internal host helper only: no public ABI, allocation, Arrow or DB call.
 * The host applies its normal numeric typmod before encoding. We neither
 * accept NaN/exponents nor round or convert through floating point.
 * Arrow decimal128 buffers contain native-endian signed two's-complement
 * coefficients. Parquet byte order is the SDK/Arrow writer's responsibility.
 */
#ifndef OG_LAKE_DECIMAL128_CODEC_H
#define OG_LAKE_DECIMAL128_CODEC_H
#include <stddef.h>
#include <stdint.h>
#include <string.h>

static_assert(sizeof(unsigned __int128)==16,"decimal128 requires a 128-bit host integer");

static inline bool LakeDecimal128Parameters(int precision,int scale)
{
    return precision>=1&&precision<=38&&scale>=0&&scale<=precision;
}

static inline unsigned __int128 LakeDecimal128Limit(int precision)
{
    unsigned __int128 limit=1;
    for(int i=0;i<precision;++i)limit*=10;
    return limit;
}

/* text is numeric_out_with_zero's bounded ordinary decimal, without NUL.
 * Leading integer digits may be omitted for |x|<1 (e.g. ".0001"); short
 * fractional output is padded exactly; excess fractional digits are rejected,
 * including zeros, rather than silently rescaled or rounded.
 * On failure neither the caller's output nor its prior value is modified.
 */
static inline bool LakeDecimal128Encode(const char* text,size_t length,
    int precision,int scale,unsigned char* output)
{
    if(!text||!output||!LakeDecimal128Parameters(precision,scale)||length==0||length>80)return false;
    size_t position=0;bool negative=false;
    if(text[0]=='-'){negative=true;if(++position==length)return false;}
    unsigned __int128 limit=LakeDecimal128Limit(precision),value=0;
    int fractional=0;bool dot=false,integer=false,fraction_digit=false;
    for(;position<length;++position){
        unsigned char ch=static_cast<unsigned char>(text[position]);
        if(ch=='.'){
            if(dot)return false;
            dot=true;continue;
        }
        if(ch<'0'||ch>'9')return false;
        if(dot){if(++fractional>scale)return false;fraction_digit=true;}
        else integer=true;
        unsigned int digit=ch-'0';
        if(value>(limit-1-digit)/10)return false;
        value=value*10+digit;
    }
    if((!integer&&!fraction_digit)||(dot&&!fraction_digit))return false;
    for(int i=fractional;i<scale;++i){
        if(value>(limit-1)/10)return false;
        value*=10;
    }
    /* Unsigned subtraction defines every two's-complement bit, including zero,
     * without negating the signed minimum or overflowing a signed expression. */
    unsigned __int128 bits=negative?static_cast<unsigned __int128>(0)-value:value;
    memcpy(output,&bits,16);return true;
}

/* output includes the NUL terminator; 48 bytes always suffice for p<=38.
 * No memory is written until input parameters, precision and capacity pass.
 * SQL NULL is represented by the caller's validity bit, never a coefficient.
 */
static inline bool LakeDecimal128Decode(const unsigned char* input,int precision,
    int scale,char* output,size_t capacity)
{
    if(!input||!output||!LakeDecimal128Parameters(precision,scale))return false;
    unsigned __int128 bits;memcpy(&bits,input,16);
    bool negative=(bits>>127)!=0;
    unsigned __int128 value=negative?static_cast<unsigned __int128>(0)-bits:bits;
    if(value>=LakeDecimal128Limit(precision))return false;
    char reverse[38];int digits=0;
    do{reverse[digits++]=static_cast<char>('0'+value%10);value/=10;}while(value!=0);
    int integer_digits=digits>scale?digits-scale:1;
    size_t needed=static_cast<size_t>(negative?1:0)+integer_digits+(scale?scale+1:0)+1;
    if(capacity<needed)return false;
    char temporary[48];size_t at=0;
    if(negative)temporary[at++]='-';
    if(digits>scale)for(int i=digits-1;i>=scale;--i)temporary[at++]=reverse[i];
    else temporary[at++]='0';
    if(scale){
        temporary[at++]='.';
        for(int i=scale-1;i>=0;--i)temporary[at++]=i<digits?reverse[i]:'0';
    }
    temporary[at++]='\0';
    memcpy(output,temporary,at);return true;
}
#endif
