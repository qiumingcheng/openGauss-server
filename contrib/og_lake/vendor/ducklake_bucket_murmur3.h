/* Adapted from DuckLake 0aaa8e10 common/ducklake_murmur3.hpp.
 * Algorithm constants/body/tail/fmix are preserved. Explicit little-endian
 * loads and signed-int64 serialization replace host-memory representation.
 * Fixed DuckLake x86/ARM little-endian reference and pg_lake Long hash use
 * these bytes. Do not substitute the four-byte integer hash or host hash.
 *
 * Copyright 2018-2025 Stichting DuckDB Foundation
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
#ifndef OG_LAKE_DUCKLAKE_BUCKET_MURMUR3_H
#define OG_LAKE_DUCKLAKE_BUCKET_MURMUR3_H
#include <stdint.h>
#include <stddef.h>
static inline uint32_t OgLakeBucketMurmur3(const uint8_t* data,size_t len)
{
    const size_t nblocks=len/4;uint32_t h1=0;
    const uint32_t c1=UINT32_C(0xcc9e2d51),c2=UINT32_C(0x1b873593);
    for(size_t i=0;i<nblocks;++i){
        const uint8_t* p=data+i*4;
        uint32_t k1=(uint32_t)p[0]|((uint32_t)p[1]<<8)|((uint32_t)p[2]<<16)|((uint32_t)p[3]<<24);
        k1*=c1;k1=(k1<<15)|(k1>>17);k1*=c2;
        h1^=k1;h1=(h1<<13)|(h1>>19);h1=h1*5+UINT32_C(0xe6546b64);
    }
    const uint8_t* tail=data+nblocks*4;uint32_t k1=0;
    if((len&3)>=3)k1^=(uint32_t)tail[2]<<16;
    if((len&3)>=2)k1^=(uint32_t)tail[1]<<8;
    if((len&3)>=1){k1^=(uint32_t)tail[0];k1*=c1;k1=(k1<<15)|(k1>>17);k1*=c2;h1^=k1;}
    h1^=(uint32_t)len;h1^=h1>>16;h1*=UINT32_C(0x85ebca6b);h1^=h1>>13;h1*=UINT32_C(0xc2b2ae35);h1^=h1>>16;
    return h1;
}
static inline uint32_t OgLakeBucketMurmur3Int64(int64_t value)
{
    uint64_t bits=(uint64_t)value;uint8_t bytes[8];
    for(unsigned i=0;i<8;++i)bytes[i]=(uint8_t)(bits>>(8*i));
    return OgLakeBucketMurmur3(bytes,sizeof(bytes));
}
#endif
