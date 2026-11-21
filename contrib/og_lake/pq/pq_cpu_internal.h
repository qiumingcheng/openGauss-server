#ifndef OG_LAKE_PQ_CPU_INTERNAL_H
#define OG_LAKE_PQ_CPU_INTERNAL_H
#include "pq_cpu.h"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>
namespace pq_cpu_ms {
void Poll();
namespace diskann {
/* Private field shell for original numerical methods; not the upstream class
 * ABI, not persisted, and never exposed outside this component. */
class FixedChunkPQTable {
public:
    float* tables_tr=nullptr;
    uint64_t ndims=0,n_chunks=0;
    uint32_t* chunk_offsets=nullptr;
    float* centroid=nullptr;
    void populate_chunk_distances(const float*,float*);
    void populate_chunk_inner_products(const float*,float*);
    float l2_distance(const float*,uint8_t*);
    float inner_product(const float*,uint8_t*);
    void inflate_vector(uint8_t*,float*);
};
int generate_pq_pivots_simplified(const float*,size_t,size_t,size_t,std::vector<float>&);
int generate_pq_data_from_pivots_simplified(const float*,size_t,const float*,size_t,size_t,size_t,std::vector<uint8_t>&);
void pq_dist_lookup(const uint8_t*,size_t,size_t,const float*,float*);
}
}
#endif
