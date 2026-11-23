/* Private build-variant metadata bridge; frozen PQ v1 sources stay unchanged.
 * This is NOT a lock around training or numeric BLAS operations. */
#ifndef OG_LAKE_PQ_CPU_LOCKING_BLAS_META_H
#define OG_LAKE_PQ_CPU_LOCKING_BLAS_META_H
#include "pq_cpu.h"
#ifdef __cplusplus
extern "C" {
#endif
int pq_cpu_blas_metadata_initialize(PqCpuError*);
const char* pq_cpu_blas_immutable_config(void);
int pq_cpu_blas_parallel_checked(void);
int pq_cpu_blas_num_threads_checked(void);
uint32_t pq_cpu_blas_metadata_init_calls(void);
uint32_t pq_cpu_blas_metadata_source_reads(void);
/* Test-only faults before the first once initialization:1=NULL metadata,
 * 2=oversized metadata,3=allocation exception. Never a replacement PQ backend. */
int pq_cpu_blas_metadata_test_fault(unsigned);
#ifdef __cplusplus
}
#endif
#endif
