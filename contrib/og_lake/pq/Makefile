# Standalone static PQ component library (symbol-isolated before linking gaussdb).
# Keeps C++ helpers, weak COMDAT groups and OpenBLAS symbols hidden behind the
# checked C ABI in pq_cpu.h, pq_parquet_format.h and pq_parquet_frontier_draft.h.

subdir = contrib/og_lake/pq
top_builddir = ../../..
srcdir = .

ifneq ($(wildcard $(top_builddir)/src/Makefile.global),)
include $(top_builddir)/src/Makefile.global
endif

PQ_BLAS_INCLUDE ?= $(OG_LAKE_PQ_BLAS_INCLUDE)
PQ_BLAS_ARCHIVE ?= $(OG_LAKE_PQ_BLAS_ARCHIVE)
PQ_OPENSSL_INCLUDE ?= $(LIBOPENSSL_INCLUDE_PATH)
PQ_CXX ?= $(if $(strip $(CXX)),$(CXX),g++)
PQ_AR ?= $(if $(strip $(AR)),$(AR),ar)
PQ_RANLIB ?= $(if $(strip $(RANLIB)),$(RANLIB),ranlib)
PQ_OBJCOPY ?= objcopy
PQ_NM ?= nm
PQ_READELF ?= readelf

ifeq ($(strip $(PQ_OPT_FLAGS)),)
ifneq ($(findstring --enable-debug,$(configure_args)),)
PQ_OPT_FLAGS = -O0 -g
else
PQ_OPT_FLAGS = -O2
endif
endif

OG_LAKE_DIR = $(srcdir)/..
PQ_OBJS = pq_cpu.o pq_cpu_ms.o pq_cpu_locking_blas_meta.o pq_parquet_format.o pq_parquet_frontier_draft.o
PQ_ARCHIVE = libog_lake_pq_cpu.a
PQ_SYMBOL_MAP = pq-symbol-map.log

PQ_COMMON_CXXFLAGS = -std=c++14 $(PQ_OPT_FLAGS) -fPIC \
	-fvisibility=hidden -fvisibility-inlines-hidden \
	-I$(OG_LAKE_DIR) -I$(srcdir) -I$(PQ_BLAS_INCLUDE) \
	$(if $(strip $(PQ_OPENSSL_INCLUDE)),-I$(PQ_OPENSSL_INCLUDE)) \
	-include $(srcdir)/pq_cpu_locking_blas_meta.h \
	-Dcblas_sgemm=oglpq_cblas_sgemm \
	-Dcblas_snrm2=oglpq_cblas_snrm2 \
	-Dopenblas_get_config=pq_cpu_blas_immutable_config \
	-Dopenblas_get_parallel=pq_cpu_blas_parallel_checked \
	-Dopenblas_get_num_threads=pq_cpu_blas_num_threads_checked

.PHONY: all clean check-blas-inputs

all: $(PQ_ARCHIVE)

check-blas-inputs:
	@if [ -z "$(strip $(PQ_BLAS_INCLUDE))" ] || [ ! -f "$(PQ_BLAS_INCLUDE)/cblas.h" ] || [ ! -f "$(PQ_BLAS_INCLUDE)/openblas_config.h" ]; then \
		echo "ERROR: OG_LAKE_PQ_BLAS_INCLUDE / PQ_BLAS_INCLUDE must point to OpenBLAS headers (cblas.h, openblas_config.h)" >&2; \
		exit 1; \
	fi
	@if [ -z "$(strip $(PQ_BLAS_ARCHIVE))" ] || [ ! -f "$(PQ_BLAS_ARCHIVE)" ]; then \
		echo "ERROR: OG_LAKE_PQ_BLAS_ARCHIVE / PQ_BLAS_ARCHIVE must point to libog_lake_pq_openblas.a" >&2; \
		exit 1; \
	fi

pq_cpu.o: $(srcdir)/pq_cpu.cpp $(srcdir)/pq_cpu.h $(srcdir)/pq_cpu_internal.h $(srcdir)/pq_cpu_locking_blas_meta.h | check-blas-inputs
	$(PQ_CXX) $(PQ_COMMON_CXXFLAGS) -c $< -o $@

pq_cpu_ms.o: $(OG_LAKE_DIR)/vendor/diskann_pq/pq_cpu_ms.cpp $(srcdir)/pq_cpu.h $(srcdir)/pq_cpu_internal.h $(srcdir)/pq_cpu_locking_blas_meta.h | check-blas-inputs
	$(PQ_CXX) $(PQ_COMMON_CXXFLAGS) -c $< -o $@

pq_cpu_locking_blas_meta.o: $(srcdir)/pq_cpu_locking_blas_meta.cpp $(srcdir)/pq_cpu.h $(srcdir)/pq_cpu_locking_blas_meta.h | check-blas-inputs
	$(PQ_CXX) $(PQ_COMMON_CXXFLAGS) -c $< -o $@

pq_parquet_format.o: $(srcdir)/pq_parquet_format.cpp $(srcdir)/pq_parquet_format.h $(srcdir)/pq_cpu.h $(srcdir)/pq_cpu_locking_blas_meta.h $(OG_LAKE_DIR)/sdk/lake_parquet_sdk.h $(OG_LAKE_DIR)/vendor/arrow20/abi.h | check-blas-inputs
	$(PQ_CXX) $(PQ_COMMON_CXXFLAGS) -c $< -o $@

pq_parquet_frontier_draft.o: $(srcdir)/pq_parquet_frontier_draft.cpp $(srcdir)/pq_parquet_frontier_draft.h $(srcdir)/pq_parquet_format.h $(srcdir)/pq_cpu.h $(srcdir)/pq_cpu_locking_blas_meta.h $(OG_LAKE_DIR)/sdk/lake_parquet_sdk.h | check-blas-inputs
	$(PQ_CXX) $(PQ_COMMON_CXXFLAGS) -c $< -o $@

$(PQ_ARCHIVE): $(PQ_OBJS) $(srcdir)/isolate_pq_symbols.sh $(PQ_BLAS_ARCHIVE)
	rm -f $@.staging
	$(PQ_AR) rcs $@.staging $(PQ_OBJS)
	NM='$(PQ_NM)' READELF='$(PQ_READELF)' bash $(srcdir)/isolate_pq_symbols.sh $@.staging $(PQ_SYMBOL_MAP) '$(PQ_OBJCOPY)' '$(PQ_RANLIB)'
	mv -f $@.staging $@

clean:
	rm -f $(PQ_OBJS) $(PQ_ARCHIVE) $(PQ_ARCHIVE).staging $(PQ_SYMBOL_MAP)
