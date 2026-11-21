# Statically linked backend objects, following contrib/file_fdw.
subdir = contrib/og_lake
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

OBJS = lake_ops.o lake_alter_type.o lake_cleanup.o lake_bind_snapshot.o lake_table_changes.o lake_time_travel.o lake_snapshot_info.o lake_snapshot_expiry.o lake_column_layout.o lake_drop_column.o lake_default_ddl.o lake_nullability.o lake_nullability_ddl.o lake_column_default.o lake_column_ddl.o lake_inline_sources.o lake_native_partition_analyze.o lake_native_partition_modify.o lake_native_partition_scan.o lake_native_partition_write.o lake_native_partition_bind.o lake_native_partition.o lake_partition_ddl.o lake_partition.o lake_partition_prune.o lake_cost.o lake_autoanalyze.o lake_stream.o lake_sample.o og_lake.o lake_maintenance.o lake_index_maintenance.o lake_host.o lake_spi.o lake_acl.o lake_catalog.o lake_options.o lake_rename.o lake_drop.o lake_index.o lake_inline_index.o lake_inline_ann.o lake_file_scalar.o lake_scalar_format.o lake_ann_format.o lake_file_ann.o lake_ann_export.o lake_ann_build.o lake_file_ann_catalog.o lake_business_create.o lake_create_publish.o lake_inline_scan.o lake_inline_reader.o lake_file_sdk.o lake_file_catalog.o lake_file_reader.o lake_arrow_builder.o lake_vector_codec.o lake_flush.o lake_flush_path.o lake_flush_file_deletes.o lake_analyze.o lake_txn.o lake_journal_payload.o lake_modify.o lake_publish.o lake_test_hooks.o lake_locator.o lake_locator_plan.o lake_row_lock.o lake_file_pq.o lake_rewrite.o lake_ann_rescan.o lake_external_path.o lake_external_fdw.o lake_copy.o
DATA = og_lake--0.74.sql og_lake--0.73--0.74.sql og_lake--0.73.sql og_lake--0.72--0.73.sql og_lake--0.72.sql og_lake--0.71--0.72.sql og_lake--0.71.sql og_lake--0.70--0.71.sql og_lake--0.70.sql og_lake--0.69--0.70.sql og_lake--0.69.sql og_lake--0.68--0.69.sql og_lake--0.68.sql og_lake--0.67--0.68.sql og_lake--0.67.sql og_lake--0.66--0.67.sql og_lake--0.66.sql og_lake--0.65--0.66.sql og_lake--0.65.sql og_lake--0.64--0.65.sql og_lake--0.64.sql og_lake--0.63--0.64.sql og_lake--0.63.sql og_lake--0.62--0.63.sql og_lake--0.62.sql og_lake--0.61--0.62.sql og_lake--0.61.sql og_lake--0.60--0.61.sql og_lake--0.60.sql og_lake--0.59--0.60.sql og_lake--0.59.sql og_lake--0.58--0.59.sql og_lake--0.58.sql og_lake--0.57--0.58.sql og_lake--0.57.sql og_lake--0.56--0.57.sql og_lake--0.56.sql og_lake--0.55--0.56.sql og_lake--0.55.sql og_lake--0.54--0.55.sql og_lake--0.54.sql og_lake--0.53--0.54.sql og_lake--0.53.sql og_lake--0.52--0.53.sql og_lake--0.52.sql og_lake--0.51--0.52.sql og_lake--0.51.sql og_lake--0.50--0.51.sql og_lake--0.50.sql og_lake--0.49--0.50.sql og_lake--0.49.sql og_lake--0.48--0.49.sql og_lake--0.48.sql og_lake--0.47--0.48.sql og_lake--0.47.sql og_lake--0.46--0.47.sql og_lake--0.46.sql og_lake--0.45--0.46.sql og_lake--0.45.sql og_lake--0.44--0.45.sql og_lake--0.44.sql og_lake--0.43--0.44.sql og_lake--0.43.sql og_lake--0.42--0.43.sql og_lake--0.42.sql og_lake--0.41--0.42.sql og_lake--0.41.sql og_lake--0.40--0.41.sql og_lake--0.40.sql og_lake--0.39--0.40.sql og_lake--0.39.sql og_lake--0.38--0.39.sql og_lake--0.38.sql og_lake--0.37--0.38.sql og_lake--0.37.sql og_lake--0.36--0.37.sql og_lake--0.36.sql og_lake--0.35--0.36.sql og_lake--0.35.sql og_lake--0.34--0.35.sql og_lake--0.34.sql og_lake--0.33--0.34.sql og_lake--0.33.sql og_lake--0.32--0.33.sql og_lake--0.32.sql og_lake--0.31--0.32.sql og_lake--0.31.sql og_lake--0.30--0.31.sql og_lake--0.30.sql og_lake--0.29--0.30.sql og_lake--0.29.sql og_lake--0.26--0.29.sql og_lake--0.26.sql og_lake--0.25--0.26.sql og_lake--0.24.sql og_lake--0.23--0.24.sql og_lake--0.25.sql og_lake--0.24--0.25.sql og_lake--0.23.sql og_lake--0.22--0.23.sql og_lake--0.22.sql og_lake--0.21--0.22.sql og_lake--0.21.sql og_lake.control og_lake--0.20.sql og_lake--0.19--0.20.sql og_lake--0.1.sql og_lake--0.2.sql og_lake--0.1--0.2.sql og_lake--0.3.sql og_lake--0.2--0.3.sql og_lake--0.4.sql og_lake--0.3--0.4.sql og_lake--0.5.sql og_lake--0.4--0.5.sql og_lake--0.6.sql og_lake--0.5--0.6.sql og_lake--0.7.sql og_lake--0.6--0.7.sql og_lake--0.8.sql og_lake--0.7--0.8.sql og_lake--0.9.sql og_lake--0.8--0.9.sql og_lake--0.10.sql og_lake--0.9--0.10.sql og_lake--0.11.sql og_lake--0.10--0.11.sql og_lake--0.12.sql og_lake--0.11--0.12.sql og_lake--0.13.sql og_lake--0.12--0.13.sql og_lake--0.14.sql og_lake--0.13--0.14.sql og_lake--0.15.sql og_lake--0.14--0.15.sql og_lake--0.16.sql og_lake--0.15--0.16.sql og_lake--0.17.sql og_lake--0.16--0.17.sql og_lake--0.18.sql og_lake--0.19.sql og_lake--0.18--0.19.sql og_lake--0.17--0.18.sql og_lake--0.20--0.21.sql

DATA += licenses/duckdb_radix_LICENSE

# Native bitmap uses a private, qualified static CRoaring dependency.  The
# source-scoped build driver supplies both paths after checking its receipt.
ifneq ($(strip $(OG_LAKE_ROARING_INCLUDE)$(OG_LAKE_ROARING_ARCHIVE)),)
ifeq ($(strip $(OG_LAKE_ROARING_INCLUDE)),)
$(error OG_LAKE_ROARING_INCLUDE is required with OG_LAKE_ROARING_ARCHIVE)
endif
ifeq ($(wildcard $(OG_LAKE_ROARING_INCLUDE)/roaring/roaring.h),)
$(error Native bitmap CRoaring headers are missing)
endif
ifeq ($(wildcard $(OG_LAKE_ROARING_ARCHIVE)),)
$(error Native bitmap CRoaring static archive is missing)
endif
ifneq ($(words $(wildcard $(srcdir)/lake_bitmap_codec.cpp $(srcdir)/lake_bitmap_format.cpp)),2)
$(error Native bitmap implementation sources are missing)
endif
OBJS += lake_bitmap_codec.o lake_bitmap_format.o
override CPPFLAGS := -DUSE_LAKE_BITMAP -I$(OG_LAKE_ROARING_INCLUDE) $(CPPFLAGS)
endif

# Optional PQ can be built in-tree from contrib/og_lake/pq + vendor/diskann_pq
# (OG_LAKE_PQ_IN_TREE=1 with OpenBLAS paths) or admitted via prebuilt archive
# (build_driver --pq-build). Clearing both keeps raw-graph build independent.
PQ_IN_TREE_ACTIVE := $(filter 1 yes true,$(OG_LAKE_PQ_IN_TREE))
ifneq ($(strip $(OG_LAKE_PQ_ARCHIVE)),)
PQ_IN_TREE_ACTIVE :=
endif

ifneq ($(strip $(PQ_IN_TREE_ACTIVE)),)
ifeq ($(strip $(OG_LAKE_PQ_BLAS_INCLUDE)),)
$(error Native in-tree PQ requires OG_LAKE_PQ_BLAS_INCLUDE)
endif
ifeq ($(strip $(OG_LAKE_PQ_BLAS_ARCHIVE)),)
$(error Native in-tree PQ requires OG_LAKE_PQ_BLAS_ARCHIVE)
endif
ifneq ($(notdir $(OG_LAKE_PQ_BLAS_ARCHIVE)),libog_lake_pq_openblas.a)
$(error Native in-tree PQ requires private libog_lake_pq_openblas.a)
endif
ifeq ($(wildcard $(OG_LAKE_PQ_BLAS_INCLUDE)/cblas.h $(OG_LAKE_PQ_BLAS_INCLUDE)/openblas_config.h $(OG_LAKE_PQ_BLAS_ARCHIVE)),)
$(error Native in-tree PQ OpenBLAS headers or archive are missing)
endif
PQ_EFFECTIVE_INCLUDE := $(srcdir)/pq
PQ_EFFECTIVE_ARCHIVE := $(top_builddir)/contrib/og_lake/pq/libog_lake_pq_cpu.a
PQ_EFFECTIVE_LICENSE := $(srcdir)/vendor/diskann_pq/LICENSE
PQ_EFFECTIVE_BLAS_LICENSE_DIR := $(if $(strip $(OG_LAKE_PQ_BLAS_LICENSES)),$(OG_LAKE_PQ_BLAS_LICENSES),$(OG_LAKE_PQ_BLAS_INCLUDE)/../licenses)
PQ_IN_TREE_DEPS := $(wildcard $(srcdir)/pq/*.cpp $(srcdir)/pq/*.h $(srcdir)/vendor/diskann_pq/pq_cpu_ms.cpp $(srcdir)/sdk/lake_parquet_sdk.h $(srcdir)/vendor/arrow20/abi.h $(srcdir)/pq/Makefile $(srcdir)/pq/isolate_pq_symbols.sh) $(OG_LAKE_PQ_BLAS_ARCHIVE)
override CPPFLAGS := -DUSE_LAKE_PQ_CPU -I$(PQ_EFFECTIVE_INCLUDE) -I$(srcdir) $(CPPFLAGS)
lake_file_pq.o lake_ann_rescan.o: $(PQ_EFFECTIVE_INCLUDE)/pq_cpu.h $(PQ_EFFECTIVE_INCLUDE)/pq_parquet_format.h $(PQ_EFFECTIVE_INCLUDE)/pq_parquet_frontier_draft.h $(PQ_EFFECTIVE_ARCHIVE)
else ifneq ($(strip $(OG_LAKE_PQ_INCLUDE)$(OG_LAKE_PQ_ARCHIVE)$(OG_LAKE_PQ_BLAS_ARCHIVE)$(OG_LAKE_PQ_MANIFEST)),)
ifeq ($(strip $(OG_LAKE_PQ_INCLUDE)),)
$(error Native PQ include path is required)
endif
ifeq ($(strip $(OG_LAKE_PQ_MANIFEST)),)
$(error Native PQ requires an admitted dependency receipt)
endif
ifneq ($(notdir $(OG_LAKE_PQ_ARCHIVE)),libog_lake_pq_cpu.a)
$(error Native PQ requires private libog_lake_pq_cpu.a)
endif
ifneq ($(notdir $(OG_LAKE_PQ_BLAS_ARCHIVE)),libog_lake_pq_openblas.a)
$(error Native PQ requires private libog_lake_pq_openblas.a)
endif
ifneq ($(words $(wildcard $(OG_LAKE_PQ_INCLUDE)/pq_cpu.h $(OG_LAKE_PQ_INCLUDE)/pq_parquet_format.h $(OG_LAKE_PQ_INCLUDE)/pq_parquet_frontier_draft.h $(OG_LAKE_PQ_INCLUDE)/sdk/lake_parquet_sdk.h $(OG_LAKE_PQ_ARCHIVE) $(OG_LAKE_PQ_BLAS_ARCHIVE) $(OG_LAKE_PQ_MANIFEST))),7)
$(error Native PQ admitted headers/archives/receipt are missing)
endif
PQ_EFFECTIVE_INCLUDE := $(OG_LAKE_PQ_INCLUDE)
PQ_EFFECTIVE_ARCHIVE := $(OG_LAKE_PQ_ARCHIVE)
PQ_EFFECTIVE_LICENSE := $(OG_LAKE_PQ_INCLUDE)/../licenses/Microsoft-DiskANN-LICENSE
PQ_EFFECTIVE_BLAS_LICENSE_DIR := $(OG_LAKE_PQ_INCLUDE)/../licenses/OpenBLAS
override CPPFLAGS := -DUSE_LAKE_PQ_CPU -I$(PQ_EFFECTIVE_INCLUDE) $(CPPFLAGS)
lake_file_pq.o lake_ann_rescan.o: $(PQ_EFFECTIVE_INCLUDE)/pq_cpu.h $(PQ_EFFECTIVE_INCLUDE)/pq_parquet_format.h $(PQ_EFFECTIVE_INCLUDE)/pq_parquet_frontier_draft.h $(PQ_EFFECTIVE_ARCHIVE) $(OG_LAKE_PQ_BLAS_ARCHIVE)
endif

include $(top_srcdir)/src/gausskernel/common.mk

ifneq ($(strip $(PQ_IN_TREE_ACTIVE)),)
$(PQ_EFFECTIVE_ARCHIVE): $(PQ_IN_TREE_DEPS)
	$(MAKE) -C $(srcdir)/pq all \
		OG_LAKE_PQ_BLAS_INCLUDE='$(OG_LAKE_PQ_BLAS_INCLUDE)' \
		OG_LAKE_PQ_BLAS_ARCHIVE='$(OG_LAKE_PQ_BLAS_ARCHIVE)'
all: $(PQ_EFFECTIVE_ARCHIVE)
objfiles.txt: $(PQ_EFFECTIVE_ARCHIVE)
endif

.PHONY: clean-pq
clean: clean-pq
clean-pq:
	$(MAKE) -C $(srcdir)/pq clean

# This tree may build without --enable-depend. Keep copied protocol DDL fresh.
lake_catalog.o: $(srcdir)/lake_catalog_ddl.inc

install: install-data
.PHONY: install-data installdirs uninstall-data
install-data: installdirs
	$(INSTALL_DATA) $(addprefix $(srcdir)/, $(DATA)) '$(DESTDIR)$(datadir)/extension/'
installdirs:
	$(MKDIR_P) '$(DESTDIR)$(datadir)/extension'
uninstall: uninstall-data
uninstall-data:
	rm -f $(addprefix '$(DESTDIR)$(datadir)/extension'/, $(notdir $(DATA)))

# Shared scalar type/floating-order helpers must also rebuild without depend.
lake_bind_snapshot.o lake_index.o lake_inline_index.o lake_inline_scan.o lake_file_scalar.o lake_file_reader.o: $(srcdir)/lake_scalar_key.h

ifneq ($(strip $(OG_LAKE_ROARING_ARCHIVE)),)
lake_bitmap_codec.o lake_bitmap_format.o: $(srcdir)/lake_bitmap_codec.h $(srcdir)/lake_bitmap_format.h $(OG_LAKE_ROARING_ARCHIVE)
install-data: install-roaring-license
.PHONY: install-roaring-license uninstall-roaring-license
install-roaring-license: installdirs
	$(INSTALL_DATA) $(OG_LAKE_ROARING_INCLUDE)/../licenses/CRoaring-LICENSE '$(DESTDIR)$(datadir)/extension/'
uninstall-data: uninstall-roaring-license
uninstall-roaring-license:
	rm -f '$(DESTDIR)$(datadir)/extension/CRoaring-LICENSE'
endif

# Row-head ABI changes must rebuild callers even without --enable-depend.
lake_native_partition_modify.o lake_native_partition_bind.o lake_row_lock.o lake_acl.o lake_catalog.o lake_txn.o lake_modify.o lake_maintenance.o: $(srcdir)/lake_row_lock.h

ifneq ($(strip $(PQ_EFFECTIVE_ARCHIVE)),)
install-data: install-pq-licenses
.PHONY: install-pq-licenses uninstall-pq-licenses
install-pq-licenses: installdirs
	$(MKDIR_P) '$(DESTDIR)$(datadir)/extension/og_lake_pq_licenses/OpenBLAS'
	$(INSTALL_DATA) '$(PQ_EFFECTIVE_LICENSE)' '$(DESTDIR)$(datadir)/extension/og_lake_pq_licenses/Microsoft-DiskANN-LICENSE'
	@if [ -f '$(srcdir)/vendor/diskann_pq/NOTICE.txt' ] && [ -n '$(strip $(PQ_IN_TREE_ACTIVE))' ]; then \
		$(INSTALL_DATA) '$(srcdir)/vendor/diskann_pq/NOTICE.txt' '$(DESTDIR)$(datadir)/extension/og_lake_pq_licenses/Microsoft-DiskANN-NOTICE.txt'; \
	fi
	$(INSTALL_DATA) '$(PQ_EFFECTIVE_BLAS_LICENSE_DIR)'/* '$(DESTDIR)$(datadir)/extension/og_lake_pq_licenses/OpenBLAS/'
uninstall-data: uninstall-pq-licenses
uninstall-pq-licenses:
	rm -f '$(DESTDIR)$(datadir)/extension/og_lake_pq_licenses/Microsoft-DiskANN-LICENSE' \
	      '$(DESTDIR)$(datadir)/extension/og_lake_pq_licenses/Microsoft-DiskANN-NOTICE.txt'
	rm -f '$(DESTDIR)$(datadir)/extension/og_lake_pq_licenses/OpenBLAS/'*
	rmdir '$(DESTDIR)$(datadir)/extension/og_lake_pq_licenses/OpenBLAS' '$(DESTDIR)$(datadir)/extension/og_lake_pq_licenses' || true
endif

# Optional PQ ABI and catalog signatures must rebuild callers without depend.
lake_bind_snapshot.o lake_file_pq.o lake_file_ann.o lake_file_ann_catalog.o lake_index.o: $(srcdir)/lake_file_pq.h
lake_inline_index.o: $(srcdir)/lake_file_ann_catalog.h

# Shared stream, sample and reader ABI dependencies without --enable-depend.
$(OBJS): $(srcdir)/lake_txn_private.h $(srcdir)/lake_inline_reader.h $(srcdir)/lake_file_reader.h
lake_stream.o lake_inline_scan.o lake_file_reader.o: $(srcdir)/lake_stream.h
lake_analyze.o lake_sample.o lake_native_partition_analyze.o: $(srcdir)/lake_sample.h

# Background task metadata/algorithm adapters share an explicit internal ABI.
lake_index_maintenance.o lake_maintenance.o lake_file_ann_catalog.o lake_file_scalar.o: $(srcdir)/lake_index_maintenance.h

lake_bind_snapshot.o lake_autoanalyze.o lake_analyze.o lake_maintenance.o lake_publish.o lake_acl.o: $(srcdir)/lake_autoanalyze.h

# Planner cost image changes must rebuild callers without --enable-depend.
lake_cost.o lake_index.o lake_inline_scan.o lake_inline_index.o lake_inline_ann.o lake_file_scalar.o lake_file_ann_catalog.o lake_native_partition_scan.o: $(srcdir)/lake_cost.h

# Existing status callers also need the extended coverage-estimate prototypes.
lake_bind_snapshot.o lake_index.o lake_file_scalar.o: $(srcdir)/lake_file_scalar.h
lake_bind_snapshot.o lake_index.o lake_inline_ann.o lake_file_ann_catalog.o: $(srcdir)/lake_file_ann_catalog.h

# Stream validates the same canonical metric/operator mapping as serial ANN.
lake_bind_snapshot.o lake_stream.o: $(srcdir)/lake_inline_ann.h
lake_ann_rescan.o lake_file_reader.o lake_file_ann.o lake_file_ann_catalog.o lake_inline_reader.o lake_inline_scan.o: $(srcdir)/lake_ann_rescan.h
lake_ann_rescan.o: $(srcdir)/lake_pq_test.h $(srcdir)/lake_file_pq.h
ifneq ($(strip $(PQ_EFFECTIVE_INCLUDE)),)
lake_ann_rescan.o: $(PQ_EFFECTIVE_INCLUDE)/pq_cpu.h $(PQ_EFFECTIVE_INCLUDE)/pq_parquet_format.h $(PQ_EFFECTIVE_INCLUDE)/pq_parquet_frontier_draft.h
endif

# Partition descriptors change the shared Native binding layout.
$(OBJS): $(srcdir)/lake_inline_private.h $(srcdir)/lake_partition.h $(srcdir)/lake_partition_prune.h

# The native parent foundation remains closed and shares the core reloptions ABI.
$(OBJS): $(srcdir)/lake_native_partition.h

# Multi-carrier descriptors are private until the complete evolution gate opens.
$(OBJS): $(srcdir)/lake_inline_sources.h $(srcdir)/lake_flush.h $(srcdir)/lake_version.h
lake_maintenance.o lake_rewrite.o lake_publish.o: $(srcdir)/lake_rewrite.h

# Fixed DuckDB identifier keyword source is part of this TU.
lake_snapshot_info.o: $(srcdir)/lake_snapshot_keywords.inc

# External file FDW / openat2 confinement / SDK6 dependencies without --enable-depend.
lake_file_sdk.o lake_external_fdw.o lake_copy.o: $(srcdir)/lake_parquet_fd_sdk.h
lake_external_path.o lake_external_fdw.o lake_copy.o: $(srcdir)/lake_external_path.h $(srcdir)/lake_external_fdw.h
lake_partition.o lake_partition_prune.o: $(srcdir)/lake_numeric.h
lake_external_fdw.o lake_copy.o: $(srcdir)/lake_copy.h
lake_copy.o: $(srcdir)/lake_arrow_builder.h $(srcdir)/lake_file_reader.h $(srcdir)/lake_file_sdk.h $(srcdir)/lake_numeric.h $(srcdir)/lake_types.h $(srcdir)/lake_vector_codec.h
lake_business_create.o lake_column_ddl.o lake_column_default.o lake_create_publish.o lake_default_ddl.o lake_publish.o lake_catalog.o: $(srcdir)/lake_column_default.h
