# Local Parquet provider candidate

The managed and external providers compile the same implementation in
`../sdk/arrow_parquet_provider.cpp`. The external build also enables the
borrowed-descriptor API and exports `og_lake_parquet_sdk_v6`. Keep both source
directories side by side when building. The installed base C header is shared.

The native executor owns planning, snapshots, row identity, deletes and catalog
publication. This independently compiled library implements file operations
using Apache Arrow/Parquet 20.0.0. Its C ABI and Arrow C Data batches keep Arrow
C++17 objects outside the openGauss C++ ABI. PIC static Arrow/Parquet archives
and their bundled dependencies are linked privately into this one shared
library. The export map exposes only the versioned C entrypoint. It does not
depend on backend-visible Arrow/Parquet shared libraries, which avoids sharing
their C++ ABI with unrelated extensions. Loading the library does not enable
the complete native file path; bounded SQL0.8 local reads have a separate gate.

The initial provider admits flat bool, int32, int64 and UTF-8 columns, explicit
Parquet field IDs, selected columns and row groups, and absolute original file
positions. Name mapping belongs to the host; ordinal projection must follow an
explicit validated mapping. It uses synchronous local descriptor I/O, never an
external query executor or asynchronous I/O callback. Files must be immutable.

Each reader has one outstanding batch. Release it before advancing. A batch
retains its Arrow buffer pool after reader close and still needs batch_release.
Arrow allocations are budgeted; metadata parsing also limits serialized footer,
string and container sizes. These limits do not constitute a process RSS cap.

Writer append borrows caller Arrow C Data only during that call and leaves its
release callbacks intact. The initial implementation synchronously writes each
append using WriteTable, starting row groups per append; coalescing many tiny
appends is not optimized. The host must select reasonable append batches.
Output preserves input order and stores exact field IDs. Finish verifies row
count and size, fsyncs local data and parent directory, then returns success.
This does not confirm remote or RAC shared-storage durability.

The host registers candidate ownership before writer_open, including failed
opens that can leave a partial file. Never publish unless Finish succeeds.
Unfinished close removes only the created inode at its original anchored parent
directory entry; cleanup failures must leave the host orphan record pending.
Do not expose paths to concurrent replacement or reuse failed candidate names.
The host must retain the loaded library until all handles and batches are gone.
Cancellation callbacks must not throw or call the database ERROR mechanism.

Build and real-file tests are driven by the workspace
`integration/opengauss-native-lake/sdk/` scripts. They pin dependency sources,
record hashes and use an independent C11 consumer plus the pinned unmodified
DuckDB reader. I/O fault injection is test-only, restricted to owned candidate
paths, and is not linked into this library. Native FDW, deletion vectors, flush,
remote storage, crash recovery and multi-instance acceptance remain separate.
