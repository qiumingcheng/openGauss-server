# vendored diskann_pq subset

- **Upstream Repository**: Microsoft/DiskANN (`cpp_main` legacy CPU PQ numerical subset)
- **Upstream Commit**: `78256bbab4685e1774e78d331e081a153be26823`
- **License**: MIT License (`LICENSE`, plus `NOTICE.txt`)
- **File**: `pq_cpu_ms.cpp`

## Private Adaptations vs Upstream

1. Core numerical training, encoding, lookup table, and math utility loops remain preserved verbatim from the locked upstream commit.
2. Bounded array ownership, checked output transfer, return-code cancellation polling (`PqCpuPoll()`), and private `namespace pq_cpu_ms` declarations were added so no exceptions or C++ containers cross the C boundary in `contrib/og_lake/pq/`.
3. Included header path uses `#include "pq/pq_cpu_internal.h"` relative to `contrib/og_lake/`.
4. Compiled as part of standalone static library `libog_lake_pq_cpu.a` with `-fvisibility=hidden` and `objcopy` symbol isolation against serial/LP64/`USE_LOCKING` OpenBLAS (`oglpq_` prefix).
