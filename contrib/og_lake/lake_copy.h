/* Controlled local Parquet/CSV/JSON COPY FROM/TO helpers for SQL0.62 / SQL0.71. */
#ifndef OG_LAKE_COPY_H
#define OG_LAKE_COPY_H
#include "postgres.h"
#include "access/og_lake.h"
#include "executor/tuptable.h"
#include "utils/tuplestore.h"

extern void LakeJsonInspectFd(int fd, Tuplestorestate* store, TupleDesc outDesc);
extern void LakeJsonCountFd(int fd, int64* rowCountOut, int32* colCountOut);
#endif
