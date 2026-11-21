/* Read-only external Parquet/CSV file foreign tables and schema inspection. */
#ifndef OG_LAKE_EXTERNAL_FDW_H
#define OG_LAKE_EXTERNAL_FDW_H
#include "postgres.h"
#include "lake_file_sdk.h"
#include "nodes/parsenodes.h"
#include "utils/rel.h"

struct LakeExternalParquetColumnMap {
    AttrNumber target_attnum;
    int32 parquet_ordinal;
    Oid parquet_pg_type;
    Oid target_pg_type;
    int32 parquet_typmod;
    int32 target_typmod;
};

extern bool LakeIsExternalFileRelation(Relation relation);
extern void LakeExternalBindForeignTable(Oid relationOid, CreateForeignTableStmt* statement);
extern OgLakeSdkFooter* LakeExternalOpenFooter(const LakeExternalFdSdk* sdk, int fd);
extern void LakeExternalCloseFooter(const LakeExternalFdSdk* sdk, OgLakeSdkFooter* volatile* slot) noexcept;
extern bool LakeExternalMapSdkType(uint32 sdkType, Oid* pgTypeOut, const char** sqlNameOut);
extern LakeExternalParquetColumnMap* LakeExternalBuildColumnMap(const LakeExternalFdSdk* sdk,
    OgLakeSdkFooter* footer, const OgLakeSdkFooterInfo* info, TupleDesc desc, List* targetAttnums,
    int* mappedCountOut);
extern Datum LakeExternalConvertDatum(Datum value, const LakeExternalParquetColumnMap* map);
#endif
