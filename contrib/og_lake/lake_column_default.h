/* Internal typed constants, not an SDK ABI or a general expression engine. */
#ifndef OG_LAKE_COLUMN_DEFAULT_H
#define OG_LAKE_COLUMN_DEFAULT_H
#include "lake_inline_private.h"
#include "nodes/primnodes.h"
struct ColumnDef;
extern bool LakePlainCreateColumnConstraints(const ColumnDef*,bool);
extern void LakePrepareCreateColumnLiteral(ColumnDef*,Oid,int32,Oid,Size*);
/* Same64MiB logical payload bound as the pending transaction journal. Host
 * parser/cook/allocator temporaries are not an all-process RSS claim. */
static const Size LAKE_CREATE_LITERAL_DEFAULT_MEMORY_LIMIT = 64UL * 1024UL * 1024UL;

#define OG_LAKE_LITERAL_DEFAULT_SQL_VERSION "0.39"
#define OG_LAKE_ALTER_DEFAULT_SQL_VERSION "0.41"
#define OG_LAKE_BINARY_DEFAULT_SQL_VERSION "0.45"
static const Size LAKE_LITERAL_DEFAULT_TEXT_LIMIT = 1024UL * 1024UL;

extern bool LakeLiteralDefaultVersionEnabled(void);
extern bool LakeAlterDefaultVersionEnabled(void);
extern bool LakeBinaryDefaultVersionEnabled(void);
extern bool LakeLiteralDefaultType(Oid type, int32 typmod = -1);
extern Const* LakeParseLiteralDefault(Node* raw, Oid type, int32 typmod, Oid collation);
extern Const* LakeParseCurrentLiteralDefault(Node* raw, Oid type, int32 typmod, Oid collation);
extern char* LakeEncodeLiteralDefault(Oid type, int32 typmod, Datum value);
extern char* LakeEncodeBinaryCanonical(Datum datum, Size maxEncodedBytes);
extern Datum LakeDecodeBinaryCanonical(const char* canonical, Size maxEncodedBytes);
extern const char* LakeCurrentDefaultMetadata(Oid type, const char* canonical, bool* expression);
extern Datum LakeDecodeLiteralDefault(Oid type, int32 typmod, const char* canonical);
extern void LakeReadColumnDefault(LakeInlineBinding* binding, LakeInlineColumn* field,
    int64 tableStart, Snapshot snapshot);
extern Size LakeColumnDefaultBytes(const LakeInlineColumn* source);
extern void LakeCopyColumnDefault(LakeInlineColumn* target, const LakeInlineColumn* source,
    LakeInlineBinding* allocationBudget);
extern void LakeMaterializeInitialDefault(const LakeInlineColumn* field, Datum* value, bool* isnull, Size* binaryRowBytes);
extern bool LakeColumnDefaultsEqual(const LakeInlineColumn* a, const LakeInlineColumn* b);
extern void LakeValidateHostDefaults(const LakeInlineBinding* binding, Relation relation);
#endif
