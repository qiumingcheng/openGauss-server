/* Runtime-only locator codec. No persisted DuckLake schema changes. */
#ifndef OG_LAKE_LOCATOR_H
#define OG_LAKE_LOCATOR_H
#include "postgres.h"
#include "lake_inline_private.h"

static const Size LAKE_LOCATOR_PAYLOAD_SIZE = 112;
static const uint16 LAKE_LOCATOR_WIRE_VERSION = 2;
enum LakeLocatorSource { LAKE_LOCATOR_INLINE=1, LAKE_LOCATOR_FILE=2, LAKE_LOCATOR_TXN=3 };
struct LakeRowLocator {
    uint8 source;
    Oid database_oid;
    Oid relation_oid;
    Oid server_oid;
    uint64 schema_version;
    LakeTableIdentity identity;
    int64 row_id;
    uint64 begin_snapshot;
    uint64 read_snapshot;
    uint64 file_id;
    uint64 file_row_position;
    Oid inline_oid;
    Oid leaf_oid;
    uint64 transaction_nonce;
    CommandId producer_command_id;
    uint64 local_version_token;
};

/* The C struct is decoded working state; never memcpy it to/from a bytea. */
extern Oid LakeLocatorMarkerOid(bool missingOk);
extern bool LakeIsLocatorPlaceholder(Expr* expression, Index target);
extern bytea* LakeEncodeRowLocator(const LakeRowLocator* locator);
extern LakeRowLocator LakeDecodeRowLocator(Datum datum);
struct LakeTxnTable;
extern void LakeValidateInlineLocator(const LakeRowLocator* locator,
    const LakeTxnTable* table, uint64 transactionNonce, CommandId modifyingCommand);
#endif
