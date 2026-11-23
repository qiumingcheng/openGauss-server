/* Explicit little-endian encoding of the 112-byte runtime locator. */
#include "postgres.h"
#include "lake_inline_sources.h"
#include "knl/knl_variable.h"
#include "lake_locator.h"
#include "lake_txn_private.h"
#include "lake_native_partition.h"
#include "utils/builtins.h"

static uint64 LakeGetLe(const unsigned char* p, unsigned bytes)
{
    uint64 value=0;
    for(unsigned i=0;i<bytes;++i) value|=((uint64)p[i])<<(8*i);
    return value;
}

static void LakePutLe(unsigned char* p, uint64 value, unsigned bytes)
{
    for(unsigned i=0;i<bytes;++i) p[i]=(unsigned char)(value>>(8*i));
}

static void LakeCheckLocatorShape(const LakeRowLocator* value)
{
    if(value->source<LAKE_LOCATOR_INLINE || value->source>LAKE_LOCATOR_TXN ||
       !OidIsValid(value->database_oid) || !OidIsValid(value->relation_oid) || !OidIsValid(value->server_oid) ||
       value->schema_version>(uint64)PG_INT64_MAX ||
       (value->identity.kind!=LAKE_TABLE_PERMANENT && value->identity.kind!=LAKE_TABLE_LOCAL) ||
       (value->identity.kind==LAKE_TABLE_PERMANENT && value->identity.key.permanent_table_id<0) ||
       (value->identity.kind==LAKE_TABLE_LOCAL && (value->identity.key.local_object_token==0 ||
        value->source!=LAKE_LOCATOR_TXN || value->schema_version!=0 || value->leaf_oid!=InvalidOid)) ||
       value->read_snapshot>(uint64)PG_INT64_MAX || value->begin_snapshot>value->read_snapshot ||
       value->transaction_nonce==0 || value->producer_command_id==InvalidCommandId)
        ereport(ERROR,(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),errmsg("invalid native lake row locator fields")));
    if(value->source==LAKE_LOCATOR_INLINE) {
        if(value->row_id<0 || !OidIsValid(value->inline_oid) || value->file_id!=0 ||
           value->file_row_position!=0 || value->local_version_token!=0)
            ereport(ERROR,(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),errmsg("invalid native lake inline locator")));
    } else if(value->source==LAKE_LOCATOR_TXN) {
        if(value->row_id!=0 || value->begin_snapshot!=0 || value->file_id!=0 ||
           value->file_row_position!=0 || value->inline_oid!=InvalidOid || value->local_version_token==0)
            ereport(ERROR,(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),errmsg("invalid native lake transaction locator")));
    } else if(value->row_id<0 || value->inline_oid!=InvalidOid || value->local_version_token!=0) {
        ereport(ERROR,(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),errmsg("invalid native lake file locator")));
    }
}

bytea* LakeEncodeRowLocator(const LakeRowLocator* value)
{
    LakeCheckLocatorShape(value);
    bytea* result=(bytea*)palloc0(VARHDRSZ+LAKE_LOCATOR_PAYLOAD_SIZE);
    SET_VARSIZE(result,VARHDRSZ+LAKE_LOCATOR_PAYLOAD_SIZE);
    unsigned char* p=(unsigned char*)VARDATA(result);
    p[0]='O'; p[1]='L'; p[2]='K'; p[3]='R';
    LakePutLe(p+4,LAKE_LOCATOR_WIRE_VERSION,2); LakePutLe(p+6,LAKE_LOCATOR_PAYLOAD_SIZE,2);
    p[8]=value->source; p[9]=value->identity.kind; /* Remaining reserved bytes stay zero. */
    LakePutLe(p+12,value->database_oid,4); LakePutLe(p+16,value->relation_oid,4);
    LakePutLe(p+20,value->server_oid,4); LakePutLe(p+24,value->schema_version,8);
    LakePutLe(p+32,LakeIdentityKey(value->identity),8); LakePutLe(p+40,(uint64)value->row_id,8);
    LakePutLe(p+48,value->begin_snapshot,8); LakePutLe(p+56,value->read_snapshot,8);
    LakePutLe(p+64,value->file_id,8); LakePutLe(p+72,value->file_row_position,8);
    LakePutLe(p+80,value->inline_oid,4); LakePutLe(p+84,value->leaf_oid,4);
    LakePutLe(p+88,value->transaction_nonce,8); LakePutLe(p+96,value->producer_command_id,4);
    LakePutLe(p+104,value->local_version_token,8);
    return result;
}

LakeRowLocator LakeDecodeRowLocator(Datum datum)
{
    bytea* packed=DatumGetByteaPP(datum);
    const unsigned char* p=(const unsigned char*)VARDATA_ANY(packed);
    if(VARSIZE_ANY_EXHDR(packed)!=LAKE_LOCATOR_PAYLOAD_SIZE ||
       p[0]!='O' || p[1]!='L' || p[2]!='K' || p[3]!='R' ||
       LakeGetLe(p+4,2)!=LAKE_LOCATOR_WIRE_VERSION || LakeGetLe(p+6,2)!=LAKE_LOCATOR_PAYLOAD_SIZE ||
       (p[9]!=LAKE_TABLE_PERMANENT && p[9]!=LAKE_TABLE_LOCAL) || LakeGetLe(p+10,2)!=0 || LakeGetLe(p+100,4)!=0)
        ereport(ERROR,(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),errmsg("invalid native lake row locator header")));
    LakeRowLocator value;
    value.source=p[8]; value.database_oid=(Oid)LakeGetLe(p+12,4);
    value.relation_oid=(Oid)LakeGetLe(p+16,4); value.server_oid=(Oid)LakeGetLe(p+20,4);
    value.schema_version=LakeGetLe(p+24,8);
    uint64 objectKey=LakeGetLe(p+32,8);
    if(p[9]==LAKE_TABLE_PERMANENT && objectKey>(uint64)PG_INT64_MAX)
        ereport(ERROR,(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),errmsg("native lake permanent table ID is out of range")));
    value.identity=p[9]==LAKE_TABLE_PERMANENT ? LakePermanentIdentity((int64)objectKey) : LakeLocalIdentity(objectKey);
    uint64 rowId=LakeGetLe(p+40,8);
    /* All admitted sources use a nonnegative SQL BIGINT, including invalid=0 for TXN. */
    if(rowId>(uint64)PG_INT64_MAX)
        ereport(ERROR,(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),errmsg("native lake locator row ID is out of range")));
    value.row_id=(int64)rowId;
    value.begin_snapshot=LakeGetLe(p+48,8); value.read_snapshot=LakeGetLe(p+56,8);
    value.file_id=LakeGetLe(p+64,8); value.file_row_position=LakeGetLe(p+72,8);
    value.inline_oid=(Oid)LakeGetLe(p+80,4); value.leaf_oid=(Oid)LakeGetLe(p+84,4);
    value.transaction_nonce=LakeGetLe(p+88,8);
    value.producer_command_id=(CommandId)LakeGetLe(p+96,4);
    value.local_version_token=LakeGetLe(p+104,8);
    LakeCheckLocatorShape(&value);
    if((Pointer)packed!=DatumGetPointer(datum)) pfree(packed);
    return value;
}

void LakeValidateInlineLocator(const LakeRowLocator* value, const LakeTxnTable* table,
    uint64 transactionNonce, CommandId modifyingCommand)
{
    LakeCheckLocatorShape(value);
    LakeTxnRequireActiveTable(table);
    const LakeInlineBinding* binding=table->binding;
    if((OidIsValid(table->native_leaf_partition_oid)&&!LakeNativePartitionVersionEnabled()) ||
       value->leaf_oid!=table->native_leaf_partition_oid || (value->source==LAKE_LOCATOR_FILE&&!LakeFileModificationsEnabled()))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake file modification requires extension 0.10; partitioned targets are not enabled")));
    if(value->database_oid!=u_sess->proc_cxt.MyDatabaseId || value->relation_oid!=binding->relation_oid ||
       value->server_oid!=binding->server_oid || value->schema_version!=(uint64)binding->schema_version ||
       !LakeSameIdentity(value->identity,binding->identity) || value->read_snapshot!=(uint64)binding->snapshot_id ||
       value->transaction_nonce!=transactionNonce || value->producer_command_id>modifyingCommand ||
       (value->source==LAKE_LOCATOR_INLINE && (binding->inline_sources_loaded ?
            LakeFindInlineSource(binding,value->inline_oid)==NULL : value->inline_oid!=binding->inline_oid)))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake row locator does not match the active transaction binding")));
    if(value->source==LAKE_LOCATOR_FILE) {
        const LakeDataFile* file=NULL;
        for(int i=0;i<binding->file_count;++i)
            if((uint64)binding->files[i].file_id==value->file_id)file=&binding->files[i];
        if(file==NULL || value->file_row_position>=(uint64)file->record_count ||
            value->begin_snapshot>(uint64)binding->snapshot_id)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake file locator is outside its fixed scan binding")));
    }
    /* Caller additionally resolves TXN token and compares the exact old version.
     * This codec validates neither SQL privileges nor version ownership alone. */
}
