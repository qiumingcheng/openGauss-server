/* DuckLake SQL directory selection; file provider never chooses a snapshot. */
#include "postgres.h"
#include "lake_version.h"
#include "lake_inline_sources.h"
#include "lake_inline_private.h"
#include "lake_file_sdk.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

static void LakeRejectInlineFileDeletes(LakeInlineBinding* binding, Snapshot snapshot)
{
    /* DuckLake creates this standard carrier lazily. File-position deletes can
     * exist here without any ducklake_delete_file row, including after UPDATE
     * has placed the replacement in the ordinary inline data carrier. */
    char* name = psprintf("ducklake_inlined_delete_%lld", (long long)LakePermanentTableId(binding));
    if (!OidIsValid(get_relname_relid(name, binding->metadata_schema_oid))) return;
    char* relation = LakeMetadataRelation(binding->metadata_schema_oid, name);
    Oid type = INT8OID;
    Datum argument = Int64GetDatum(binding->snapshot_id);
    /* The trusted metadata path validates the actual owner/OID and executes
     * under the caller's fixed catalog snapshot. Future deletes are invisible.
     * Holding the SPI relation lock also protects this check from carrier DDL. */
    LakeMetaRows* rows = LakeMetadataQuery(psprintf(
        "SELECT EXISTS(SELECT 1 FROM %s WHERE begin_snapshot IS NULL OR begin_snapshot <= $1)", relation),
        1, &type, &argument, NULL, snapshot, true, SPI_OK_SELECT, binding->metadata_schema_oid);
    LakeMetadataRequireCount(rows, 1, "inline file deletion capability check");
    if (DatumGetBool(LakeMetadataValue(rows, 0, 1, BOOLOID)))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake inline file delete visibility is not implemented")));
}

static void LakeAppendRelativePath(StringInfo target,const char* part)
{
    if(part==NULL||*part=='\0'||*part=='/'||strchr(part,':')!=NULL)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake file reads currently require local relative paths")));
    if(strlen(part)>=MAXPGPATH)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake path component exceeds host limit")));
    const char* at=part;
    while(*at){
        const char* end=strchr(at,'/');if(end==NULL)end=at+strlen(at);
        Size count=end-at;
        if(count==0||(count==1&&at[0]=='.')||(count==2&&at[0]=='.'&&at[1]=='.'))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid relative lake file path component")));
        if(target->len>0&&target->data[target->len-1]!='/')appendStringInfoChar(target,'/');
        appendBinaryStringInfo(target,at,count);
        at=*end?end+1:end;
    }
    if(target->len>=MAXPGPATH)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake file path exceeds host limit")));
}

static char* LakeResolveDataFile(const char* root,const char* schema,const char* table,const char* file,bool inspectPath=true)
{
    if(root==NULL||root[0]!='/'||strlen(root)==0||root[strlen(root)-1]!='/')
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake catalog data root")));
    StringInfoData path;initStringInfo(&path);appendStringInfoString(&path,root);
    LakeAppendRelativePath(&path,schema);LakeAppendRelativePath(&path,table);LakeAppendRelativePath(&path,file);
    /* Physical index tasks validate this canonical path during their I/O
     * phase, outside catalog publication locks. The lexical checks above are
     * still mandatory; ordinary business readers retain the existing check. */
    if(!inspectPath)return path.data;
    char* canonical=realpath(path.data,NULL);
    if(canonical==NULL)
        ereport(ERROR,(errcode_for_file_access(),errmsg("cannot resolve native lake data file: %m")));
    bool same=strcmp(canonical,path.data)==0;free(canonical);
    if(!same)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake file paths cannot use symbolic-link aliases")));
    return path.data;
}

static LakeDataFile* LakeFindDataFile(LakeInlineBinding* binding, int64 id)
{
    int low=0, high=binding->file_count;
    while(low<high){int mid=low+(high-low)/2;if(binding->files[mid].file_id<id)low=mid+1;else high=mid;}
    return low<binding->file_count&&binding->files[low].file_id==id?&binding->files[low]:NULL;
}

static int64 LakeOptionalInteger(const LakeMetaRows* rows, uint32 row, int column)
{
    bool absent=false;Datum value=LakeMetadataValue(rows,row,column,INT8OID,true,&absent);
    int64 result=absent?-1:DatumGetInt64(value);
    if(!absent&&result<0)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("negative native lake optional file metadata")));
    return result;
}

static void LakeLoadFileDeletes(LakeInlineBinding* binding, Snapshot snapshot,
    const char* schemaPath, const char* tablePath)
{
    Oid types[2]={INT8OID,INT8OID};
    Datum args[2]={Int64GetDatum(LakePermanentTableId(binding)),Int64GetDatum(binding->snapshot_id)};
    char* data=LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_data_file");
    char* deletes=LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_delete_file");
    LakeMetaRows* rows=LakeMetadataQuery(psprintf(
        "SELECT d.delete_file_id,d.data_file_id,d.begin_snapshot,d.delete_count,d.file_size_bytes,"
        "d.path::text,d.path_is_relative,d.format::text,d.encryption_key::text,d.partial_max "
        "FROM %s d JOIN %s f ON f.data_file_id=d.data_file_id AND f.table_id=d.table_id "
        "WHERE d.table_id=$1 AND d.begin_snapshot<=$2 AND (d.end_snapshot IS NULL OR d.end_snapshot>$2) "
        "AND f.begin_snapshot<=$2 AND (f.end_snapshot IS NULL OR f.end_snapshot>$2) "
        "ORDER BY d.data_file_id,d.delete_file_id LIMIT 4097",deletes,data),
        2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    if(rows->count>4096)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake delete file binding exceeds limit")));
    int64 total=0;
    for(uint32 i=0;i<rows->count;++i){
        int64 id=DatumGetInt64(LakeMetadataValue(rows,i,2,INT8OID));
        LakeDataFile* file=LakeFindDataFile(binding,id);
        if(file==NULL||file->deletes!=NULL)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate or unbound native lake delete file")));
        if(strcmp(LakeMetadataText(rows,i,8),"parquet")!=0)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake Puffin delete vectors are not enabled")));
        bool keyIsNull=false;(void)LakeMetadataValue(rows,i,9,TEXTOID,true,&keyIsNull);
        if(!keyIsNull||!DatumGetBool(LakeMetadataValue(rows,i,7,BOOLOID)))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake encrypted or absolute delete files are not enabled")));
        LakeDeleteFile* del=(LakeDeleteFile*)palloc0(sizeof(LakeDeleteFile));file->deletes=del;
        del->file_id=DatumGetInt64(LakeMetadataValue(rows,i,1,INT8OID));
        del->begin_snapshot=DatumGetInt64(LakeMetadataValue(rows,i,3,INT8OID));
        del->record_count=DatumGetInt64(LakeMetadataValue(rows,i,4,INT8OID));
        del->file_bytes=DatumGetInt64(LakeMetadataValue(rows,i,5,INT8OID));
        del->partial_max=LakeOptionalInteger(rows,i,10);
        if(del->file_id<0||del->begin_snapshot<0||del->record_count<0||
            del->record_count>file->record_count||del->file_bytes<12)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake delete file population")));
        if(del->record_count>LAKE_FILE_DELETE_POSITION_LIMIT-total)
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake deletion input exceeds its position budget")));
        total+=del->record_count;
        del->path=LakeResolveDataFile(binding->data_root,schemaPath,tablePath,LakeMetadataText(rows,i,6));
    }
    char* name=psprintf("ducklake_inlined_delete_%lld",(long long)LakePermanentTableId(binding));
    if(!OidIsValid(get_relname_relid(name,binding->metadata_schema_oid)))return;
    char* carrier=LakeMetadataRelation(binding->metadata_schema_oid,name);
    LakeMetaRows* invalid=LakeMetadataQuery(psprintf(
        "SELECT EXISTS(SELECT 1 FROM %s WHERE (begin_snapshot<=$2 OR begin_snapshot IS NULL) "
        "AND (file_id IS NULL OR file_id<0 OR row_id IS NULL OR row_id<0 OR begin_snapshot IS NULL OR begin_snapshot<0))",carrier),
        2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    LakeMetadataRequireCount(invalid,1,"inline file deletion validity check");
    if(DatumGetBool(LakeMetadataValue(invalid,0,1,BOOLOID)))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake inline file deletion")));
    rows=LakeMetadataQuery(psprintf(
        "SELECT d.file_id,d.row_id FROM %s d JOIN %s f ON f.data_file_id=d.file_id "
        "WHERE f.table_id=$1 AND f.begin_snapshot<=$2 AND (f.end_snapshot IS NULL OR f.end_snapshot>$2) "
        "AND d.begin_snapshot<=$2 ORDER BY d.file_id,d.row_id LIMIT %d",carrier,data,
        LAKE_FILE_DELETE_POSITION_LIMIT+1),2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    if(rows->count>(uint32)(LAKE_FILE_DELETE_POSITION_LIMIT-total))
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake deletion input exceeds its position budget")));
    uint32 at=0;
    while(at<rows->count){
        int64 id=DatumGetInt64(LakeMetadataValue(rows,at,1,INT8OID));
        LakeDataFile* file=LakeFindDataFile(binding,id);
        if(file==NULL)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("inline deletion references an unbound native lake file")));
        uint32 end=at+1;
        while(end<rows->count&&DatumGetInt64(LakeMetadataValue(rows,end,1,INT8OID))==id)++end;
        file->inline_delete_count=(int)(end-at);
        file->inline_delete_positions=(int64*)palloc(sizeof(int64)*file->inline_delete_count);
        for(uint32 i=at;i<end;++i){
            int64 position=DatumGetInt64(LakeMetadataValue(rows,i,2,INT8OID));
            if(position<0||position>=file->record_count)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("inline deletion position exceeds its native lake file")));
            file->inline_delete_positions[i-at]=position;
        }
        at=end;
    }
}

static void LakeLoadDataFilesInternal(LakeInlineBinding* binding,Snapshot snapshot,int64 onlyFile,bool physicalIndex)
{
    bool versioned=LakeFileVersionedReadsEnabled();
    if(physicalIndex&&!versioned)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("physical lake index input requires versioned file support")));
    if(!versioned)LakeRejectInlineFileDeletes(binding,snapshot);
    Oid types[3]={INT8OID,INT8OID,INT8OID};
    Datum args[3]={Int64GetDatum(LakePermanentTableId(binding)),Int64GetDatum(binding->snapshot_id),Int64GetDatum(onlyFile)};
    char* deletes=LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_delete_file");
    if(!versioned){
        LakeMetaRows* deleted=LakeMetadataQuery(psprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE table_id=$1 "
            "AND begin_snapshot<=$2 AND (end_snapshot IS NULL OR end_snapshot>$2))",deletes),
            2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
        LakeMetadataRequireCount(deleted,1,"delete file capability check");
        if(DatumGetBool(LakeMetadataValue(deleted,0,1,BOOLOID)))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake file deletes require extension 0.9")));
    }
    char* files=LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_data_file");
    LakeMetaRows* rows=LakeMetadataQuery(psprintf("SELECT data_file_id,begin_snapshot,row_id_start,record_count,"
        "file_size_bytes,row_group_count,path::text,path_is_relative,file_format::text,mapping_id,"
        "encryption_key::text,partial_max,partition_id FROM %s WHERE table_id=$1 AND begin_snapshot<=$2 "
        "AND (end_snapshot IS NULL OR end_snapshot>$2)%s ORDER BY data_file_id LIMIT %d",files,
        physicalIndex?" AND data_file_id=$3":"",physicalIndex?2:4097),
        physicalIndex?3:2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    if(rows->count==0)return;
    if(physicalIndex&&rows->count!=1)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate physical lake index source identity")));
    LakeRequireFileReadVersion();if(!physicalIndex)(void)LakeFileProvider();
    if(rows->count>4096)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake file binding exceeds the initial 4096-file limit")));
    char* tables=LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_table");
    char* schemas=LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_schema");
    LakeMetaRows* paths=LakeMetadataQuery(psprintf("SELECT t.path::text,t.path_is_relative,s.path::text,"
        "s.path_is_relative,s.parent_schema_id FROM %s t JOIN %s s ON s.schema_id=t.schema_id "
        "WHERE t.table_id=$1 AND t.begin_snapshot<=$2 AND (t.end_snapshot IS NULL OR t.end_snapshot>$2) "
        "AND s.begin_snapshot<=$2 AND (s.end_snapshot IS NULL OR s.end_snapshot>$2)",tables,schemas),
        2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    LakeMetadataRequireCount(paths,1,"file table/schema path");
    bool parent_null=false;(void)LakeMetadataValue(paths,0,5,INT8OID,true,&parent_null);
    if(!parent_null||!DatumGetBool(LakeMetadataValue(paths,0,2,BOOLOID))||!DatumGetBool(LakeMetadataValue(paths,0,4,BOOLOID)))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake nested or absolute schema/table paths are not enabled")));
    const char* table_path=LakeMetadataText(paths,0,1);const char* schema_path=LakeMetadataText(paths,0,3);
    binding->file_count=rows->count;binding->files=(LakeDataFile*)palloc0(sizeof(LakeDataFile)*binding->file_count);
    for(uint32 i=0;i<rows->count;++i){
        LakeDataFile* file=&binding->files[i];
        file->file_id=DatumGetInt64(LakeMetadataValue(rows,i,1,INT8OID));
        file->begin_snapshot=DatumGetInt64(LakeMetadataValue(rows,i,2,INT8OID));
        Datum start=LakeMetadataValue(rows,i,3,INT8OID,true,&file->row_id_start_null);
        file->row_id_start=file->row_id_start_null?0:DatumGetInt64(start);
        file->record_count=DatumGetInt64(LakeMetadataValue(rows,i,4,INT8OID));
        file->file_bytes=DatumGetInt64(LakeMetadataValue(rows,i,5,INT8OID));
        file->row_group_count=LakeOptionalInteger(rows,i,6);file->partial_max=LakeOptionalInteger(rows,i,12);
        if(file->file_id<0||file->begin_snapshot<0||file->file_bytes<12||file->record_count<0||
            (!file->row_id_start_null&&(file->row_id_start<0||file->record_count>PG_INT64_MAX-file->row_id_start))||
            (i>0&&file->file_id<=binding->files[i-1].file_id))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake file metadata")));
        if(!versioned&&(file->row_id_start_null||file->partial_max>=0))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake explicit or partial file versions require extension 0.9")));
        if(!DatumGetBool(LakeMetadataValue(rows,i,8,BOOLOID))||strcmp(LakeMetadataText(rows,i,9),"parquet")!=0)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake file format or absolute path is not enabled")));
        for(int column=10;column<=11;++column){
            bool isnull=false;(void)LakeMetadataValue(rows,i,column,column==11?TEXTOID:INT8OID,true,&isnull);
            if(!isnull)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake file mapping or encryption is not enabled")));
        }
        file->path=LakeResolveDataFile(binding->data_root,schema_path,table_path,LakeMetadataText(rows,i,7),!physicalIndex);
        bool partitionNull=false;Datum partition=LakeMetadataValue(rows,i,13,INT8OID,true,&partitionNull);
        int64 partitionId=partitionNull?-1:DatumGetInt64(partition);
        if(!partitionNull&&partitionId<0)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("negative native lake file partition identity")));
        LakeLoadFileIdentityPartition(binding,file,partitionId,snapshot);
        if(physicalIndex&&(!LakePartitionComboVersionEnabled()||binding->has_added_columns||
           binding->has_dropped_columns||binding->retired_count>0||binding->future_count>0))
            LakeRejectIdentityPartitionIntersection(binding,"physical index construction");
    }
    if(versioned&&!physicalIndex)LakeLoadFileDeletes(binding,snapshot,schema_path,table_path);
}

void LakeLoadDataFiles(LakeInlineBinding* binding,Snapshot snapshot)
{LakeLoadDataFilesInternal(binding,snapshot,-1,false);}

void LakeLoadIndexDataFile(LakeInlineBinding* binding,Snapshot snapshot,int64 fileId)
{
    if(fileId<0)ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("physical lake index requires a nonnegative source file ID")));
    LakeLoadDataFilesInternal(binding,snapshot,fileId,true);
}
