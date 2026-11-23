/* Local output directories are prepared durably before the catalog claim. */
#include "postgres.h"
#include "lake_flush.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

static void LakeFlushAppendPath(StringInfo path,const char* relative)
{
    if(relative==NULL||relative[0]=='\0'||relative[0]=='/'||strchr(relative,':')!=NULL)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native flush requires local relative schema/table paths")));
    const char* at=relative;
    while(*at){
        const char* end=strchr(at,'/');if(end==NULL)end=at+strlen(at);
        Size length=end-at;
        if(length==0||(length==1&&at[0]=='.')||(length==2&&at[0]=='.'&&at[1]=='.'))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native flush path component")));
        if(path->len>0&&path->data[path->len-1]!='/')appendStringInfoChar(path,'/');
        appendBinaryStringInfo(path,at,length);
        if(path->len>=MAXPGPATH)ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native flush directory path exceeds host limit")));
        at=*end?end+1:end;
    }
}

void LakeEnsureLocalDirectory(const char* path)
{
    if(path==NULL||path[0]!='/'||strlen(path)>=MAXPGPATH)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid absolute native flush directory")));
    char* copy=pstrdup(path+1);int volatile directory=-1,child=-1;
    PG_TRY();
    {
        directory=open("/",O_RDONLY|O_DIRECTORY|O_CLOEXEC);
        if(directory<0)ereport(ERROR,(errcode_for_file_access(),errmsg("cannot open native flush path root: %m")));
        char* at=copy;
        while(*at){
            char* end=strchr(at,'/');if(end)*end='\0';
            if(*at=='\0'||strcmp(at,".")==0||strcmp(at,"..")==0)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid absolute native flush directory component")));
            child=openat(directory,at,O_RDONLY|O_DIRECTORY|O_CLOEXEC|O_NOFOLLOW);
            if(child<0&&errno==ENOENT){
                if(mkdirat(directory,at,0700)!=0&&errno!=EEXIST)
                    ereport(ERROR,(errcode_for_file_access(),errmsg("cannot create native flush directory: %m")));
                child=openat(directory,at,O_RDONLY|O_DIRECTORY|O_CLOEXEC|O_NOFOLLOW);
                if(child<0)ereport(ERROR,(errcode_for_file_access(),errmsg("cannot open created native flush directory: %m")));
                int synced;do{synced=fsync(directory);}while(synced<0&&errno==EINTR);
                if(synced<0)ereport(ERROR,(errcode_for_file_access(),errmsg("cannot persist native flush directory entry: %m")));
            }
            if(child<0)ereport(ERROR,(errcode_for_file_access(),errmsg("cannot open native flush directory without aliases: %m")));
            int previous=directory;directory=child;child=-1;
            if(close(previous)!=0)ereport(ERROR,(errcode_for_file_access(),errmsg("cannot close native flush parent directory: %m")));
            if(end==NULL)break;
            at=end+1;
        }
        int last=directory;directory=-1;
        if(close(last)!=0)ereport(ERROR,(errcode_for_file_access(),errmsg("cannot close native flush directory: %m")));
    }
    PG_CATCH();
    {
        if(child>=0)(void)close(child);
        if(directory>=0)(void)close(directory);
        PG_RE_THROW();
    }
    PG_END_TRY();pfree(copy);
}

char* LakePrepareFlushDirectory(const LakeInlineBinding* binding,Snapshot snapshot)
{
    Oid ns=binding->metadata_schema_oid;
    char* tables=LakeMetadataRelation(ns,"ducklake_table");char* schemas=LakeMetadataRelation(ns,"ducklake_schema");
    Oid types[2]={INT8OID,INT8OID};Datum args[2]={Int64GetDatum(LakePermanentTableId(binding)),Int64GetDatum(binding->snapshot_id)};
    LakeMetaRows* paths=LakeMetadataQuery(psprintf("SELECT s.path::text,s.path_is_relative,t.path::text,t.path_is_relative,"
        "s.parent_schema_id FROM %s t JOIN %s s ON s.schema_id=t.schema_id WHERE t.table_id=$1 AND "
        "t.begin_snapshot<=$2 AND(t.end_snapshot IS NULL OR t.end_snapshot>$2) AND s.begin_snapshot<=$2 AND "
        "(s.end_snapshot IS NULL OR s.end_snapshot>$2)",tables,schemas),2,types,args,NULL,snapshot,true,SPI_OK_SELECT,ns);
    LakeMetadataRequireCount(paths,1,"flush output directory binding");
    bool parentNull=false;LakeMetadataValue(paths,0,5,INT8OID,true,&parentNull);
    if(!parentNull||!DatumGetBool(LakeMetadataValue(paths,0,2,BOOLOID))||!DatumGetBool(LakeMetadataValue(paths,0,4,BOOLOID)))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native flush requires unnested relative schema/table paths")));
    const char* root=binding->data_root;
    if(root==NULL||root[0]!='/'||strlen(root)>=MAXPGPATH||root[strlen(root)-1]!='/')
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native flush catalog data root")));
    StringInfoData path;initStringInfo(&path);appendStringInfoString(&path,root);
    LakeFlushAppendPath(&path,LakeMetadataText(paths,0,1));LakeFlushAppendPath(&path,LakeMetadataText(paths,0,3));
    LakeEnsureLocalDirectory(path.data);
    return path.data;
}
