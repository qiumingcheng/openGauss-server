/* Actual DuckLake C API transactions. No protocol row is manufactured. */
#include "duckdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
static int assertions=0;
static void sql(duckdb_connection c,const char* command){
 duckdb_result r;
 if(duckdb_query(c,command,&r)!=DuckDBSuccess){fprintf(stderr,"SQL failed: %s\n%s\n",command,duckdb_result_error(&r));duckdb_destroy_result(&r);exit(1);}
 duckdb_destroy_result(&r);
}
static void image(duckdb_connection c,const char* command,const char* expected){
 duckdb_result r;if(duckdb_query(c,command,&r)!=DuckDBSuccess){fprintf(stderr,"image: %s\n",duckdb_result_error(&r));exit(1);}
 char actual[1024]={0};size_t used=0;
 for(idx_t row=0;row<duckdb_row_count(&r);++row){
  for(idx_t col=0;col<duckdb_column_count(&r);++col){
   char* s=duckdb_value_varchar(&r,col,row);int n=snprintf(actual+used,sizeof(actual)-used,"%s%s",col?",":"",s?s:"NULL");
   duckdb_free(s);if(n<0||(size_t)n>=sizeof(actual)-used){fprintf(stderr,"image overflow\n");exit(1);}used+=(size_t)n;
  }
  if(used+1>=sizeof(actual)){fprintf(stderr,"image overflow\n");exit(1);}actual[used++]=';';actual[used]=0;
 }
 duckdb_destroy_result(&r);++assertions;
 if(strcmp(actual,expected)!=0){fprintf(stderr,"image mismatch: %s expected %s\n",actual,expected);exit(1);}
 printf("image %s -> %s\n",command,actual);fflush(stdout);
}
static int attempt(duckdb_connection c,const char* label,const char* command){
 duckdb_result r;int ok=duckdb_query(c,command,&r)==DuckDBSuccess;
 printf("outcome %s %s %s\n",label,ok?"success":"error",ok?"":duckdb_result_error(&r));fflush(stdout);
 duckdb_destroy_result(&r);return ok;
}
int main(int argc,char**argv){
 if(argc!=2)return 2;
 duckdb_database db=NULL;duckdb_connection a=NULL,b=NULL;
 if(duckdb_open(NULL,&db)!=DuckDBSuccess||duckdb_connect(db,&a)!=DuckDBSuccess||duckdb_connect(db,&b)!=DuckDBSuccess)return 3;
 char command[4096];int n=snprintf(command,sizeof(command),"ATTACH 'ducklake:%s/catalog.ducklake' AS lake (DATA_PATH '%s/data',DUCKLAKE_VERSION '1.1',AUTOMATIC_MIGRATION FALSE,DATA_INLINING_ROW_LIMIT 100)",argv[1],argv[1]);
 if(n<0||(size_t)n>=sizeof(command)||strchr(argv[1],'\''))return 4;
 sql(a,command);sql(a,"SET threads=1");sql(b,"SET threads=1");
 sql(a,"CREATE TABLE lake.t1(id INTEGER,val INTEGER);INSERT INTO lake.t1 VALUES(1,10),(2,20),(3,30)");
 sql(a,"BEGIN;CALL ducklake_flush_inlined_data('lake')");
 image(a,"SELECT id,val FROM lake.t1 ORDER BY id","1,10;2,20;3,30;");
 sql(b,"UPDATE lake.t1 SET val=11 WHERE id=1");
 image(b,"SELECT id,val FROM lake.t1 ORDER BY id","1,11;2,20;3,30;");
 int commit1=attempt(a,"flush-read_then_update_then_flush-commit","COMMIT");
 if(!commit1)attempt(a,"rollback_failed_flush","ROLLBACK");
 image(b,"SELECT id,val FROM lake.t1 ORDER BY id","1,11;2,20;3,30;");
 sql(a,"CALL ducklake_flush_inlined_data('lake')");
 image(a,"SELECT rowid,id,val FROM lake.t1 ORDER BY id","0,1,11;1,2,20;2,3,30;");
 sql(a,"CREATE TABLE lake.t2(id INTEGER,val INTEGER);INSERT INTO lake.t2 VALUES(1,10),(2,20),(3,30)");
 sql(b,"BEGIN");image(b,"SELECT id,val FROM lake.t2 ORDER BY id","1,10;2,20;3,30;");
 sql(a,"CALL ducklake_flush_inlined_data('lake')");
 image(b,"SELECT id,val FROM lake.t2 ORDER BY id","1,10;2,20;3,30;");
 int update2=attempt(b,"old-reader_update_after_flush","UPDATE lake.t2 SET val=21 WHERE id=2");
 int commit2=update2?attempt(b,"old-reader_update_commit","COMMIT"):0;
 if(!commit2)attempt(b,"rollback_old_reader","ROLLBACK");
 image(a,"SELECT id,val FROM lake.t2 ORDER BY id",commit2?"1,10;2,21;3,30;":"1,10;2,20;3,30;");
 sql(a,"CREATE TABLE lake.t3(id INTEGER,val INTEGER);INSERT INTO lake.t3 VALUES(1,10),(2,20),(3,30)");
 sql(a,"BEGIN;CALL ducklake_flush_inlined_data('lake')");
 sql(b,"INSERT INTO lake.t3 VALUES(4,40)");
 int commit3=attempt(a,"flush-read_then_insert_then_flush-commit","COMMIT");
 if(!commit3){attempt(a,"rollback_insert_flush","ROLLBACK");sql(a,"CALL ducklake_flush_inlined_data('lake')");}
 image(b,"SELECT rowid,id,val FROM lake.t3 ORDER BY id","0,1,10;1,2,20;2,3,30;3,4,40;");
 image(b,"SELECT count(*),count(DISTINCT id),sum(val) FROM lake.t3","4,4,100;");
 printf("PASS actual_reference_flush_interleavings assertions=%d commits=%d,%d,%d\n",assertions,commit1,commit2,commit3);
 duckdb_disconnect(&a);duckdb_disconnect(&b);duckdb_close(&db);return 0;
}
