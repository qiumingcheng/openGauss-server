#!/usr/bin/env python3
"""Import immutable Oracle CSV rows into a separately owned AStore test schema."""
import argparse,csv,hashlib,json,math
from pathlib import Path
import numpy as np
import psycopg2.extras
from verify_query_protocol import connect

SCHEMA='vf_oracle_join_p2'
OWNER='Owned by DiskANN query filter P2 Oracle static compatibility tests'

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--documents',type=Path,required=True);p.add_argument('--business',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--setup-timeout-ms',type=int,default=300000,
                   help='statement timeout for fixture import and index creation only')
    a=p.parse_args()
    if a.setup_timeout_ms <= 0:p.error('--setup-timeout-ms must be positive')
    a.output.mkdir(parents=True,exist_ok=False)
    docs=[];business=[]
    for r in csv.DictReader(a.documents.read_text().splitlines()):
        v=json.loads(r['EMBEDDING_TEXT']);assert len(v)==8 and all(math.isfinite(x) for x in v)
        assert all(float(np.float32(x))==x for x in v) and sum(x*x for x in v)>0
        docs.append((int(r['ID']),int(r['BUCKET_ID']) if r['BUCKET_ID'] else None,int(r['TENANT_ID']),json.dumps(v,separators=(',',':'))))
    for r in csv.DictReader(a.business.read_text().splitlines()):business.append((int(r['BUCKET_ID']),int(r['ACL_GROUP'])))
    assert len(docs)==50000 and [r[0] for r in docs]==list(range(1,50001)) and len(business)==1550
    conn=connect();cur=conn.cursor();commands=[]
    def sql(q):
        cur.execute(q);rows=cur.fetchall() if cur.description else [];commands.append(dict(sql=q,rows=rows));return rows
    try:
        sql('SET enable_diskann_filter=off;SET enable_opfusion=off;SET statement_timeout='+str(a.setup_timeout_ms))
        found=sql("SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname='"+SCHEMA+"'")
        if found and found!=[(OWNER,)]:raise RuntimeError('refuse unrelated fixture schema')
        if not found:
            sql('CREATE SCHEMA '+SCHEMA);sql("COMMENT ON SCHEMA "+SCHEMA+" IS '"+OWNER+"'")
            sql('CREATE TABLE '+SCHEMA+'.documents(id int PRIMARY KEY,bucket_id int,tenant_id int NOT NULL,embedding vector(8) NOT NULL CHECK(vector_norm(embedding)>0)) WITH(storage_type=astore)')
            sql('CREATE TABLE '+SCHEMA+'.allowed_buckets(bucket_id int NOT NULL,acl_group int NOT NULL,PRIMARY KEY(acl_group,bucket_id)) WITH(storage_type=astore)')
            psycopg2.extras.execute_values(cur,'INSERT INTO '+SCHEMA+'.documents VALUES %s',docs,page_size=2000)
            psycopg2.extras.execute_values(cur,'INSERT INTO '+SCHEMA+'.allowed_buckets VALUES %s',business,page_size=2000)
            sql('CREATE INDEX vf_oracle_docs_diskann ON '+SCHEMA+'.documents USING diskann(embedding vector_cosine_ops) WITH(index_size=16)')
            sql('CREATE INDEX vf_oracle_docs_bucket ON '+SCHEMA+'.documents(bucket_id)')
            sql('CREATE INDEX vf_oracle_acl_bucket ON '+SCHEMA+'.allowed_buckets(bucket_id)')
            sql('ANALYZE '+SCHEMA+'.documents');sql('ANALYZE '+SCHEMA+'.allowed_buckets')
        actual=sql('SELECT id,bucket_id,tenant_id,embedding::text FROM '+SCHEMA+'.documents ORDER BY id')
        assert len(actual)==len(docs) and all(x[:3]==y[:3] and json.loads(x[3])==json.loads(y[3]) for x,y in zip(actual,docs))
        assert sql('SELECT bucket_id,acl_group FROM '+SCHEMA+'.allowed_buckets ORDER BY bucket_id,acl_group')==business
        manifest=dict(result='PASS',source_exports={str(x.resolve()):hashlib.sha256(x.read_bytes()).hexdigest() for x in (a.documents,a.business)},document_rows=len(docs),business_rows=len(business),dimensions=8,source_format='FLOAT32 (separately observed Oracle metadata); exact float32 roundtrip checked',storage='astore',schema=SCHEMA,ownership=OWNER,note='Bucket remains nullable. Nonzero CHECK is stronger than Oracle DDL and justified only for this verified static corpus, not a general Oracle compatibility assumption.')
        (a.output/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n');print(json.dumps(manifest,ensure_ascii=False))
    finally:
        # Do not duplicate all vectors in command output: the export and verified manifest are the data authority.
        for item in commands:
            if len(item['rows'])>100:item['row_count']=len(item.pop('rows'))
        (a.output/'commands.json').write_text(json.dumps(commands,ensure_ascii=False,indent=2)+'\n');cur.close();conn.close()

if __name__=='__main__':main()
