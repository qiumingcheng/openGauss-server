"""Independent primitive/nearest-centroid oracle; never calls the PQ backend."""
import hashlib
import math
import re
import struct

NUMERICAL_BACKEND='MicrosoftDiskANN78256bbab4685e1774e78d331e081a153be26823|plain8bit|zero_mean0|opq0|raii_poll_v1'
BLAS_CONFIG='OpenBLAS 0.3.18 NO_LAPACK NO_LAPACKE NO_AFFINITY PRESCOTT SINGLE_THREADED'
SIDE_FILES=['ann_pq_model.parquet','ann_pq_codes.parquet','ann_pq_manifest.parquet']
def integer(value):return struct.pack('>Q',value)
def literal(value):return "'"+str(value).replace("'","''")+"'"
def verify(directory,metadata,duck,check,label):
    """metadata comes from one exact active catalog coverage; DuckDB is separate.
    Every code byte is independently checked against nearest L2 chunk centers.
    A conservative float32 accumulation interval admits rounding boundary ties;
    this is not an exact original-vector SQL-distance or ANN-recall oracle.
    """
    text=metadata['manifest'];match=re.fullmatch(r'og_lake_ann_pq_v2\|([0-9a-f]{64})\|([0-9a-f]{64})\|([1-9][0-9]*)\|([1-9][0-9]*)\|([1-9][0-9]*)',text)
    check(label+' strict catalog format2 descriptor',bool(match));graph_root,pq_root=match.group(1,2);m,train,n=map(int,match.group(3,4,5));d=16;metric=metadata['metric']
    check(label+' bounded source training and full dense node coverage',256<=train<=1024 and n>=512 and m==4)
    rows=duck('SELECT node_id,flags,hex(vector_le) AS v,hex(neighbors_le) AS e,hex(postings_le) AS p FROM read_parquet('+literal(directory/'ann_nodes.parquet')+') ORDER BY node_id',label+'-graph')
    check(label+' graph dense node IDs',[x['node_id']for x in rows],list(range(n)))
    graph_context=hashlib.sha256(('og_lake_ann_context_v2|'+str(metadata['uuid'])+'|'+str(metadata['generation'])+'|'+metadata['definition_hash']+'|'+metadata['segment']+'|'+metadata['token']+'|'+metadata['source_hash']+'|'+metadata['schema_hash']).encode()).hexdigest()
    ent=sum(x['flags']&1 for x in rows);posts=sum(len(bytes.fromhex(x['p']))//32 for x in rows)
    def graph_start(domain):return hashlib.sha256(domain.encode()+graph_context.encode()+b''.join(struct.pack('<Q',x)for x in [1,d,metric,n,ent,posts,64]))
    pages=[]
    for first in range(0,n,64):
        part=rows[first:first+64];h=graph_start('og_lake_ann_page_v1');h.update(struct.pack('<QQ',first//64,len(part)))
        for row in part:
            h.update(struct.pack('<QQQ',row['node_id'],row['flags']&1,(row['flags']>>1)&1))
            for field in ['v','e','p']:
                raw=bytes.fromhex(row[field]);h.update(struct.pack('<Q',len(raw))+raw)
        pages.append(dict(page_id=first//64,row_count=len(part),entry_count=sum(x['flags']&1 for x in part),posting_count=sum(len(bytes.fromhex(x['p']))//32 for x in part),page_hash=h.hexdigest()))
    got_pages=duck('SELECT * FROM read_parquet('+literal(directory/'ann_lookup.parquet')+') ORDER BY page_id',label+'-graph-directory');check(label+' every graph page hash and populations',got_pages,pages)
    h=graph_start('og_lake_ann_directory_v1');h.update(struct.pack('<Q',len(pages)))
    for page in pages:h.update(b''.join(struct.pack('<Q',page[k])for k in ['page_id','row_count','entry_count','posting_count'])+page['page_hash'].encode())
    check(label+' independent exact graph root',h.hexdigest(),graph_root)
    model=duck('SELECT centroid_id,hex(centers) AS b FROM read_parquet('+literal(directory/SIDE_FILES[0])+') ORDER BY centroid_id',label+'-model')
    codes=duck('SELECT node_id,hex(codes) AS b FROM read_parquet('+literal(directory/SIDE_FILES[1])+') ORDER BY node_id',label+'-codes')
    manifest=duck('SELECT field_id,hex(value) AS b FROM read_parquet('+literal(directory/SIDE_FILES[2])+') ORDER BY field_id',label+'-manifest')
    check(label+' model dense IDs',[x['centroid_id']for x in model],list(range(256)));check(label+' codes dense IDs',[x['node_id']for x in codes],list(range(n)));check(label+' manifest fixed ordered fields',[x['field_id']for x in manifest],list(range(1,21)))
    fields={x['field_id']:bytes.fromhex(x['b'])for x in manifest};h=hashlib.sha256(b'og_lake_pq_parquet_artifact_v1')
    for k in range(1,21):h.update(integer(k)+integer(len(fields[k]))+fields[k])
    check(label+' independent trusted PQ root',h.hexdigest(),pq_root)
    for k,v in {1:1,2:1,3:1,4:256,5:d,6:m,7:metric,8:train,9:n,10:metadata['file_id'],20:1}.items():check(label+' canonical manifest primitive '+str(k),fields[k],integer(v))
    context=hashlib.sha256(b'og_lake_native_pq_graph_context_v1'+graph_context.encode()+graph_root.encode()+metadata['source_hash'].encode()+metadata['schema_hash'].encode()+b''.join(integer(x)for x in [metadata['file_id'],n,d,m,metric,1])).hexdigest()
    check(label+' node-order/source/schema context',fields[11].decode(),context);check(label+' source hash',fields[12].decode(),metadata['source_hash']);check(label+' schema hash',fields[13].decode(),metadata['schema_hash']);check(label+' locked MS source',fields[15].decode(),'78256bbab4685e1774e78d331e081a153be26823')
    alg=hashlib.sha256(NUMERICAL_BACKEND.encode()+integer(1)+integer(1)+integer(256)).hexdigest();backend=hashlib.sha256(b'og_lake_pq_cpu_backend_v1'+alg.encode()+BLAS_CONFIG.encode()).hexdigest()
    check(label+' numerical algorithm hash',fields[16].decode(),alg);check(label+' immutable locking BLAS fingerprint',fields[14].decode(),backend)
    def start(domain):return hashlib.sha256(domain.encode()+NUMERICAL_BACKEND.encode()+context.encode()+BLAS_CONFIG.encode()+b''.join(integer(x)for x in [1,d,m,metric,256]))
    centers=[struct.unpack('<16f',bytes.fromhex(x['b']))for x in model];check(label+' all model centers finite',all(math.isfinite(v)for row in centers for v in row))
    h=start('og_lake_pq_cpu_codebook_v1')
    for row in model:
        for bits, in struct.iter_unpack('<I',bytes.fromhex(row['b'])):h.update(integer(bits))
    codebook=h.hexdigest();check(label+' every model bit contributes to independent codebook root',fields[17].decode(),codebook)
    h=start('og_lake_pq_cpu_model_v1');h.update(integer(train)+codebook.encode());model_hash=h.hexdigest();check(label+' model identity root',fields[18].decode(),model_hash)
    payload=b''.join(bytes.fromhex(x['b'])for x in codes);check(label+' exact code payload length',len(payload),n*m)
    h=start('og_lake_pq_cpu_nodecodes_v1');h.update(model_hash.encode()+integer(metadata['file_id'])+integer(n)+payload);check(label+' every code byte contributes to independent payload root',fields[19].decode(),h.hexdigest())
    exact=ambiguous=0;maximum_excess=0.0
    for at,row in enumerate(rows):
        vector=struct.unpack('<16f',bytes.fromhex(row['v']))
        for chunk in range(m):
            dims=range(chunk*4,chunk*4+4);distances=[sum((float(vector[j])-float(c[j]))**2 for j in dims)for c in centers]
            expected=min(range(256),key=distances.__getitem__);actual=payload[at*m+chunk]
            scale=max(1.0,sum(float(vector[j])**2+max(float(c[j])**2 for c in centers)for j in dims));bound=64*2**-23*scale
            excess=distances[actual]-distances[expected];maximum_excess=max(maximum_excess,excess)
            if actual==expected:exact+=1
            else:ambiguous+=1
            if excess>bound:raise AssertionError(label+' independently invalid code at node '+str(at)+' chunk '+str(chunk)+': '+str((actual,expected,excess,bound)))
    check(label+' independent oracle covers every persisted code byte',exact+ambiguous,n*m)
    return dict(nodes=n,code_bytes_checked=n*m,exact_argmin_bytes=exact,float32_boundary_ties=ambiguous,maximum_centroid_excess=maximum_excess,graph_root=graph_root,pq_root=pq_root,context=context)
