"""Original query blocks for J-05 full-row and repeated-target acceptance."""
VECTOR='[1,3,5,7,11,13,17,19]'
CASES=[
    dict(name='target_projection',projection='d.id,d.bucket_id,d.tenant_id',condition='d.tenant_id=7 AND a.bucket_id IN(626,286)',limit=10),
    dict(name='business_projection',projection='d.id,d.bucket_id,a.acl_group',condition='d.tenant_id=7 AND a.bucket_id IN(626,286)',limit=10),
    dict(name='all_nonunique',projection='d.id,d.bucket_id,a.acl_group',condition='d.tenant_id=7',limit=10),
    dict(name='four_matches',projection='d.id,a.acl_group',condition='d.id=31626',limit=4),
    dict(name='row_limit_two',projection='d.id',condition='d.id=31626',limit=2),
    dict(name='many_to_many',projection='d.id,a.acl_group,b.acl_group',condition='d.id=31626',limit=100,many=True),
]

def source(case,oracle):
    d='hjbm_documents' if oracle else 'vf_oracle_join_p2.documents'
    a='hjbm_allowed_buckets' if oracle else 'vf_oracle_join_p2.allowed_buckets'
    text=f'{d} d JOIN {a} a ON d.bucket_id=a.bucket_id'
    if case.get('many'):text+=f' JOIN {a} b ON d.bucket_id=b.bucket_id'
    return text

def native_query(case):
    return f"SELECT {case['projection']} FROM {source(case,False)} WHERE {case['condition']} ORDER BY d.embedding <=> '{VECTOR}'::vector LIMIT {case['limit']}"

def oracle_query(case,mode):
    hint='' if mode=='exact' else 'vector_index_transform(d hjbm_docs_hnsw '+dict(pre='pre_filter_with_join_back',in_filter='in_filter_with_join_back',post='post_filter_with_join_back')[mode]+')'
    # Oracle's query block identifiers have a length limit. Keep each original
    # query distinct without obscuring the requested transformation with an
    # invalid QB_NAME hint.
    qb='J5_'+str(CASES.index(case))+'_'+mode
    return f"SELECT /*+ gather_plan_statistics qb_name({qb}) {hint} */ {case['projection']} FROM {source(case,True)} WHERE {case['condition']} ORDER BY vector_distance(d.embedding,to_vector('{VECTOR}'),cosine) FETCH {'EXACT' if mode=='exact' else 'APPROXIMATE'} FIRST {case['limit']} ROWS ONLY"+(' WITH TARGET ACCURACY 90' if mode!='exact' else '')+';'
