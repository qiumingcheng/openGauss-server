"""Original global, independent inner Top-K and correlated inner Top-K blocks."""
from join_multiset_cases import VECTOR
CASES=['global_business','global_target','independent_top3','correlated_top3']

def query(case,oracle,mode):
    d='hjbm_documents' if oracle else 'vf_oracle_join_p2.documents'
    a='hjbm_allowed_buckets' if oracle else 'vf_oracle_join_p2.allowed_buckets'
    dist=f"vector_distance(d.embedding,to_vector('{VECTOR}'),cosine)" if oracle else f"d.embedding <=> '{VECTOR}'::vector"
    transformation='' if mode=='exact' or not oracle else 'vector_index_transform(d hjbm_docs_hnsw '+dict(pre='pre_filter_with_join_back',in_filter='in_filter_with_join_back',post='post_filter_with_join_back')[mode]+')'
    qb='J6_'+str(CASES.index(case))+'_'+mode
    limit="FETCH EXACT FIRST {k} ROWS ONLY" if oracle and mode=='exact' else "FETCH APPROXIMATE FIRST {k} ROWS ONLY WITH TARGET ACCURACY 90" if oracle else 'LIMIT {k}'
    if case.startswith('global'):
        hint='leading(a d) use_nl(d)' if oracle else 'leading((a d)) nestloop(a d)'
        projection='d.id,d.bucket_id,a.acl_group' if case=='global_business' else 'd.id,d.bucket_id'
        hints=f'gather_plan_statistics qb_name({qb}) {transformation} {hint}' if oracle else hint
        return f"SELECT /*+ {hints} */ {projection} FROM {d} d JOIN {a} a ON a.bucket_id=d.bucket_id WHERE d.tenant_id=7 AND a.bucket_id IN(626,286) ORDER BY {dist} "+limit.format(k=10)
    correlated=case=='correlated_top3'
    outerhint='leading(a v) use_nl(v) no_merge(v)' if oracle else 'leading((a v)) nestloop(a v)'
    innerhint=f'/*+ qb_name({qb}) {transformation} */ ' if oracle else ''
    inner=f"SELECT {innerhint}d.id,d.bucket_id FROM {d} d WHERE d.tenant_id=7 AND d.bucket_id="+('a.bucket_id' if correlated else '626')+f' ORDER BY {dist} '+limit.format(k=3)
    source=f'{a} a CROSS APPLY ({inner}) v' if oracle and correlated else f'{a} a CROSS JOIN LATERAL ({inner}) v' if correlated else f'{a} a JOIN ({inner}) v ON v.bucket_id=a.bucket_id'
    where='a.bucket_id IN(626,286)' if correlated else 'a.bucket_id=626'
    hints='gather_plan_statistics '+outerhint if oracle else outerhint
    return f'SELECT /*+ {hints} */ v.id,v.bucket_id,a.acl_group FROM {source} WHERE {where} ORDER BY a.bucket_id,a.acl_group,v.id'
