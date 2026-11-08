#!/usr/bin/env python3
"""Frozen query definitions used by offline Oracle result replay.

Live Oracle capture belongs to the historical capture tooling; this module
provides the unchanged CASES and QUERY_VECTOR without a live-capture entry.
"""

QUERY_VECTOR='[1,3,5,7,11,13,17,19]'
CASES=[
 ('semi','d.id,d.bucket_id,d.tenant_id','hjbm_documents d','d.tenant_id=7 AND EXISTS(SELECT 1 FROM hjbm_allowed_buckets a WHERE a.bucket_id=d.bucket_id AND a.acl_group<=3)'),
 ('anti','d.id,d.bucket_id,d.tenant_id','hjbm_documents d','d.tenant_id=7 AND NOT EXISTS(SELECT 1 FROM hjbm_allowed_buckets a WHERE a.bucket_id=d.bucket_id AND a.acl_group<=3)'),
 ('left_on_where','d.id,a.acl_group,a.bucket_id','hjbm_documents d LEFT JOIN hjbm_allowed_buckets a ON d.bucket_id=a.bucket_id AND a.acl_group=1 AND a.bucket_id<50','d.tenant_id=7 AND (a.bucket_id>=40 OR a.bucket_id IS NULL)'),
 ('left_nonunique','d.id,a.acl_group,a.bucket_id','hjbm_documents d LEFT JOIN hjbm_allowed_buckets a ON d.bucket_id=a.bucket_id','d.tenant_id=7 AND (a.acl_group>=3 OR a.bucket_id IS NULL)'),
 ('null_rejected_outer','d.id,a.acl_group,a.bucket_id','hjbm_documents d LEFT JOIN hjbm_allowed_buckets a ON d.bucket_id=a.bucket_id','d.tenant_id=7 AND a.acl_group=3'),
 ('nullable_target','d.id,a.acl_group,a.bucket_id','hjbm_allowed_buckets a LEFT JOIN hjbm_documents d ON d.bucket_id=a.bucket_id AND d.id<5','a.bucket_id IN(626,286)'),
 ('full_outer','d.id,a.acl_group,a.bucket_id','hjbm_documents d FULL OUTER JOIN hjbm_allowed_buckets a ON d.bucket_id=a.bucket_id','d.id<10 OR d.id IS NULL'),
]

