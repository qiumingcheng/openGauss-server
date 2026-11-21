/* Host-only tagged table identity. LOCAL keys never enter durable metadata. */
#ifndef OG_LAKE_IDENTITY_H
#define OG_LAKE_IDENTITY_H
#include "postgres.h"
enum LakeTableIdentityKind { LAKE_TABLE_PERMANENT = 1, LAKE_TABLE_LOCAL = 2 };
struct LakeTableIdentity {
    uint8 kind;
    union { int64 permanent_table_id; uint64 local_object_token; } key;
};
static inline LakeTableIdentity LakePermanentIdentity(int64 id)
{
    if (id < 0)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("negative permanent lake table identity")));
    LakeTableIdentity result; result.kind = LAKE_TABLE_PERMANENT; result.key.permanent_table_id = id; return result;
}
static inline LakeTableIdentity LakeLocalIdentity(uint64 token)
{
    LakeTableIdentity result; result.kind = LAKE_TABLE_LOCAL; result.key.local_object_token = token; return result;
}
static inline uint64 LakeIdentityKey(const LakeTableIdentity& identity)
{
    return identity.kind == LAKE_TABLE_PERMANENT ? (uint64)identity.key.permanent_table_id : identity.key.local_object_token;
}
static inline bool LakeSameIdentity(const LakeTableIdentity& a, const LakeTableIdentity& b)
{
    return a.kind == b.kind && LakeIdentityKey(a) == LakeIdentityKey(b);
}
#endif
