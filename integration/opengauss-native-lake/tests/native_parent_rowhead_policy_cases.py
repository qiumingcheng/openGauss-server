"""Private SQL26 row-head policy cases for the existing owned mutation fixture.

No independent runner, connection, service control, mapping mutation or SDK
operation is provided. The future exact-source harness owns all resources.
"""
from copy import deepcopy

MODES = ('rowhead-policy-legacy25', 'rowhead-policy-access',
         'rowhead-policy-readonly-binding', 'rowhead-policy-dml',
         'rowhead-policy-flush', 'rowhead-policy-create')
MAP_FIELDS = (
    'binding_id::text', 'relation_oid', 'row_head_oid', 'reclaim_cursor',
    'reclaim_round_end', 'reclaim_scanned', 'reclaimed_rows', 'reclaim_rounds',
    'last_reclaim_at::text', 'last_vacuum_at::text', 'live_rows_estimate',
    'dead_rows_estimate', 'head_bytes', 'soft_budget_exceeded',
    'last_delete_xid', 'oldest_xmin_observed', 'leaf_partition_oid', 'partition_no')


def run_case(c, mode):
    if mode not in MODES:
        raise ValueError('unknown row-head policy case')
    installed = '0.25' if mode == 'rowhead-policy-legacy25' else '0.26'
    if (c.contract.get('typed_rowhead_policy_qualified_to_test') is not True or
            c.contract.get('ready_for_native_qualification') is not True or
            c.contract.get('runtime_version') != '0.26' or
            c.contract.get('partition_sql_version') != installed):
        raise RuntimeError('typed row-head policy contract remains closed')
    query, scalar, sql, check, t = c.query, c.scalar, c.sql, c.check, c.t
    record = dict(mode=mode, installed_sql=installed, expected_runtime='0.26',
                  rejections=[], calls=[], direct_mapping_writes=False,
                  source='existing real parent/two leaves/independent heap fixture')
    c.r['typed_rowhead_policy'] = record
    check('policy installed extension is actual catalog version',
          scalar(t.a, "SELECT extversion FROM pg_extension WHERE extname='og_lake'"), installed)
    check('policy inherits disabled automatic maintenance',
          scalar(t.a, 'SHOW enable_lake_maintenance'), 'off')
    names = {}
    for tid, head in c.heads.items():
        rows = query(t.observer,
            'SELECT n.nspname,k.relname FROM pg_class k JOIN pg_namespace n ON n.oid=k.relnamespace WHERE k.oid=%s',
            (head['oid'],))
        check('policy uses one existing actual head/'+str(tid), len(rows), 1)
        names[tid] = sql.Identifier(*rows[0])

    def snapshot(actor=None):
        actor = actor or t.observer
        return dict(protocol=t.protocol(actor), parent_maps=c.maps(c.pid, actor),
            business=c.rows(actor), heap=c.oracle(actor),
            heads={tid: query(actor, sql.SQL('SELECT row_id FROM {} ORDER BY row_id').format(name))
                   for tid, name in names.items()},
            head_maps=query(actor, 'SELECT '+','.join(MAP_FIELDS)+
                ' FROM og_lake_internal.row_lock_binding WHERE relation_oid=%s ORDER BY leaf_partition_oid', (c.pid,)),
            identities=query(actor, 'SELECT h.relation_oid,h.leaf_partition_oid,h.partition_no,h.binding_id::text,'
                'h.row_head_oid,b.server_oid,b.table_id,b.table_uuid::text FROM og_lake_internal.row_lock_binding h '
                'JOIN og_lake_internal.partition_binding p ON p.parent_relid=h.relation_oid '
                'AND p.leaf_partition_oid=h.leaf_partition_oid AND p.partition_no=h.partition_no AND p.binding_id=h.binding_id '
                'JOIN og_lake_internal.table_binding b ON b.binding_id=p.binding_id '
                'WHERE h.relation_oid=%s ORDER BY h.leaf_partition_oid', (c.pid,)))

    def deny(actor, statement, state, args=None):
        # The local error savepoint preserves the caller's pre-existing state.
        query(actor, 'SAVEPOINT policy_expected_failure')
        observed = None
        try:
            try:
                query(actor, statement, args)
            except Exception as error:
                observed = dict(sqlstate=getattr(error, 'pgcode', None), message=str(error))
        finally:
            query(actor, 'ROLLBACK TO policy_expected_failure')
            query(actor, 'RELEASE SAVEPOINT policy_expected_failure')
        record['rejections'].append(dict(statement=str(statement), expected=state, observed=deepcopy(observed)))
        check('policy denial reaches exact expected SQLSTATE', observed is not None and observed['sqlstate'] == state)

    reclaim_sql = 'SELECT og_lake.reclaim_row_heads(%s::regclass,1)'

    def reclaim(actor):
        before = snapshot(actor)
        removed = int(scalar(actor, reclaim_sql, (c.pid,)))
        after = snapshot(actor)
        losses = {tid: len(before['heads'][tid])-len(after['heads'][tid]) for tid in names}
        check('policy positive control reaches an actual idle synchronization key', removed, 1)
        check('policy positive return matches one actual physical key loss', sum(losses.values()), 1)
        check('policy positive maintenance changes exactly one leaf', sum(value != 0 for value in losses.values()), 1)
        check('policy maintenance preserves visible business and native heap',
              (after['business'], after['heap']), (before['business'], before['heap']))
        check('policy maintenance preserves canonical eight-dimensional identities', after['identities'], before['identities'])
        record['calls'].append(dict(removed=removed, losses=losses))
        return after

    def warm():
        query(t.a, 'BEGIN')
        answer = c.both(t.a, 'UPDATE {table} SET val=val WHERE id=99 RETURNING shard,id,val,payload')
        check('policy warms both real business row-head domains', len(answer), 2)
        query(t.a, 'COMMIT')
        check('policy has one actual key per leaf',
              {tid: len(rows) for tid, rows in snapshot()['heads'].items()}, {c.low: 1, c.high: 1})

    def finish_rollback(before, label):
        query(t.a, 'ROLLBACK')
        check(label+' restores business/catalog/head/map state exactly', snapshot(), before)

    if mode == 'rowhead-policy-readonly-binding':
        # Public creation/detach/certification, with no write promotion and no
        # hand-made row_lock_binding or read_only updates.
        values = [(3, 303, 30, 'readonly-low'), (13, 313, 130, 'readonly-high')]
        low = c.source('policy_readonly_low', values[:1])
        high = c.source('policy_readonly_high', values[1:])
        query(t.a, 'DROP FOREIGN TABLE biz.policy_readonly_low')
        query(t.a, 'DROP FOREIGN TABLE biz.policy_readonly_high')
        parent, leaves = c.parent('policy_readonly')
        c.bind(parent, leaves, [low, high])
        check('public readonly certification produced exactly two readonly leaves',
              [row[5] for row in c.maps(parent)], [True, True])
        check('readonly parent has no synchronization head mapping',
              scalar(t.observer, 'SELECT count(*) FROM og_lake_internal.row_lock_binding WHERE relation_oid=%s', (parent,)), 0)
        check('readonly parent is a real marked native lake parent', c.marker(parent), True)
        check('readonly certified public read still works',
              query(t.observer, 'SELECT shard,id,val,payload FROM biz.policy_readonly ORDER BY shard'), values)
        before, readonly_maps = snapshot(), c.maps(parent)
        query(t.a, 'BEGIN')
        deny(t.a, reclaim_sql, '25006', (parent,))
        finish_rollback(before, 'readonly binding refusal')
        check('readonly refusal keeps its exact certification mapping', c.maps(parent), readonly_maps)
        check('readonly refusal creates no phantom synchronization heads',
              scalar(t.observer, 'SELECT count(*) FROM og_lake_internal.row_lock_binding WHERE relation_oid=%s', (parent,)), 0)
        record['readonly_parent'] = dict(parent=parent, leaves=leaves, tables=[low, high], maps=readonly_maps)
        return

    warm()
    before = snapshot()
    record['baseline_identity'] = deepcopy(before['identities'])

    if mode == 'rowhead-policy-legacy25':
        query(t.a, 'BEGIN')
        deny(t.a, reclaim_sql, '42809', (c.pid,))
        finish_rollback(before, 'retained SQL25 parent contract')
        return

    if mode == 'rowhead-policy-access':
        role = c.roles.create('rowhead_reader')
        actor, identity = c.roles.connect(c.db, 'rowhead_reader')
        c.configure(actor)
        record['ordinary_login'] = identity
        query(t.a, sql.SQL('GRANT USAGE ON SCHEMA biz,og_lake TO {}').format(sql.Identifier(role)))
        query(t.a, sql.SQL('GRANT SELECT ON biz.parent TO {}').format(sql.Identifier(role)))
        query(t.a, sql.SQL('GRANT EXECUTE ON FUNCTION og_lake.reclaim_row_heads(regclass,integer) TO {}').format(sql.Identifier(role)))
        check('nonowner can actually read its permitted parent', c.rows(actor), before['business'])
        check('nonowner is not the parent owner',
              scalar(actor, 'SELECT relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user) FROM pg_class WHERE oid=%s', (c.pid,)), False)
        query(actor, 'BEGIN')
        try:
            deny(actor, reclaim_sql, '42501', (c.pid,))
        finally:
            query(actor, 'ROLLBACK')
        check('nonowner denial has no maintenance/business side effects', snapshot(), before)
        query(t.a, 'BEGIN READ ONLY')
        check('actual caller transaction is read only', scalar(t.a, 'SHOW transaction_read_only'), 'on')
        deny(t.a, reclaim_sql, '25006', (c.pid,))
        finish_rollback(before, 'read-only transaction refusal')
        query(t.a, 'BEGIN')
        reclaim(t.a)
        finish_rollback(before, 'authorized positive control rollback')
        return

    if mode == 'rowhead-policy-dml':
        query(t.a, 'BEGIN'); query(t.a, 'SAVEPOINT policy_prior_dml')
        c.both(t.a, 'UPDATE {table} SET val=val+7 WHERE shard=1 RETURNING shard,id,val,payload')
        pending_rows = c.rows(t.a)
        deny(t.a, reclaim_sql, '0A000', (c.pid,))
        check('maintenance refusal preserves earlier pending DML', c.rows(t.a), pending_rows)
        query(t.a, 'ROLLBACK TO policy_prior_dml'); query(t.a, 'RELEASE SAVEPOINT policy_prior_dml')
        check('rollback of DML restores original visible rows', c.rows(t.a), before['business'])
        reclaim(t.a)
        finish_rollback(before, 'prior DML then rollback permits maintenance')

        query(t.a, 'BEGIN'); query(t.a, 'SAVEPOINT policy_prior_maintenance')
        maintained = reclaim(t.a)
        deny(t.a, 'UPDATE biz.parent SET val=val+7 WHERE shard=1', '0A000')
        check('DML refusal preserves earlier uncommitted maintenance', snapshot(t.a), maintained)
        query(t.a, 'ROLLBACK TO policy_prior_maintenance'); query(t.a, 'RELEASE SAVEPOINT policy_prior_maintenance')
        c.both(t.a, 'UPDATE {table} SET val=val+7 WHERE shard=1 RETURNING shard,id,val,payload')
        finish_rollback(before, 'rollback of maintenance marker permits DML')

        query(t.a, 'BEGIN'); query(t.a, 'SAVEPOINT policy_released_maintenance')
        reclaim(t.a); query(t.a, 'RELEASE SAVEPOINT policy_released_maintenance')
        deny(t.a, 'UPDATE biz.parent SET val=val+7 WHERE shard=1', '0A000')
        finish_rollback(before, 'released child maintenance marker remains active in parent')
        return

    if mode == 'rowhead-policy-flush':
        aux = c.source('policy_flush_aux', [(3, 703, 70, 'flush-aux')])
        check('flush uses an actual nonempty same-catalog ordinary lake table',
              query(t.observer, 'SELECT shard,id,val,payload FROM biz.policy_flush_aux'), [(3, 703, 70, 'flush-aux')])
        check('aux and both typed leaves use the same canonical server',
              scalar(t.observer, 'SELECT count(DISTINCT server_oid) FROM og_lake_internal.table_binding WHERE table_id=ANY(%s::bigint[])',
                     ([c.low, c.high, aux],)), 1)
        before = snapshot()
        flush = "SELECT og_lake.flush('biz.policy_flush_aux'::regclass)"
        record['flush_target'] = dict(table_id=aux, relation='biz.policy_flush_aux', native_parent_flush=False)

        query(t.a, 'BEGIN'); query(t.a, 'SAVEPOINT policy_prior_flush')
        query(t.a, flush)
        deny(t.a, reclaim_sql, '0A000', (c.pid,))
        query(t.a, 'ROLLBACK TO policy_prior_flush'); query(t.a, 'RELEASE SAVEPOINT policy_prior_flush')
        reclaim(t.a)
        finish_rollback(before, 'rollback of real ordinary flush permits parent maintenance')

        query(t.a, 'BEGIN'); query(t.a, 'SAVEPOINT policy_prior_maintenance')
        maintained = reclaim(t.a)
        deny(t.a, flush, '0A000')
        check('flush refusal preserves earlier uncommitted maintenance', snapshot(t.a), maintained)
        query(t.a, 'ROLLBACK TO policy_prior_maintenance'); query(t.a, 'RELEASE SAVEPOINT policy_prior_maintenance')
        query(t.a, flush)
        finish_rollback(before, 'rollback of maintenance marker permits real ordinary flush')
        return

    if mode == 'rowhead-policy-create':
        create = sql.SQL('CREATE FOREIGN TABLE biz.policy_pending(shard integer,id integer,val integer,payload text) '
            "SERVER {} OPTIONS(binding_mode 'create',lake_schema 'main')").format(sql.Identifier(t.server))
        def pending_exists():
            return scalar(t.a, "SELECT count(*) FROM pg_class k JOIN pg_namespace n ON n.oid=k.relnamespace WHERE n.nspname='biz' AND k.relname='policy_pending'")
        query(t.a, 'BEGIN'); query(t.a, 'SAVEPOINT policy_prior_create')
        query(t.a, create)
        check('real business CREATE produced a pending host relation', pending_exists(), 1)
        deny(t.a, reclaim_sql, '0A000', (c.pid,))
        query(t.a, 'ROLLBACK TO policy_prior_create'); query(t.a, 'RELEASE SAVEPOINT policy_prior_create')
        check('CREATE savepoint rollback removes the pending host relation', pending_exists(), 0)
        reclaim(t.a)
        finish_rollback(before, 'rollback of business CREATE permits maintenance')

        query(t.a, 'BEGIN'); query(t.a, 'SAVEPOINT policy_prior_maintenance')
        maintained = reclaim(t.a)
        deny(t.a, create, '0A000')
        check('rejected CREATE leaves no host object', pending_exists(), 0)
        check('CREATE refusal preserves uncommitted maintenance', snapshot(t.a), maintained)
        query(t.a, 'ROLLBACK TO policy_prior_maintenance'); query(t.a, 'RELEASE SAVEPOINT policy_prior_maintenance')
        query(t.a, create)
        check('rollback of maintenance marker permits real business CREATE', pending_exists(), 1)
        finish_rollback(before, 'later business CREATE outer rollback')
        check('outer rollback removes its transient business relation', pending_exists(), 0)

        # Configuration/binding DDL shares the same exclusion marker. The
        # existing contract allows it with maintenance; do not invent a ban.
        option = "SELECT og_lake.set_option(%s::name,'data_inlining_row_limit','4096')"
        for first in ('maintenance', 'option'):
            query(t.a, 'BEGIN')
            if first == 'maintenance':
                reclaim(t.a); query(t.a, option, (t.server,))
            else:
                query(t.a, option, (t.server,)); reclaim(t.a)
            finish_rollback(before, 'public config/maintenance coexistence '+first)
        record['binding_config_contract'] = 'shared subtransaction marker permits coexistence; business CREATE is distinct'
