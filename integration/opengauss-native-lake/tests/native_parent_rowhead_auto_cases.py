"""Private typed worker/VACUUM cases, callable only by a future reviewed fixture.

This module has no DB imports, service/configuration changes, fixture creation,
manual reclamation call or supervisor. The exact ordinary campaign owns those
lifecycles; the existing real native-parent mutation fixture supplies business
data and its independent partitioned heap oracle. Native qualification is closed.
"""
import json
import time
from copy import deepcopy

MODES = ('rowhead-auto', 'rowhead-vacuum-reuse')
FIELDS = ('binding_id::text', 'relation_oid', 'row_head_oid', 'reclaim_cursor',
          'reclaim_round_end', 'reclaim_scanned', 'reclaimed_rows', 'reclaim_rounds',
          'last_reclaim_at::text', 'last_vacuum_at::text', 'live_rows_estimate',
          'dead_rows_estimate', 'head_bytes', 'soft_budget_exceeded',
          'last_delete_xid', 'oldest_xmin_observed', 'leaf_partition_oid', 'partition_no')


def require_contract(contract):
    if (contract.get('partition_sql_version') != '0.26' or
            contract.get('typed_rowhead_qualified_to_test') is not True or
            contract.get('typed_worker_qualified_to_test') is not True or
            contract.get('ready_for_native_qualification') is not True or
            contract.get('typed_worker_head_only') is not True):
        raise RuntimeError('typed worker SQL26 source/build/SDK contract remains closed')


class ClaimGuard:
    """Use the existing database-scoped test claim; never set registry state.

    Future fixture adapter constructs this immediately after Case(), before its
    first business source table, and closes it after actor rollback but before
    owned DB cleanup. The parent campaign owns exact configuration restoration.
    """
    def __init__(self, c):
        require_contract(c.contract)
        self.c, self.held = c, False

    def claim(self):
        if self.held:
            raise AssertionError('duplicate owned maintenance claim')
        deadline = time.monotonic()+self.c.budget_seconds
        while not self.c.scalar(self.c.t.observer, 'SELECT og_lake_internal.test_maintenance_claim()'):
            if time.monotonic() >= deadline:
                raise AssertionError('bounded owned-database maintenance claim deadline')
            time.sleep(.02)
        self.held = True

    def release(self):
        if not self.held:
            raise AssertionError('release without exact owned maintenance claim')
        self.c.check('release only held database-scoped maintenance claim',
                     self.c.scalar(self.c.t.observer, 'SELECT og_lake_internal.test_maintenance_release()'))
        self.held = False

    def close_after_actor_rollback(self):
        if self.held:
            self.release()


def run_case(c, mode):
    require_contract(c.contract)
    if mode not in MODES:
        raise ValueError('unknown typed automatic mode')
    if not 20 <= c.budget_seconds <= 180 or not c.claim_guard.held:
        raise AssertionError('bounded budget and real fixture maintenance claim required')
    query, scalar, sql, check, t = c.query, c.scalar, c.sql, c.check, c.t
    guard = c.claim_guard
    tids = (c.low, c.high)
    r = dict(mode=mode, observations=[], worker_identity_proofs=[],
             manual_reclaim_calls=0, mapping_mutations=0,
             stale_target_negative_verified=False, vacuum_and_reuse_case_checks_completed=False)
    c.r['typed_automatic_rowhead'] = r
    check('actual campaign enabled native launcher', scalar(t.observer, 'SHOW enable_lake_maintenance'), 'on')
    check('owned fixture uses actual selected database OID',
          scalar(t.observer, 'SELECT oid FROM pg_database WHERE datname=current_database()'), c.database_oid)

    def identity():
        return query(t.observer,
            'SELECT h.binding_id::text,h.relation_oid,h.leaf_partition_oid,h.partition_no,'
            'h.row_head_oid,b.server_oid,b.table_id,b.table_uuid::text,p.parent_relid,'
            'p.leaf_partition_oid,p.partition_no,ch.parentid,ch.partitionno,ch.parttype,'
            'n.nspname,k.relname,k.relowner,k.relkind,k.relpersistence '
            'FROM og_lake_internal.row_lock_binding h '
            'JOIN og_lake_internal.partition_binding p ON p.binding_id=h.binding_id '
            'JOIN og_lake_internal.table_binding b ON b.binding_id=h.binding_id '
            'JOIN pg_partition ch ON ch.oid=h.leaf_partition_oid '
            'JOIN pg_class k ON k.oid=h.row_head_oid '
            'JOIN pg_namespace n ON n.oid=k.relnamespace '
            'WHERE h.relation_oid=%s ORDER BY h.leaf_partition_oid', (c.pid,))

    original_identity = identity()
    check('automatic target has two full canonical chains', len(original_identity), 2)
    for row in original_identity:
        expected = c.heads[row[6]]
        check('automatic typed target matches actual fixture/'+str(row[6]), row[:5],
              (expected['binding'], c.pid, expected['leaf'], expected['no'], expected['oid']))
        check('automatic canonical map matches real pg_partition/'+str(row[6]), row[8:14],
              (c.pid, expected['leaf'], expected['no'], c.pid, expected['no'], 'p'))
        check('automatic head is owned persistent private heap/'+str(row[6]),
              (row[14], row[17], row[18]), ('og_lake_internal', 'r', 'p'))
    check('actual typed heads are independent relations', len({row[4] for row in original_identity}), 2)
    heads = {row[6]: sql.Identifier(row[14], row[15]) for row in original_identity}
    r['original_identity'] = deepcopy(original_identity)

    def maps():
        rows = query(t.observer, 'SELECT '+','.join(FIELDS)+
                     ' FROM og_lake_internal.row_lock_binding WHERE relation_oid=%s '
                     'ORDER BY leaf_partition_oid', (c.pid,))
        if len(rows) != 2:
            raise AssertionError('typed maintenance mapping disappeared')
        return {row[16]: dict(zip([x.split('::')[0] for x in FIELDS], row)) for row in rows}

    def counts():
        return {tid: scalar(t.observer, sql.SQL('SELECT count(*) FROM {}').format(heads[tid]))
                for tid in tids}

    def state():
        return json.loads(scalar(t.observer, 'SELECT og_lake.host_status()'))['automatic_flush']

    def business():
        return dict(protocol=t.protocol(t.observer), parent_maps=c.maps(c.pid),
                    rows=c.rows(t.observer), heap=c.oracle(t.observer), next_ids=c.next_ids())

    def invariant(label, original):
        check(label+' preserves exact typed identities', identity(), original_identity)
        check(label+' preserves full business/catalog/heap state', business(), original)
        check(label+' keeps independent full heap equality', c.rows(t.observer), c.oracle(t.observer))

    def observe(label, predicate, deadline):
        while True:
            image = dict(at=time.monotonic(), label=label, mapping=maps(), counts=counts(), worker=state())
            r['observations'].append(deepcopy(image))
            if predicate(image):
                check(label, True)
                return image
            if time.monotonic() >= deadline:
                raise AssertionError('original bounded automatic observation deadline: '+label)
            time.sleep(.02)

    def warm():
        check('real warmer owns the database maintenance claim', guard.held)
        query(t.a, 'BEGIN')
        values = c.both(t.a, 'UPDATE {table} SET val=val+1 WHERE id=99 '
                        'RETURNING shard,id,val,payload')
        check('actual DML modified one business row in each leaf', len(values), 2)
        query(t.a, 'COMMIT')
        check('actual committed DML created both typed heads', counts(), {tid: 1 for tid in tids})
        lo = [row['r'] for row in c.file_rows if row['shard'] == 1]
        hi = [row[0] for row in c.inline_rows(c.high) if row[3] == 10 and row[2] is None]
        check('automatic test uses real same-R domains', lo, hi)
        check('automatic DML head preserves allocator identity', c.next_ids(), c.first_next)
        return business(), lo[0]

    def prove_real_worker():
        # No map values are changed. A relation lock delays the actual worker
        # before its initial map read; preserve its native lock/alarm settings.
        # A missed handshake, timeout or failed worker is a real case failure.
        before = state()
        map_oid = scalar(t.observer, "SELECT 'og_lake_internal.row_lock_binding'::regclass::oid")
        query(t.b, 'BEGIN')
        query(t.b, 'LOCK TABLE og_lake_internal.row_lock_binding IN ACCESS EXCLUSIVE MODE')
        try:
            guard.release()
            deadline = time.monotonic()+3
            while True:
                worker = state()
                evidence = query(t.observer,
                    'SELECT a.pid,a.datid,a.application_name,l.locktype,l.database,'
                    'l.relation,l.mode,l.granted FROM pg_stat_activity a '
                    'JOIN pg_locks l ON l.pid=a.pid '
                    "WHERE a.datid=%s AND a.application_name='og_lake flush worker' "
                    "AND l.locktype='relation' AND l.database=%s AND l.relation=%s AND NOT l.granted",
                    (c.database_oid, c.database_oid, map_oid))
                match = [row for row in evidence if row[0] == worker.get('worker_pid') and
                         worker.get('database_oid') == c.database_oid and worker.get('phase') == 3]
                if match:
                    r['worker_identity_proofs'].append(dict(worker=worker, locks=match,
                        owned_database_oid=c.database_oid, map_oid=map_oid,
                        no_timeout_extension=True, observed_at=time.monotonic()))
                    break
                if time.monotonic() >= deadline:
                    raise AssertionError('actual owned worker map-lock handshake was not observed')
                time.sleep(.01)
        finally:
            query(t.b, 'ROLLBACK')
        check('worker proof matches exact PID/app/database/relation lock', bool(r['worker_identity_proofs']))
        check('handshake does not hide a failed worker or timeout', state()['failed_batches'], before['failed_batches'])
        return before

    initial, shared_r = warm()
    worker_before = prove_real_worker()
    observe('actual worker drains real business-created heads in both leaves',
            lambda image: image['counts'] == {tid: 0 for tid in tids},
            time.monotonic()+c.budget_seconds)
    guard.claim()
    invariant('actual worker identity-proved initial pass', initial)
    original, shared_r = warm()

    if mode == 'rowhead-auto':
        before_maps = maps()
        query(t.a, 'BEGIN')
        held = c.both(t.a, 'UPDATE {table} SET val=val+7 WHERE shard=1 '
                      'RETURNING shard,id,val,payload')
        check('real low-leaf writer holds exactly one business version', len(held), 1)
        try:
            query(t.observer, sql.SQL('SELECT row_id FROM {} WHERE row_id=%s '
                                     'FOR UPDATE NOWAIT').format(heads[c.low]), (shared_r,))
        except Exception as exc:
            check('independent tuple probe proves actual low-leaf writer lock', getattr(exc, 'pgcode', None), '55P03')
        else:
            raise AssertionError('busy business head was unexpectedly lockable')
        guard.release()
        deadline = time.monotonic()+c.budget_seconds
        high_leaf = c.heads[c.high]['leaf']
        observed = observe('automatic worker advances the free typed sibling leaf',
            lambda image: image['counts'][c.high] == 0 and image['counts'][c.low] == 1 and
            image['mapping'][high_leaf]['reclaimed_rows']-before_maps[high_leaf]['reclaimed_rows'] == 1,
            deadline)
        check('free-leaf persistent deletion counter advanced by actual key loss',
              observed['mapping'][high_leaf]['reclaimed_rows']-before_maps[high_leaf]['reclaimed_rows'], 1)
        invariant('busy writer committed view', original)
        query(t.a, 'ROLLBACK')
        observe('automatic worker later drains the released real writer head',
                lambda image: image['counts'] == {tid: 0 for tid in tids}, deadline)
        guard.claim()
        invariant('automatic fair head-only maintenance', original)

    else:
        # Each population has one actual DML head and19999 capacity keys.
        # Only already-owned synchronization heaps are seeded; no map editing.
        for tid in tids:
            query(t.a, sql.SQL('INSERT INTO {}(row_id) SELECT i '
                              'FROM generate_series(10000,29998)i').format(heads[tid]))
        check('first actual capacity population is20000 per real leaf', counts(), {tid: 20000 for tid in tids})

        def sizes():
            return {tid: tuple(int(x) for x in query(t.observer,
                'SELECT pg_relation_size(%s::regclass),pg_indexes_size(%s::regclass),'
                'pg_total_relation_size(%s::regclass)', (c.heads[tid]['oid'],)*3)[0]) for tid in tids}

        first_sizes = sizes()
        population_maps = maps()
        outside_snapshot = c.diagnostic(t.b)
        query(t.b, 'BEGIN READ ONLY')
        transaction_snapshot = c.diagnostic(t.b)
        query(t.b, 'DECLARE typed_retained NO SCROLL CURSOR FOR '
                   'SELECT shard,id,val,payload FROM biz.parent ORDER BY shard,id')
        query(t.b, 'FETCH 1 FROM typed_retained')
        retained = c.diagnostic(t.b)
        check('real RC cursor pins snapshots beyond diagnostic statement baseline',
              retained['registered_snapshots'] > transaction_snapshot['registered_snapshots'])
        r['retained_cursor'] = dict(outside=outside_snapshot, transaction=transaction_snapshot, retained=retained)
        guard.release()
        deadline = time.monotonic()+c.budget_seconds
        drained = observe('automatic deletion drains both20000-key populations',
            lambda image: image['counts'] == {tid: 0 for tid in tids} and all(
                item['reclaimed_rows']-population_maps[leaf]['reclaimed_rows'] == 20000
                for leaf, item in image['mapping'].items()), deadline)
        check('both typed deletion counters equal actual first population loss',
              {leaf: item['reclaimed_rows']-population_maps[leaf]['reclaimed_rows']
               for leaf, item in drained['mapping'].items()},
              {c.heads[tid]['leaf']: 20000 for tid in tids})

        def backlog_after_drain(image):
            for leaf, item in image['mapping'].items():
                prior = drained['mapping'][leaf]
                if (item['last_vacuum_at'] is None or item['last_vacuum_at'] == prior['last_vacuum_at'] or
                        item['last_delete_xid'] is None or item['oldest_xmin_observed'] is None or
                        item['oldest_xmin_observed'] > item['last_delete_xid'] or
                        (item['dead_rows_estimate'] or 0) <= 0):
                    return False
            return True

        pinned = observe('completed per-leaf vacuum retains backlog under actual old cursor snapshot',
                         backlog_after_drain, deadline)
        r['pinned_vacuum'] = deepcopy(pinned)
        query(t.b, 'CLOSE typed_retained')
        closed = c.diagnostic(t.b)
        check('CLOSE releases actual cursor snapshot references',
              closed['registered_snapshots'], transaction_snapshot['registered_snapshots'])
        query(t.b, 'COMMIT')
        released = c.diagnostic(t.b)
        check('cursor transaction returns to identical diagnostic baseline',
              released['registered_snapshots'], outside_snapshot['registered_snapshots'])
        check('cursor leaves no staged business row journal', released['transaction_rows'], 0)
        r['retained_cursor'].update(closed=closed, released=released)
        barrier = {leaf: dict(first_safe=None, later_vacuum=None) for leaf in maps()}
        r['vacuum_reuse_barriers'] = barrier

        def later_safe_vacuum(image):
            for leaf, item in image['mapping'].items():
                if (item['last_vacuum_at'] is None or
                        item['last_vacuum_at'] == pinned['mapping'][leaf]['last_vacuum_at'] or
                        item['last_delete_xid'] is None or item['oldest_xmin_observed'] is None or
                        item['oldest_xmin_observed'] <= item['last_delete_xid']):
                    continue
                first = barrier[leaf]['first_safe']
                if first is None or first['last_delete_xid'] != item['last_delete_xid']:
                    barrier[leaf] = dict(first_safe=deepcopy(item), later_vacuum=None)
                elif first['last_vacuum_at'] != item['last_vacuum_at']:
                    barrier[leaf]['later_vacuum'] = deepcopy(item)
            return all(item['later_vacuum'] is not None for item in barrier.values())

        # Keep one original observation deadline. Do not restart a90s window
        # when first_safe appears; dead estimates need not be zero.
        observe('each typed head completes a later vacuum after its safe cutoff', later_safe_vacuum, deadline)
        guard.claim()
        check('both real heads empty before actual second population', counts(), {tid: 0 for tid in tids})
        before_second = sizes()
        for tid in tids:
            query(t.a, sql.SQL('INSERT INTO {}(row_id) VALUES(%s)').format(heads[tid]), (shared_r,))
            query(t.a, sql.SQL('INSERT INTO {}(row_id) SELECT i '
                              'FROM generate_series(10000,29998)i').format(heads[tid]))
        check('second actual population is20000 in each unchanged typed head', counts(), {tid: 20000 for tid in tids})
        second_sizes = sizes()
        r['space_reuse'] = dict(first_population=first_sizes, before_second_population=before_second,
                               second_population=second_sizes, rows_per_leaf=20000,
                               index_growth_is_recorded_not_required_zero=True)
        for tid in tids:
            check('real second population reuses leaf heap pages/'+str(tid),
                  second_sizes[tid][0] <= first_sizes[tid][0]+8192)
        invariant('typed worker vacuum and actual heap-page reuse', original)
        r['vacuum_and_reuse_case_checks_completed'] = True

    check('automatic path produced no hidden failed worker batch', state()['failed_batches'], worker_before['failed_batches'])
    check('automatic case finishes with exact owned maintenance claim', guard.held)
    r['status'] = 'case_checks_completed'


if __name__ == '__main__':
    raise SystemExit('prepare-only typed worker sidecar; no standalone execution')
