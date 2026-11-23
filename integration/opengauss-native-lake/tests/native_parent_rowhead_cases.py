"""Private SQL26 typed-parent row-head cases; never creates/runs a fixture.

The reviewed mutation harness supplies its real two-leaf fixture, independent
heap truth, exact source/SDK admission, supervisor and ownership cleanup.  This
sidecar is not independently executable and does not relax any admission.
"""
from copy import deepcopy

MODES = ('rowhead-identity', 'rowhead-busy-fairness',
         'rowhead-rollback', 'rowhead-bounded')
MAP_FIELDS = (
    'binding_id::text', 'relation_oid', 'row_head_oid', 'reclaim_cursor',
    'reclaim_round_end', 'reclaim_scanned', 'reclaimed_rows', 'reclaim_rounds',
    'last_reclaim_at::text', 'last_vacuum_at::text', 'live_rows_estimate',
    'dead_rows_estimate', 'head_bytes', 'soft_budget_exceeded',
    'last_delete_xid', 'oldest_xmin_observed', 'leaf_partition_oid', 'partition_no')


def run_case(c, mode):
    """Called only by a future independently activated exact-source harness."""
    if mode not in MODES:
        raise ValueError('unknown typed row-head mode')
    if (c.contract.get('typed_rowhead_qualified_to_test') is not True or
            c.contract.get('partition_sql_version') != '0.26' or
            c.contract.get('ready_for_native_qualification') is not True):
        raise RuntimeError('SQL26 typed row-head contract is still closed')
    query, scalar, sql, check, t = c.query, c.scalar, c.sql, c.check, c.t
    tids = (c.low, c.high)
    record = dict(mode=mode, calls=[], holder_checks=[], fixture='real native two RANGE leaves',
                  direct_mapping_writes=False, service_control=False,
                  automatic_vacuum_qualified=False)
    c.r['typed_rowhead'] = record

    def identity():
        value = query(t.observer,
            'SELECT h.binding_id::text,h.relation_oid,h.leaf_partition_oid,'
            'h.partition_no,h.row_head_oid,p.parent_relid,p.leaf_partition_oid,'
            'p.partition_no,b.table_id,b.server_oid,b.table_uuid::text,'
            'n.nspname,k.relname,k.relowner,k.relkind,k.relpersistence,'
            'ch.parentid,ch.partitionno,ch.parttype '
            'FROM og_lake_internal.row_lock_binding h '
            'JOIN og_lake_internal.partition_binding p ON p.binding_id=h.binding_id '
            'JOIN og_lake_internal.table_binding b ON b.binding_id=h.binding_id '
            'JOIN pg_class k ON k.oid=h.row_head_oid '
            'JOIN pg_namespace n ON n.oid=k.relnamespace '
            'JOIN pg_partition ch ON ch.oid=h.leaf_partition_oid '
            'WHERE h.relation_oid=%s ORDER BY h.leaf_partition_oid', (c.pid,))
        check('typed GC has exactly two canonical identity chains', len(value), 2)
        for row in value:
            expected = c.heads[row[8]]
            check('GC binding/parent/leaf/number/head match real fixture/'+str(row[8]),
                  row[:5], (expected['binding'], c.pid, expected['leaf'],
                            expected['no'], expected['oid']))
            check('GC map and real pg_partition agree/'+str(row[8]),
                  (row[5:8], row[16:19]),
                  ((c.pid, expected['leaf'], expected['no']),
                   (c.pid, expected['no'], 'p')))
            check('GC head is an owned private persistent heap/'+str(row[8]),
                  (row[11], row[14], row[15]), ('og_lake_internal', 'r', 'p'))
        check('GC typed identities remain distinct despite equal row_id',
              len({(row[0], row[2], row[4]) for row in value}), 2)
        return value

    original_identity = identity()
    head_sql = {row[8]: sql.Identifier(row[11], row[12]) for row in original_identity}
    record['original_identity'] = deepcopy(original_identity)

    def head_rows(reader=None):
        reader = reader or t.observer
        return {tid: [x[0] for x in query(reader,
                sql.SQL('SELECT row_id FROM {} ORDER BY row_id').format(head_sql[tid]))]
                for tid in tids}

    def map_rows(reader=None):
        reader = reader or t.observer
        rows = query(reader, 'SELECT '+','.join(MAP_FIELDS)+
                     ' FROM og_lake_internal.row_lock_binding '
                     'WHERE relation_oid=%s ORDER BY leaf_partition_oid', (c.pid,))
        check('GC preserves the complete two-row typed maintenance mapping', len(rows), 2)
        return {row[16]: row for row in rows}

    def business():
        return dict(protocol=t.protocol(t.observer), parent_maps=c.maps(c.pid),
                    rows=c.rows(t.observer), heap=c.oracle(t.observer),
                    next_ids=c.next_ids())

    def verify_business(label, expected):
        check(label+' preserves complete committed business/catalog state', business(), expected)
        check(label+' preserves exact head and typed storage identities', identity(), original_identity)

    def seed(tid, first, count):
        # Capacity probes insert only synchronization keys into already-owned
        # real head heaps. No binding, cursor, statistics or status is forged.
        query(t.b, sql.SQL('INSERT INTO {}(row_id) '
                          'SELECT i FROM generate_series(%s,%s)i').format(head_sql[tid]),
              (first, first+count-1))

    def batch(limit=512, actor=None):
        actor = actor or t.b
        before, before_maps = head_rows(actor), map_rows(actor)
        removed = int(scalar(actor, 'SELECT og_lake.reclaim_row_heads(%s::regclass,%s)',
                             (c.pid, limit)))
        after, after_maps = head_rows(actor), map_rows(actor)
        losses = {tid: sorted(set(before[tid])-set(after[tid])) for tid in tids}
        affected = [tid for tid in tids if losses[tid]]
        changed_maps = [leaf for leaf in before_maps if before_maps[leaf] != after_maps[leaf]]
        scanned = {leaf: after_maps[leaf][5]-before_maps[leaf][5] for leaf in before_maps}
        deleted = {leaf: after_maps[leaf][6]-before_maps[leaf][6] for leaf in before_maps}
        check('every leaf inspected-key delta respects the caller bound',
              all(0 <= count <= limit for count in scanned.values()))
        check('one call inspects no more than the caller limit in total',
              0 <= sum(scanned.values()) <= limit <= 512)
        check('actual returned deletions never exceed inspected keys',
              0 <= removed <= sum(scanned.values()))
        for tid in tids:
            leaf = c.heads[tid]['leaf']
            check('persistent deletion count equals real typed row loss/'+str(tid),
                  deleted[leaf], len(losses[tid]))
            if leaf not in changed_maps:
                check('unselected leaf has no inspected or deleted counter delta/'+str(tid),
                      (scanned[leaf], deleted[leaf]), (0, 0))
        check('one GC call never creates or transfers synchronization keys',
              all(set(after[tid]) <= set(before[tid]) for tid in tids))
        check('one GC call removes keys from at most one real leaf', len(affected) <= 1)
        check('GC return count equals observed same-transaction physical row loss',
              removed, sum(len(x) for x in losses.values()))
        check('one GC call respects caller limit and absolute512 cap', 0 <= removed <= limit <= 512)
        check('one GC call updates at most one leaf maintenance record', len(changed_maps) <= 1)
        if affected:
            check('deleted keys and maintenance update have same typed leaf',
                  changed_maps, [c.heads[affected[0]]['leaf']])
        call = dict(limit=limit, returned=removed, before=before, after=after,
                    losses=losses, affected=affected, before_maps=before_maps,
                    after_maps=after_maps, changed_maps=changed_maps,
                    scanned_deltas=scanned, deletion_counter_deltas=deleted)
        record['calls'].append(deepcopy(call))
        check('manual GC retains canonical parent/leaf/head identities', identity(), original_identity)
        return call

    lo = [row for row in c.file_rows if row['shard'] == 1]
    hi = [row for row in c.inline_rows(c.high) if row[3] == 10 and row[2] is None]
    check('actual same-business-key input exists once in each leaf', (len(lo), len(hi)), (1, 1))
    check('two independent leaf allocators really used same numeric R', lo[0]['r'], hi[0][0])
    shared_r = lo[0]['r']
    record['shared_logical_row_id'] = shared_r
    check('two actual business rows share only the business-key value',
          (lo[0]['id'], hi[0][4]), (99, 99))

    def warm_real_heads():
        query(t.a, 'BEGIN')
        result = c.both(t.a, 'UPDATE {table} SET val=val WHERE id=99 '
                            'RETURNING shard,id,val,payload')
        check('real business DML reaches both independent same-R lock domains', len(result), 2)
        query(t.a, 'COMMIT')
        check('real DML creates exactly the correct two single-key heads',
              head_rows(), {tid: [shared_r] for tid in tids})
        check('warming existing logical rows does not consume new row IDs',
              c.next_ids(), c.first_next)
        return business()

    if mode == 'rowhead-identity':
        original = warm_real_heads()
        first, second = batch(1), batch(1)
        check('two fair manual calls remove one independently identified head each',
              (first['returned'], second['returned']), (1, 1))
        check('equal numeric R is not mistaken for one shared head',
              sorted(set(first['affected']+second['affected'])), sorted(tids))
        check('both actual heads empty after exact two-leaf reclamation',
              head_rows(), {tid: [] for tid in tids})
        check('empty-head maintenance returns zero', batch(1)['returned'], 0)
        check('other empty head also completes its original round', batch(1)['returned'], 0)
        check('real empty rounds reset both persistent cursors before rewarming',
              {leaf: (row[3], row[4]) for leaf, row in map_rows().items()},
              {c.heads[tid]['leaf']: (-1, -1) for tid in tids})
        verify_business('same-R reclamation', original)

        # Two calls starting from NULL timestamps are insufficient: both the
        # old statement clock and the corrected wall clock naturally visit
        # each leaf once. Four dependent calls with two real keys per leaf
        # expose the old third/fourth-call tie without forged mapping state.
        query(t.a, 'BEGIN')
        warmed = c.both(t.a, 'UPDATE {table} SET val=val '
                       'WHERE shard IN (1,8,10,19) RETURNING shard,id,val,payload')
        check('same-statement fairness starts from four real business writes', len(warmed), 4)
        query(t.a, 'COMMIT')
        four_heads = {
            c.low: sorted(row['r'] for row in c.file_rows if row['shard'] in (1, 8)),
            c.high: sorted(row[0] for row in c.inline_rows(c.high)
                           if row[3] in (10, 19) and row[2] is None)}
        check('same-statement fixture has two original logical IDs per leaf',
              {tid: len(values) for tid, values in four_heads.items()}, {tid: 2 for tid in tids})
        check('four real writes created precisely their typed lock keys', head_rows(), four_heads)
        original = business()
        before_maps = map_rows()
        same_statement = query(t.b,
            'WITH RECURSIVE calls(step,removed) AS ('
            'SELECT 1,og_lake.reclaim_row_heads(%s::regclass,1) UNION ALL '
            'SELECT step+1,og_lake.reclaim_row_heads(%s::regclass,1) '
            'FROM calls WHERE step<4) SELECT step,removed FROM calls ORDER BY step',
            (c.pid, c.pid))
        check('four ordered calls in one statement each reclaim exactly one key',
              same_statement, [(1, 1), (2, 1), (3, 1), (4, 1)])
        after_maps = map_rows()
        leaf_deltas = {leaf: after_maps[leaf][6]-before_maps[leaf][6] for leaf in before_maps}
        check('same statement fairly visits each real leaf twice',
              leaf_deltas, {c.heads[tid]['leaf']: 2 for tid in tids})
        check('four idle limit1 calls inspect exactly two keys per leaf',
              {leaf: after_maps[leaf][5]-before_maps[leaf][5] for leaf in before_maps},
              {c.heads[tid]['leaf']: 2 for tid in tids})
        check('same-statement fairness leaves neither real head stranded',
              head_rows(), {tid: [] for tid in tids})
        record['same_statement_fairness'] = dict(calls=same_statement, before_maps=before_maps,
                                                 after_maps=after_maps, original_keys=four_heads)
        verify_business('same-statement fair maintenance', original)

    elif mode == 'rowhead-busy-fairness':
        for held_tid, held_shard, free_tid in [(c.low, 1, c.high), (c.high, 10, c.low)]:
            original = warm_real_heads()
            query(t.a, 'BEGIN')
            held = c.both(t.a, 'UPDATE {table} SET val=val+7 WHERE shard=%s '
                              'RETURNING shard,id,val,payload', (held_shard,))
            check('real business transaction holds exactly one leaf target', len(held), 1)
            holder_pid = scalar(t.a, 'SELECT pg_backend_pid()')
            held_rows = query(t.a, sql.SQL('SELECT row_id,xmin::text,xmax::text '
                                          'FROM {} WHERE row_id=%s').format(head_sql[held_tid]),
                              (shared_r,))
            try:
                query(t.observer, sql.SQL('SELECT row_id FROM {} WHERE row_id=%s '
                                         'FOR UPDATE NOWAIT').format(head_sql[held_tid]), (shared_r,))
            except Exception as exc:
                check('independent native tuple-lock probe proves holder ownership',
                      getattr(exc, 'pgcode', None), '55P03')
            else:
                raise AssertionError('actual busy head was unexpectedly lockable')
            record['holder_checks'].append(dict(holder_pid=holder_pid, held_tid=held_tid,
                                                held_leaf=c.heads[held_tid]['leaf'], rows=held_rows))
            progressed = False
            # A busy business writer locks a head tuple, not necessarily the
            # map. The first selection may choose that busy leaf. Require
            # bounded progress without asserting that the first call skips it.
            for attempt in range(4):
                batch(1)
                current = head_rows()
                check('manual GC never deletes a currently held same-R head',
                      current[held_tid], [shared_r])
                if current[free_tid] == []:
                    progressed = True
                    break
            check('other leaf progresses within four bounded selections', progressed)
            verify_business('busy-leaf committed visibility', original)
            query(t.a, 'ROLLBACK')
            drained = False
            for attempt in range(4):
                batch(1)
                if head_rows() == {tid: [] for tid in tids}:
                    drained = True
                    break
            check('released former writer head is reclaimable within four selections', drained)
            verify_business('post-writer rollback maintenance', original)

    elif mode == 'rowhead-rollback':
        original = warm_real_heads()
        for tid in tids:
            seed(tid, 10000, 7)
        heads_before, maps_before = head_rows(), map_rows()
        query(t.b, 'BEGIN')
        query(t.b, 'SAVEPOINT reclaim_child')
        check('child transaction actually reclaims three keys', batch(3)['returned'], 3)
        check('uncommitted reclamation stays invisible to independent observer',
              (head_rows(), map_rows()), (heads_before, maps_before))
        query(t.b, 'ROLLBACK TO reclaim_child')
        query(t.b, 'RELEASE reclaim_child')
        check('child abort restores head rows and every maintenance field',
              (head_rows(t.b), map_rows(t.b)), (heads_before, maps_before))
        check('same outer transaction remains usable for a second real reclaim', batch(3)['returned'], 3)
        query(t.b, 'ROLLBACK')
        check('outer abort restores both typed mappings and all synchronization rows',
              (head_rows(), map_rows()), (heads_before, maps_before))
        verify_business('subtransaction and outer rollback', original)

    elif mode == 'rowhead-bounded':
        original = business()
        for tid in tids:
            seed(tid, 10000, 1025)
        check('capacity fixture contains1025 independent keys in each real leaf',
              {tid: len(v) for tid, v in head_rows().items()}, {tid: 1025 for tid in tids})
        before_invalid = (head_rows(), map_rows())
        for bad in (0, 513):
            try:
                query(t.b, 'SELECT og_lake.reclaim_row_heads(%s::regclass,%s)', (c.pid, bad))
            except Exception as exc:
                check('native public limit rejects invalid bound/'+str(bad),
                      getattr(exc, 'pgcode', None), '22023')
            else:
                raise AssertionError('invalid manual batch limit was accepted')
            check('invalid batch size has no maintenance side effects',
                  (head_rows(), map_rows()), before_invalid)
        first, second = batch(512), batch(512)
        # The API bounds work by both 512 keys and 1000ms, so a busy machine
        # may complete fewer than512 even with no competing tuple locker.
        # Inspect actual losses and chosen maps; never assert a throughput.
        check('each capacity selection updates one real maintenance map',
              (len(first['changed_maps']), len(second['changed_maps'])), (1, 1))
        check('first two capacity calls rotate over different canonical leaves',
              sorted(first['changed_maps']+second['changed_maps']),
              sorted(c.heads[tid]['leaf'] for tid in tids))
        check('bounded deletion counts stay independent per leaf',
              {tid: len(v) for tid, v in head_rows().items()},
              {tid: 1025-len(first['losses'][tid])-len(second['losses'][tid]) for tid in tids})
        total = first['returned']+second['returned']
        for attempt in range(30):
            total += batch(512)['returned']
            if head_rows() == {tid: [] for tid in tids}:
                break
        check('finite bounded calls reclaim exactly2050 synchronization keys', total, 2050)
        check('both capacity heads logically empty', head_rows(), {tid: [] for tid in tids})
        verify_business('capacity-only maintenance', original)

    check('typed manual GC preserves complete independent heap business truth',
          c.rows(t.observer), c.oracle(t.observer))
    record['status'] = 'case_checks_completed'


if __name__ == '__main__':
    raise SystemExit('prepare-only sidecar; use reviewed future SQL26 fixture harness')
