"""Closed private two-row EPQ regression; uses the existing owned fixture.

No new runtime, SQL timeout, supervisor, service, or cleanup implementation.
Both targets originate in the same actual Parquet file. The held transaction
changes both before the waiter starts, then commits only after a real XID/tuple
wait is proved. A single waiting statement must recheck both old file rows.
"""
from copy import deepcopy

MODE = 'wait-multirow'
EPQ_FIX = '7ce63fef4c75c12096367b763ae41ee25df8663215af184ced0fdc5d0e9de215'
EPQ_SOURCE = '0879a8e2e79671859ff6fccd385f3693bcdb951272e48b2ae14b9fe829441085'


def require_contract(contract):
    if (contract.get('native_multirow_epq_qualified_to_test') is not True or
            contract.get('ready_for_native_qualification') is not True or
            contract.get('epq_context_lifetime_fix_patch_sha256') != EPQ_FIX or
            contract.get('native_inputs', {}).get(
                'contrib/og_lake/lake_native_partition_modify.cpp') != EPQ_SOURCE):
        raise RuntimeError('multirow EPQ source/build/QA contract remains closed')


def run_case(c):
    require_contract(c.contract)
    query, scalar, check, t = c.query, c.scalar, c.check, c.t
    result = dict(mode=MODE, timelines=[], simultaneous_target_count=2,
                  manual_head_or_mapping_writes=0, runtime_epq_counter_available=False)
    c.r['multirow_epq'] = result
    targets = sorted((row for row in c.file_rows if row['shard'] in (1, 8)),
                     key=lambda row: row['shard'])
    check('two actual original file rows in the same leaf',
          [(row['shard'], row['id']) for row in targets], [(1, 99), (8, 8)])
    check('original file rows have two distinct logical identities',
          len({row['r'] for row in targets}), 2)
    check('original file rows have two distinct physical positions',
          len({row['p'] for row in targets}), 2)
    check('both targets still originate from committed Parquet',
          [row for row in c.inline_rows(c.low)
           if row[3] in (1, 8) and row[2] is None], [])
    expected_before = [(1, 99, 100, 'low-key-湖'), (8, 8, None, 'tail-湖')]
    check('literal initial target truth is independent of lake output',
          c.rows(t.observer, 'shard IN (1,8)'), expected_before)
    check('independent native heap has identical literal targets',
          query(t.observer, 'SELECT shard,id,val,payload FROM biz.exact '
                'WHERE shard IN (1,8) ORDER BY shard,id'), expected_before)
    logical_ids = [row['r'] for row in targets]
    head_name = scalar(t.observer, 'SELECT %s::oid::regclass::text',
                       (c.heads[c.low]['oid'],))
    first = ("UPDATE {table} SET val=1000+shard,payload='A-'||shard::text "
             'WHERE shard IN (1,8) RETURNING shard,id,val,payload')
    second = ("UPDATE {table} SET val=COALESCE(val,0)+1,payload=payload||'-B' "
              'WHERE shard IN (1,8) RETURNING shard,id,val,payload')
    expected_holder = [(1, 99, 1001, 'A-1'), (8, 8, 1008, 'A-8')]
    expected_waiter = [(1, 99, 1002, 'A-1-B'), (8, 8, 1009, 'A-8-B')]

    def timeline(table):
        query(t.a, 'BEGIN')
        query(t.b, 'BEGIN')
        holder_rows = c.command(t.a, table, first)
        check(table+' one holder statement changes both rows', sorted(holder_rows), expected_holder)
        if table == 'parent':
            heads = query(t.a, 'SELECT row_id,xmin::text,xmax::text FROM '+head_name+
                          ' WHERE row_id=ANY(%s::bigint[]) ORDER BY row_id', (logical_ids,))
            check('holder owns both actual same-leaf logical head tuples',
                  [row[0] for row in heads], sorted(logical_ids))
            result['holder_heads'] = dict(head_oid=c.heads[c.low]['oid'],
                leaf_oid=c.heads[c.low]['leaf'], rows=deepcopy(heads))
        holder = scalar(t.a, 'SELECT pg_backend_pid()')
        pending = c.async_command(t.b, table, second)
        proof = c.wait_holder(pending, holder)
        check(table+' actual native wait precedes holder commit', not pending['future'].done())
        query(t.a, 'COMMIT')
        holder_snapshot = t.head()[0]
        # Preserve the fixture's original post-holder completion deadline.
        # No extra inspection work is inserted before finish_worker.
        reply = c.finish_worker(pending)
        if reply['state'] != 'completed':
            query(t.b, 'ROLLBACK')
            check(table+' waiting two-row statement completes', reply['state'], 'completed')
        check(table+' single waiting statement returns two refreshed rows',
              sorted(reply['rows']), expected_waiter)
        check(table+' waiting native command tag', reply['tag'], 'UPDATE 2')
        if table == 'parent':
            visible_a = [row for row in c.inline_rows(c.low)
                         if row[3] in (1, 8) and row[2] is None]
            check('observer sees both A versions before B commits',
                  sorted((row[0], row[1], row[3], row[5], row[6]) for row in visible_a),
                  sorted((target['r'], holder_snapshot, target['shard'], 1000+target['shard'],
                          'A-'+str(target['shard'])) for target in targets))
            for target in targets:
                check('A changed committed birth for file row/'+str(target['shard']),
                      holder_snapshot != target['b'])
        query(t.b, 'COMMIT')
        waiter_snapshot = t.head()[0]
        result['timelines'].append(deepcopy(dict(table=table, first=first, second=second,
            holder_rows=holder_rows, wait=proof, reply=reply,
            holder_snapshot=holder_snapshot, waiter_snapshot=waiter_snapshot)))
        return reply, holder_snapshot, waiter_snapshot

    heap_reply, _, _ = timeline('exact')
    lake_reply, a_snapshot, b_snapshot = timeline('parent')
    check('both-row RETURNING equals the independent native heap timeline',
          sorted(lake_reply['rows']), sorted(heap_reply['rows']))
    check('both-row final full-table equality after query End', c.rows(), c.oracle())
    check('both-row updates preserve table-local allocators', c.next_ids(), c.first_next)
    versions = c.inline_rows(c.low)
    for target in targets:
        actual = sorted((row[1], row[2], row[3], row[5], row[6])
                        for row in versions if row[0] == target['r'])
        check('both committed versions retain exact R and half-open intervals/'+str(target['shard']),
              actual, [(a_snapshot, b_snapshot, target['shard'], 1000+target['shard'],
                        'A-'+str(target['shard'])),
                       (b_snapshot, None, target['shard'], 1001+target['shard'],
                        'A-'+str(target['shard'])+'-B')])
        check('original file deletion uses P and A snapshot/'+str(target['shard']),
              (c.file_id, target['p'], a_snapshot) in c.deletion_rows(c.low))
    result['status'] = 'case_checks_completed'


if __name__ == '__main__':
    raise SystemExit('private prepared sidecar: exact fixture/QA activation required')
