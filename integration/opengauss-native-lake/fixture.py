#!/usr/bin/env python3
"""Generate a real fixture through the registered reference adapter, never by fabricating IDs."""
import json
import os
from pathlib import Path
import uuid

from lake_harness import (Blocked, Context, EXIT, check_dependencies, context_from, execute, expand_command,
                          fingerprint, now, parser, read_json, registry, sha256, validate_case_id, write_json)



def validate_multitable_manifest(manifest):
    """Validate observed two-table records, never manufacture missing identities."""
    entries = manifest.get("business_tables", [])
    if (len(entries) != 2 or {entry.get("alias") for entry in entries} != {"t", "u"}
            or len({entry.get("table_id") for entry in entries}) != 2):
        raise ValueError("MULTITABLE needs exactly two distinct observed T/U table identities")
    schema = [{"name": "id", "type": "INTEGER"}, {"name": "val", "type": "INTEGER"}]
    expected = {"t": [(1, 10), (2, 20), (3, 30)], "u": [(101, 100), (102, 200)]}
    snapshots = manifest.get("snapshots", [])
    snapshot_ids = {row["snapshot_id"] for row in snapshots}
    if not snapshot_ids or manifest["snapshot_aliases"].get("S10") != max(snapshot_ids):
        raise ValueError("MULTITABLE current snapshot alias must reference the observed catalog head")
    carriers_seen = set()
    for entry in entries:
        alias, table_id = entry["alias"], entry["table_id"]
        definition = entry.get("table_definition", {})
        if (type(table_id) is not int or table_id < 0 or entry.get("schema") != schema
                or entry.get("schema_name") != "biz" or entry.get("table_name") != alias
                or definition.get("table_id") != table_id or definition.get("table_name") != alias
                or definition.get("end_snapshot") is not None):
            raise ValueError("MULTITABLE schema or observed table definition is inconsistent")
        business = entry.get("expected", [])
        identities = entry.get("row_identity_map", [])
        if ([(row["id"], row["val"]) for row in business] != expected[alias]
                or entry.get("visible_count_expected") != len(business)
                or [{key: row[key] for key in ("id", "val", "logical_rowid")} for row in identities] != business):
            raise ValueError("MULTITABLE actual business values/identity observations differ")
        rowids = [row["logical_rowid"] for row in identities]
        if (len(set(rowids)) != len(business)
                or any(type(value) is not int or value < 0 or value >= 10**18 for value in rowids)
                or any(row["begin_snapshot"] not in snapshot_ids for row in identities)):
            raise ValueError("MULTITABLE observed row IDs or version bounds are invalid")
        carriers = entry.get("inline_tables", [])
        physical = entry.get("inline_rows", {})
        if (entry.get("data_files") or len(carriers) != 1 or carriers[0].get("table_id") != table_id
                or set(physical) != {carriers[0].get("table_name")}):
            raise ValueError("MULTITABLE needs one actual pure-inline carrier per table")
        carrier = carriers[0]["table_name"]
        if carrier in carriers_seen:
            raise ValueError("Distinct MULTITABLE tables cannot share a physical inline carrier")
        carriers_seen.add(carrier)
        wanted = [{"_ducklake_row_id": row["logical_rowid"], "_ducklake_begin_snapshot": row["begin_snapshot"],
                   "_ducklake_end_snapshot": None, "id": row["id"], "val": row["val"]} for row in identities]
        if sorted(physical[carrier], key=lambda row: row["id"]) != sorted(wanted, key=lambda row: row["id"]):
            raise ValueError("MULTITABLE physical observations differ from genuine logical reads")
        stats = entry.get("table_stats", [])
        if (len(stats) != 1 or stats[0].get("table_id") != table_id
                or stats[0].get("record_count") != len(business)
                or stats[0].get("next_row_id") != len(business)
                or stats[0].get("file_size_bytes") != 0
                or set(rowids) != set(range(stats[0]["next_row_id"]))):
            raise ValueError("MULTITABLE fresh legal INSERT counts/frontiers are inconsistent")
    first = next(entry for entry in entries if entry["alias"] == "t")
    for key in ("table_id", "schema", "expected", "row_identity_map", "table_stats", "inline_tables", "inline_rows"):
        if manifest[key] != first[key]:
            raise ValueError("MULTITABLE top-level compatibility projection differs: " + key)
    if manifest.get("data_files") or manifest.get("physical_positions"):
        raise ValueError("MULTITABLE cannot contain a file-backed physical layout")
    catalog = Path(manifest["catalog"]).resolve()
    owner = Path(manifest["cleanup_owner"]).resolve()
    if not catalog.is_relative_to(owner) or str(catalog) not in {item["path"] for item in manifest["files"]}:
        raise ValueError("MULTITABLE catalog must be an immutable artifact inside its owned directory")
    provenance = manifest.get("generation_provenance", {})
    generator = Path(provenance.get("generator_path", ""))
    if (not generator.is_absolute() or not generator.is_file()
            or sha256(generator) != provenance.get("generator_sha256")
            or provenance.get("source_lock_hash") != manifest["source_lock_hash"]
            or provenance.get("binary_sha256") != manifest.get("binary_sha256")):
        raise ValueError("MULTITABLE genuine generator/binary provenance is incomplete or changed")


def validate_manifest(manifest, ctx):
    # EMPTY has no business table_id; do not manufacture one for legacy shape.
    if manifest.get("case") in ("EMPTY", "EMPTY_BUNDLE"):
        import sys
        tests_dir = str(Path(__file__).resolve().parent / "tests")
        if tests_dir not in sys.path:
            sys.path.insert(0, tests_dir)
        from empty_manifest import validate_empty_manifest, validate_empty_bundle
        if manifest["case"] == "EMPTY":
            validate_empty_manifest(manifest, ctx)
        else:
            validate_empty_bundle(manifest, ctx)
        manifest["assertions"] = [{"name": "genuine_empty_reference_shape_and_provenance", "status": "pass"}]
        return manifest
    required = ("run_id", "source_lock_hash", "catalog", "table_id", "snapshot_aliases",
                "row_identity_map", "file_aliases", "physical_positions", "schema",
                "layout_settings", "expected", "cleanup_owner")
    absent = [key for key in required if key not in manifest]
    if absent:
        raise ValueError("Fixture manifest missing fields: " + ", ".join(absent))
    if manifest["run_id"] != ctx.run_id and manifest.get("parent_run_id") != ctx.run_id:
        raise ValueError("Fixture belongs to another run")
    source_lock = ctx.driver / "source-lock.json"
    if not source_lock.is_file() or manifest["source_lock_hash"] != sha256(source_lock):
        raise Blocked("Fixture source-lock is missing or stale")
    if str(manifest.get("format_version")) != "1.1":
        raise ValueError("Fixture must explicitly verify DuckLake format 1.1")
    if manifest.get("status") != "verified_reference_only" or manifest.get("automatic_migration") is not False:
        raise ValueError("Expected a verified reference-only fixture with automatic migration disabled")
    lock = read_json(source_lock)
    if manifest.get("reference_commit") != lock["repositories"]["ducklake"]["commit"]:
        raise ValueError("Fixture references another DuckLake commit")
    if manifest.get("duckdb_commit") != lock["repositories"]["duckdb"]["commit"]:
        raise ValueError("Fixture references another DuckDB commit")
    artifacts = manifest.get("files", [])
    if not artifacts or not Path(manifest["catalog"]).is_file():
        raise ValueError("Real catalog and fixture artifacts must exist")
    for item in artifacts:
        path = Path(item["path"])
        if not path.is_absolute() or not path.is_file() or sha256(path) != item.get("sha256"):
            raise ValueError("Fixture artifact missing, relative, or changed")
    profile = manifest.get("case")
    if profile not in {"F", "I", "N", "HISTORY", "TYPED", "LONG_TEXT", "INLINE_HISTORY", "SCHEMA", "MULTITABLE", "ALL_NULL", "UNKNOWN_BOUNDS", "USTORE_STREAM", "EMPTY_TABLE", "EMPTY_NO_INLINE", "ANALYZE_DISTRIBUTION", "FILE_TYPED", "FILE_BATCH", "FILE_MIXED", "FILE_INLINE_UPDATE", "FILE_INLINE_DELETE", "FILE_VERSIONED", "FILE_VERSIONED_BATCH", "FILE_SCHEMA_GAP", "FILE_UNKNOWN_BOUNDS"}:
        raise ValueError("Unknown verified reference fixture profile")
    if profile in {"F", "I", "N", "HISTORY"}:
        rows = [(row["id"], row["val"]) for row in manifest["expected"]]
        expected = [] if profile == "N" else [(1, 30 if profile == "HISTORY" else 10), (2, 100), (3, 200)]
        if rows != expected:
            raise ValueError("Fixture business values differ from the specified initial/history state")
    else:
        names = [column["name"] for column in manifest["schema"]]
        if not names or any(any(name not in row for name in names) for row in manifest["expected"]):
            raise ValueError("Reference rows do not cover the declared business schema")
    if profile in ("ALL_NULL", "UNKNOWN_BOUNDS"):
        expected_schema = [{"name":"id","type":"INTEGER"}, {"name":"val","type":"INTEGER"},
                           {"name":"flag","type":"BOOLEAN"}, {"name":"bigval","type":"BIGINT"},
                           {"name":"note","type":"VARCHAR"}]
        names = [column["name"] for column in expected_schema]
        actual = [{name:row[name] for name in names} for row in manifest["expected"]]
        wanted = [dict(id=i,val=None,flag=None,bigval=None,note=None) for i in (1,2,3)]
        if profile == "UNKNOWN_BOUNDS":
            wanted[0] = dict(id=1,val=0,flag=False,bigval=0,note="")
            if any(row.get("value") not in (None, "") for row in manifest.get("cleared_skip_options", [])):
                raise ValueError("UNKNOWN_BOUNDS must clear skip_stats_columns through the normal option API")
        stats = manifest.get("table_stats", [])
        if (manifest["schema"] != expected_schema or actual != wanted or manifest.get("data_files")
                or sum(map(len,manifest.get("inline_rows", {}).values())) != 3
                or len(stats) != 1 or stats[0].get("record_count") != 3 or stats[0].get("next_row_id") != 3):
            raise ValueError(profile + " requires three genuine inline rows with nonempty-table counters")
        by_id = {row["column_id"]:row for row in manifest.get("column_stats", [])}
        by_name = {row["column_name"]:by_id.get(row["column_id"])
                   for row in manifest.get("column_definitions", [])}
        for name in ("val","flag","bigval","note"):
            observed = by_name.get(name)
            if (observed is None or observed.get("contains_null") is not True
                    or observed.get("min_value") is not None or observed.get("max_value") is not None):
                raise ValueError(profile + " must preserve observed initial NULL column statistics: " + name)
    if profile in ("EMPTY_TABLE", "EMPTY_NO_INLINE"):
        if (manifest["schema"] != [{"name":"id","type":"INTEGER"}, {"name":"val","type":"INTEGER"}]
                or manifest["expected"] or manifest.get("visible_count_expected") != 0
                or manifest.get("table_stats") or manifest.get("column_stats")
                or any(manifest.get("inline_rows", {}).values())
                or manifest.get("data_files") or manifest["row_identity_map"]
                or manifest["physical_positions"]):
            raise ValueError(profile + " must preserve a real never-inserted table without data/statistics")
        if (type(manifest["table_id"]) is not int or manifest["table_id"] < 0
                or type(manifest["snapshot_aliases"].get("S10")) is not int):
            raise ValueError(profile + " lacks its actually observed table/snapshot identity")
        carriers, physical = manifest.get("inline_tables", []), manifest.get("inline_rows", {})
        limit = manifest.get("layout_settings", {}).get("data_inlining_row_limit")
        if profile == "EMPTY_TABLE":
            if (limit != 100 or len(carriers) != 1 or carriers[0].get("table_id") != manifest["table_id"]
                    or set(physical) != {carriers[0].get("table_name")} or any(physical.values())):
                raise ValueError("EMPTY_TABLE inline100 must retain its actual empty CREATE-time carrier")
        elif limit != 0 or carriers or physical:
            raise ValueError("EMPTY_NO_INLINE must record actual limit0 CREATE and absent carrier")
    if profile == "ANALYZE_DISTRIBUTION":
        wanted_schema = [{"name":"id","type":"INTEGER"}, {"name":"val","type":"INTEGER"},
                         {"name":"flag","type":"BOOLEAN"}, {"name":"note","type":"VARCHAR"}]
        wanted = [dict(id=i,val=1 if i<=6000 else i,
            flag=None if i%4==0 else i%2==0,
            note=None if i%5==0 else ("hot" if i<=6000 else "k"+str(i%100))) for i in range(1,12001)]
        actual = [{name:row[name] for name in ("id","val","flag","note")} for row in manifest["expected"]]
        stats, carriers = manifest.get("table_stats", []), manifest.get("inline_tables", [])
        if (manifest["schema"] != wanted_schema or actual != wanted or manifest.get("data_files")
                or manifest.get("visible_count_expected") != 12000 or len(carriers) != 1
                or sum(map(len,manifest.get("inline_rows", {}).values())) != 12000
                or len(stats) != 1 or stats[0].get("record_count") != 12000 or stats[0].get("next_row_id") != 12000
                or manifest.get("layout_settings", {}).get("data_inlining_row_limit") != 1000000):
            raise ValueError("ANALYZE_DISTRIBUTION requires complete actual 12000-row input and layout")
        identities = manifest["row_identity_map"]
        if (len(identities) != 12000 or {row["logical_rowid"] for row in identities} != set(range(12000))
                or any(type(row["begin_snapshot"]) is not int or row["begin_snapshot"]<0 for row in identities)):
            raise ValueError("ANALYZE_DISTRIBUTION actual identity/commit observations differ")
        physical = manifest["inline_rows"].get(carriers[0]["table_name"], [])
        expected_physical = [dict(_ducklake_row_id=r["logical_rowid"],
            _ducklake_begin_snapshot=r["begin_snapshot"],_ducklake_end_snapshot=None,
            **{n:r[n] for n in ("id","val","flag","note")}) for r in identities]
        if sorted(physical,key=lambda r:r["id"]) != sorted(expected_physical,key=lambda r:r["id"]):
            raise ValueError("ANALYZE_DISTRIBUTION physical versions differ from real logical reads")
    if profile == "USTORE_STREAM":
        import hashlib
        wanted_schema = [{"name":"id","type":"INTEGER"}, {"name":"val","type":"INTEGER"},
                         {"name":"note","type":"VARCHAR"}]
        wanted = [dict(id=i,val=i*10,note="".join(hashlib.sha256(f"{i}:{chunk}".encode()).hexdigest()
                  for chunk in range(6))) for i in range(1,1025)]
        actual = [{name:row[name] for name in ("id","val","note")} for row in manifest["expected"]]
        stats = manifest.get("table_stats", [])
        carriers = manifest.get("inline_tables", [])
        if (manifest["schema"] != wanted_schema or actual != wanted or manifest.get("data_files")
                or len(carriers) != 1 or sum(map(len,manifest.get("inline_rows", {}).values())) != 1024
                or len(stats) != 1 or stats[0].get("record_count") != 1024 or stats[0].get("next_row_id") != 1024
                or manifest.get("layout_settings", {}).get("data_inlining_row_limit") != 1000000):
            raise ValueError("USTORE_STREAM requires the complete genuine 1024-row inline business fixture")
    if profile == "MULTITABLE":
        validate_multitable_manifest(manifest)
    if profile=='FILE_UNKNOWN_BOUNDS':
        if len(manifest.get('data_files',[]))!=1 or any(manifest.get('inline_rows',{}).values()):
            raise ValueError('FILE_UNKNOWN_BOUNDS must keep one genuine pure-file source')
        if [(r['id'],r['val'])for r in manifest['expected']]!=[(1,0),(2,None),(3,None)]:
            raise ValueError('FILE_UNKNOWN_BOUNDS needs old non-NULL value and two NULLs')
        if any(r['value']not in(None,'')for r in manifest.get('cleared_skip_options',[])):
            raise ValueError('FILE_UNKNOWN_BOUNDS must clear its effective skip option')
    if profile=='FILE_SCHEMA_GAP':
        if len(manifest.get('data_files',[]))!=1 or any(manifest.get('inline_rows',{}).values()):
            raise ValueError('FILE_SCHEMA_GAP must keep one genuine pure-file source after unrelated lake DDL')
        if [(r['id'],r['val'])for r in manifest['expected']]!=[(1,10),(2,100),(3,200)]:
            raise ValueError('FILE_SCHEMA_GAP changed the target business rows')
    if profile=='FILE_VERSIONED_BATCH':
        files=manifest.get('data_files',[]);positions=manifest.get('physical_positions',[]);deletes=manifest.get('versioned_delete_rows',[])
        wanted_ids=[rid for rid in range(12288)for _ in range(3 if rid<8192 else 1)]
        wanted=[(i,i*(30 if i<=8192 else 10))for i in range(1,12289)]
        if len(files)!=1 or files[0]['record_count']!=28672 or len(positions)!=28672 or len(deletes)!=16384:
            raise ValueError('FILE_VERSIONED_BATCH must retain all28672 versions and16384 position deletes')
        if [row['_ducklake_internal_row_id']for row in positions]!=wanted_ids or [(r['id'],r['val'])for r in manifest['expected']]!=wanted:
            raise ValueError('FILE_VERSIONED_BATCH identities or business data differ')
    if profile=='FILE_VERSIONED':
        files=manifest.get('data_files',[]);positions=manifest.get('physical_positions',[])
        deletes=manifest.get('delete_files',[]);delete_rows=manifest.get('versioned_delete_rows',[])
        if len(files)!=1 or files[0]['record_count']!=5 or len(deletes)!=1 or len(positions)!=5 or len(delete_rows)!=2:
            raise ValueError('FILE_VERSIONED must retain actual flushed data and delete populations')
        if [row['_ducklake_internal_row_id']for row in positions]!=[0,0,0,1,2]:
            raise ValueError('FILE_VERSIONED must preserve original logical identities')
        if [row['file_row_number']for row in positions]!=list(range(5)) or [row['pos']for row in delete_rows]!=[0,1]:
            raise ValueError('FILE_VERSIONED physical positions differ')
        if [(r['id'],r['val'])for r in manifest['expected']]!=[(1,30),(2,100),(3,200)] or any(manifest.get('inline_rows',{}).values()):
            raise ValueError('FILE_VERSIONED visible result/inline layout differs')
    if profile in ('FILE_INLINE_UPDATE','FILE_INLINE_DELETE'):
        files=manifest.get('data_files',[]);deletes=manifest.get('inline_file_deletes',[])
        if len(files)!=1 or files[0]['record_count']!=3 or len(deletes)!=1:
            raise ValueError(profile+' must contain the actual three-row file and one inline deletion')
        if deletes[0]['file_id']!=files[0]['data_file_id'] or deletes[0]['row_id']!=0:
            raise ValueError(profile+' deletion must reference position zero of its actual file')
        wanted=[(1,11),(2,100),(3,200)] if profile=='FILE_INLINE_UPDATE' else [(2,100),(3,200)]
        if [(r['id'],r['val'])for r in manifest['expected']]!=wanted:
            raise ValueError(profile+' business rows differ from genuine DML')
        if sum(map(len,manifest.get('inline_rows',{}).values()))!=(1 if profile=='FILE_INLINE_UPDATE' else 0):
            raise ValueError(profile+' inline population differs')
    if profile in ("FILE_TYPED","FILE_BATCH","FILE_MIXED"):
        population=12288 if profile=="FILE_BATCH" else 3
        files=manifest.get('data_files',[]);positions=manifest.get('physical_positions',[])
        expected_count=4 if profile=='FILE_MIXED' else population
        if len(files)!=1 or files[0]['record_count']!=population or len(positions)!=population or len(manifest['expected'])!=expected_count:
            raise ValueError(profile+' must preserve the complete actual file/row populations')
        if [row['file_row_number'] for row in positions]!=list(range(population)):
            raise ValueError(profile+' must retain absolute original file positions')
        inline_count=sum(map(len,manifest.get('inline_rows',{}).values()))
        if inline_count!=(1 if profile=='FILE_MIXED' else 0):
            raise ValueError(profile+' has the wrong inline/file split')
        if profile!='FILE_MIXED' and not {'BOOLEAN','INTEGER','BIGINT','VARCHAR'}.issubset({c['type'] for c in manifest['schema']}):
            raise ValueError(profile+' must exercise four native scalar types')
    if profile == "TYPED":
        if not {"BOOLEAN", "INTEGER", "BIGINT", "VARCHAR"}.issubset(
                {column["type"] for column in manifest["schema"]}):
            raise ValueError("TYPED must include the four first-slice scalar types")
        if not any(value is None for row in manifest["expected"] for value in row.values()):
            raise ValueError("TYPED must include actual NULL values")
        if manifest.get("data_files"):
            raise ValueError("TYPED must remain an inline fixture")
    if profile == "LONG_TEXT":
        names = [column["name"] for column in manifest["schema"] if column["type"] == "VARCHAR"]
        if not any(isinstance(row[name], str) and len(row[name]) > 8192
                   for name in names for row in manifest["expected"]):
            raise ValueError("LONG_TEXT must contain actual exported text longer than 8192 characters")
        if manifest.get("data_files"):
            raise ValueError("LONG_TEXT must remain an inline fixture")
    if profile == "INLINE_HISTORY":
        states = {state["label"]: state for state in manifest.get("catalog_states", [])}
        if not {"S10", "S12"}.issubset(states):
            raise ValueError("INLINE_HISTORY needs real before/after catalog copies")
        for state in states.values():
            if not Path(state["catalog"]).is_file() or sha256(state["catalog"]) != state["path_sha256"]:
                raise ValueError("Historical reference catalog copy is missing or changed")
        if manifest.get("data_files"):
            raise ValueError("INLINE_HISTORY must not require a file reader")
    if profile == "F" and (len(manifest.get("data_files", [])) != 1
                           or any(manifest.get("inline_rows", {}).values())
                           or len(manifest["physical_positions"]) != 3):
        raise ValueError("F requires one data file, three observed positions and no inline rows")
    if profile == "I" and (manifest.get("data_files")
                           or sum(map(len, manifest.get("inline_rows", {}).values())) != 3):
        raise ValueError("I requires three real inline rows and no data files")
    if profile == "N":
        stats = manifest.get("table_stats", [])
        if (len(stats) != 1 or not isinstance(stats[0].get("record_count"), int)
                or stats[0]["record_count"] < 0 or not isinstance(stats[0].get("next_row_id"), int)
                or stats[0]["next_row_id"] < 0):
            raise ValueError("N requires one legitimate nonnegative reference statistics/allocation record")
        # A legal inline DELETE can leave record_count=1 for an empty visible
        # result. The reference query above, not stored-row statistics, proves N.
    if profile == "HISTORY" and set(manifest.get("history_expected", {})) != {"S10", "S11", "S12", "S13"}:
        raise ValueError("HISTORY lacks the required historical query results")
    manifest["assertions"] = [{"name": name, "status": "pass"} for name in
                              ("source_lock_and_reference_commits", "explicit_format_1_1",
                               "real_artifacts_sha256", "business_values_and_layout")]
    return manifest


def create_fixture_manifest(case, ctx, attempt):
    """Shared by the fixture CLI and cases that require an automatic fixture."""
    spec = case.get("fixture")
    if not spec or spec.get("implementation") != "implemented":
        raise Blocked("Real fixture generator is not implemented for this case")
    check_dependencies(spec.get("requires", []), ctx)
    attempt.mkdir(parents=True, exist_ok=True)
    manifest_file = attempt / "manifest.json"
    # Multiple cases can require F in one gate. Each gets a separate real catalog.
    fixture_id = ctx.run_id[:70] + "-fx-" + uuid.uuid4().hex[:12]
    fixture_ctx = Context(fixture_id, ctx.driver, ctx.evidence_root, ctx.source)
    command = expand_command(spec.get("command"), fixture_ctx, attempt, manifest_file)
    env = os.environ.copy()
    env.update({"OG_LAKE_RUN_ID": fixture_id, "OG_LAKE_RESULT_FILE": str(manifest_file),
                "OG_LAKE_EVIDENCE_DIR": str(attempt)})
    process = execute(command, attempt, spec.get("timeout_seconds", 180), env)
    write_json(attempt / "process.json", process)
    if process["timed_out"] or process["cancelled"]:
        raise ValueError("Fixture generation timed out/cancelled")
    if process["returncode"] == 2:
        raise Blocked("Reference fixture prerequisites are not available")
    if process["returncode"] != 0:
        raise ValueError(f"Reference fixture exited {process['returncode']}")
    manifest = validate_manifest(read_json(manifest_file), fixture_ctx)
    if manifest.get("case") != spec.get("profile"):
        raise ValueError("Generator returned another fixture profile")
    manifest["parent_run_id"] = ctx.run_id
    write_json(manifest_file, manifest)
    return manifest_file, manifest


def main():
    args_parser = parser(__doc__)
    args_parser.add_argument("action", choices=["create", "verify"])
    args_parser.add_argument("--case", required=True)
    args_parser.add_argument("--manifest", type=Path)
    args = args_parser.parse_args()
    try:
        ctx = context_from(args)
        validate_case_id(args.case)
        case = registry(ctx)["cases"].get(args.case)
        if case is None:
            raise Blocked("Unknown case: " + args.case)
        attempt = ctx.attempt("fixtures", args.case)
        result = {"schema_version": 1, "type": "fixture", "id": args.case, "run_id": ctx.run_id,
                  "started_at": now(), "status": "blocked", "provenance": fingerprint(ctx.driver, ctx.source)}
        try:
            if args.action == "verify":
                if not args.manifest:
                    raise Blocked("verify requires --manifest")
                manifest = validate_manifest(read_json(args.manifest), ctx)
                if case.get("fixture", {}).get("profile") not in (None, manifest["case"]):
                    raise ValueError("Fixture profile does not match this case")
                result.update(status="pass", manifest=str(args.manifest.resolve()),
                              assertions=manifest["assertions"])
            else:
                manifest_file, manifest = create_fixture_manifest(case, ctx, attempt)
                result.update(status="pass", manifest=str(manifest_file), assertions=manifest["assertions"])
        except Blocked as exc:
            result.update(status="blocked", reason=str(exc))
        except (ValueError, OSError, KeyError, TypeError) as exc:
            result.update(status="fail", reason=str(exc))
        result.update(finished_at=now(), exit_code=EXIT[result["status"]])
        path = attempt / "result.json"
        write_json(path, result)
        print(json.dumps({"status": result["status"], "reason": result.get("reason", ""), "result": str(path)}, ensure_ascii=False))
        return result["exit_code"]
    except Blocked as exc:
        args_parser.exit(2, f"blocked: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
