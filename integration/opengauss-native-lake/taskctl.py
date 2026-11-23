#!/usr/bin/env python3
"""Update engineering state; verified requires current successful harness evidence."""
import argparse
import fcntl
import json
from pathlib import Path

from lake_harness import (Blocked, Context, DRIVER, SOURCE, TASK_STATUSES, check_lock_fields, now, read_json,
                          sha256, verify_result, write_json)


def covered_ids(result):
    ids = {result["id"]}
    for child in result.get("children", []):
        ids.update(covered_ids(read_json(child["path"])))
    return ids


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--driver-root", type=Path, default=DRIVER)
    parser.add_argument("--source", type=Path, default=SOURCE)
    parser.add_argument("--dependency", action="store_true")
    parser.add_argument("action", choices=["list", "show", "set", "verify", "refresh"])
    parser.add_argument("id", nargs="?")
    parser.add_argument("--status", choices=sorted(TASK_STATUSES - {"verified"}))
    parser.add_argument("--reason")
    parser.add_argument("--next-action")
    parser.add_argument("--result", action="append", default=[], type=Path)
    args = parser.parse_args()
    state_path = args.driver_root / "state" / ("dependencies.json" if args.dependency else "tasks.json")
    key = "dependencies" if args.dependency else "tasks"
    try:
        if not state_path.is_file():
            raise Blocked("State file has not been initialized")
        with (state_path.parent / ".state.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            data = read_json(state_path)
            entries = data.get(key, {})
            if args.action == "refresh":
                ctx = Context("state-verification", driver=args.driver_root, source=args.source)
                invalidated = []
                for identifier, previous in entries.items():
                    if previous.get("status") != "verified":
                        continue
                    try:
                        if not previous.get("evidence"):
                            raise Blocked("No verification evidence")
                        check_lock_fields(previous, ctx)
                        for item in previous["evidence"]:
                            path = Path(item["path"])
                            if not path.is_file() or sha256(path) != item.get("sha256"):
                                raise Blocked("Result file missing or changed")
                            verify_result(path, ctx)
                        for item in previous.get("artifacts", []):
                            path = Path(item["path"])
                            if not path.is_file() or sha256(path) != item.get("sha256"):
                                raise Blocked("Deliverable missing or changed")
                    except (Blocked, ValueError, KeyError, OSError) as exc:
                        previous.update(status="implemented_unverified", reason=str(exc), updated_at=now(),
                                        next_action="Rerun the affected acceptance tests and verify fresh evidence.")
                        invalidated.append(identifier)
                data["updated_at"] = now()
                write_json(state_path, data)
                print(json.dumps({"invalidated": invalidated}, ensure_ascii=False))
                return 2 if invalidated else 0
            if args.action == "list":
                print(json.dumps(entries, ensure_ascii=False, indent=2))
                return 0
            entry = entries.get(args.id)
            if entry is None:
                raise Blocked("Unknown task/dependency: " + str(args.id))
            if args.action == "show":
                print(json.dumps(entry, ensure_ascii=False, indent=2))
                return 0
            if args.action == "set":
                if not args.status:
                    raise Blocked("set requires --status; verified is available only through verify")
                if args.status == "blocked" and not args.reason:
                    raise Blocked("blocked requires a concrete --reason")
                entry["status"] = args.status
                if args.reason:
                    entry["reason"] = args.reason
                if args.next_action:
                    entry["next_action"] = args.next_action
            else:
                if not args.result:
                    raise Blocked("verify requires --result containing actual executed assertions")
                ctx = Context("state-verification", driver=args.driver_root, source=args.source)
                check_lock_fields(entry, ctx)
                results = [verify_result(path, ctx) for path in args.result]
                for prerequisite in entry.get("depends_on", []):
                    previous = entries.get(prerequisite)
                    if not previous or previous.get("status") != "verified" or not previous.get("evidence"):
                        raise Blocked(f"Task prerequisite is not verified: {prerequisite}")
                    for item in previous.get("evidence", []):
                        path = Path(item["path"])
                        if not path.is_file() or sha256(path) != item.get("sha256"):
                            raise Blocked(f"Task prerequisite evidence changed: {prerequisite}")
                        verify_result(path, ctx)
                required = set(entry.get("test_ids", []))
                covered = set().union(*(covered_ids(result) for result in results))
                if not required or required - covered:
                    raise Blocked("Missing required test evidence: " + ", ".join(sorted(required - covered))
                                  if required else "Task has no frozen test requirements")
                artifacts = []
                for value in entry.get("deliverables", []):
                    path = Path(value)
                    if not path.is_absolute():
                        path = args.driver_root / path
                    if not path.is_file():
                        raise Blocked(f"Required deliverable missing: {path}")
                    artifacts.append({"path": str(path.resolve()), "sha256": sha256(path)})
                entry.update(status="verified", evidence=[{"path": str(path.resolve()), "sha256": sha256(path)}
                                                          for path in args.result], artifacts=artifacts,
                             reason="Required executed evidence and deliverables validated")
            entry["updated_at"] = now()
            data["updated_at"] = now()
            write_json(state_path, data)
            print(json.dumps(entry, ensure_ascii=False, indent=2))
            return 0
    except (Blocked, ValueError, TypeError, KeyError, OSError) as exc:
        parser.exit(2, f"blocked: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
