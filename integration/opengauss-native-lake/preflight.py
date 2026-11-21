#!/usr/bin/env python3
"""Read-only host/environment inventory; it does not build or start a server."""
import argparse
import json
from pathlib import Path
import shutil
import socket

from lake_harness import (Blocked, Context, DEFAULT_EVIDENCE, DRIVER, EXPECTED_BASE, SOURCE,
                          WORKSPACE, capture, check_dependencies, fingerprint, now, read_json,
                          redact, sha256, write_json)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", action="store_true", required=True)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--scope", choices=["environment", "product"], default="environment")
    parser.add_argument("--evidence-root", type=Path, default=DEFAULT_EVIDENCE)
    args = parser.parse_args()
    run_id = args.run_id or "preflight-" + now().replace(":", "").replace(".", "").replace("+", "-")
    try:
        ctx = Context(run_id, evidence_root=args.evidence_root)
        report = {"schema_version": 1, "type": "preflight", "id": "PREFLIGHT",
                  "run_id": run_id, "started_at": now(), "scope": args.scope,
                  "status": "pass", "assertions": [], "blockers": [],
                  "notice": "Environment readiness is not a product gate or BOOT-01 pass."}
        for tool in ["python3", "git", "make", "bash"]:
            found = shutil.which(tool)
            report["assertions"].append({"name": "tool:" + tool, "status": "pass" if found else "blocked", "path": found})
            if not found:
                report["blockers"].append("Missing tool: " + tool)
        report["provenance"] = fingerprint()
        git = report["provenance"]["source"]
        if not git.get("head"):
            report["blockers"].append("Project source worktree is not available")
        else:
            rc, branch, error = capture(["git", "branch", "--show-current"], SOURCE)
            report["source_branch"] = branch.decode().strip()
            if rc or report["source_branch"] != "feature/native-lake":
                report["blockers"].append("Worktree is not on feature/native-lake")
            rc, output, error = capture(["git", "merge-base", "--is-ancestor", EXPECTED_BASE, "HEAD"], SOURCE)
            if rc:
                report["blockers"].append("Expected OG baseline is not an ancestor of project HEAD")
            rc, output, error = capture(["git", "status", "--short"], SOURCE, timeout=30)
            report["source_dirty"] = output.decode(errors="replace").splitlines()
        lock = DRIVER / "source-lock.json"
        if lock.is_file():
            try:
                report["source_lock"] = {"path": str(lock), "sha256": sha256(lock),
                                        "target_format": read_json(lock).get("target_format")}
            except Blocked as exc:
                report["blockers"].append(str(exc))
        else:
            report["blockers"].append("source-lock.json has not been delivered")
        deps_file = DRIVER / "state/dependencies.json"
        report["dependencies"] = read_json(deps_file).get("dependencies", {}) if deps_file.is_file() else {}
        if args.scope == "product":
            try:
                if not report["dependencies"]:
                    raise Blocked("No dependencies registered")
                check_dependencies(list(report["dependencies"]), ctx)
            except Blocked as exc:
                report["blockers"].append(str(exc))
        report["paths"] = {}
        for name in ["debug", "release"]:
            install = WORKSPACE / (".install/opengauss-native-lake-" + name)
            runtime = WORKSPACE / ".runtime/opengauss-native-lake" / name
            report["paths"][name] = {
                "install": str(install), "data": str(runtime / "data"), "socket": str(runtime / "socket"),
                "initialized": (runtime / "data/PG_VERSION").is_file(),
                "gaussdb": str(install / "bin/gaussdb"),
                "gaussdb_sha256": sha256(install / "bin/gaussdb") if (install / "bin/gaussdb").is_file() else None}
        report["entrypoints"] = {name: (DRIVER / name).is_file()
                                 for name in ["build.sh", "reference.sh", "fixture.py", "run_case.py", "run_gate.py"]}
        report["ports"] = []
        for port in (55560, 55561, 55562):
            probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            probe.settimeout(0.2)
            try:
                listening = probe.connect_ex(("127.0.0.1", port)) == 0
            finally:
                probe.close()
            report["ports"].append({"port": port, "listening": listening})
        report["ports_note"] = "55560/61/62 are the registered Debug ports; listener ownership must be checked before starting."
        rc, output, error = capture(["ps", "-eo", "pid,comm,args"])
        report["database_processes"] = [redact(line) for line in output.decode(errors="replace").splitlines()
                                        if any(name in line for name in ("gaussdb", "postgres", "duckdb"))]
        rc, output, error = capture(["podman", "ps", "--format", "{{.Names}} {{.Status}}"])
        report["containers"] = {"returncode": rc, "output": redact(output.decode(errors="replace")),
                                "error": redact(error.decode(errors="replace"))}
        usage = shutil.disk_usage(WORKSPACE)
        report["disk_bytes"] = dict(zip(("total", "used", "free"), usage))
        report["status"] = "blocked" if report["blockers"] else "pass"
        report["finished_at"] = now()
        path = ctx.run_dir / "preflight" / ("report-" + report["finished_at"].replace(":", "").replace("/", "_") + ".json")
        write_json(path, report)
        print(json.dumps({"status": report["status"], "scope": args.scope, "blockers": report["blockers"],
                          "report": str(path)}, ensure_ascii=False))
        return 2 if report["blockers"] else 0
    except (Blocked, OSError, ValueError) as exc:
        print(json.dumps({"status": "blocked", "reason": str(exc)}, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
