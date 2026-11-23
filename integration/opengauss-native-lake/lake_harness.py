"""Fail-closed integration harness for the native lake project (Python stdlib)."""
from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
import uuid

DRIVER = Path(__file__).resolve().parent
WORKSPACE = DRIVER.parents[1]
DEFAULT_EVIDENCE = WORKSPACE / "公共文档/openGauss原生湖存储/验证"
from native_runtime import SOURCE, PROFILE
EXPECTED_BASE = "ba1e4ad2f99295b1b0f65e3880a05cb75a1121d4"
CASE_RE = re.compile(r"^[A-Z][A-Z0-9_-]*(?:/[A-Za-z0-9_-]+)*$")
RUN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$")
STATUSES = {"pass", "fail", "blocked", "skip"}
TASK_STATUSES = {"pending", "in_progress", "blocked", "implemented_unverified", "verified"}
EXIT = {"pass": 0, "fail": 1, "blocked": 2, "skip": 2}


class Blocked(Exception):
    pass


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def hash_json(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def read_json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise Blocked(f"Cannot read JSON {path}: {exc}") from exc


def write_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix="." + path.name, dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(data, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def redact(text):
    text = re.sub(r"(?i)(password|passwd|secret|access_token|token|signature)([=:\s]+)([^\s&,]+)",
                  r"\1\2<redacted>", str(text))
    return re.sub(r"(://)[^/@:\s]+:[^/@\s]+@", r"\1<redacted>@", text)


def capture(argv, cwd=None, timeout=10):
    try:
        proc = subprocess.run(argv, cwd=cwd, capture_output=True, timeout=timeout, check=False)
        return proc.returncode, proc.stdout, proc.stderr
    except (OSError, subprocess.TimeoutExpired) as exc:
        return 127, b"", str(exc).encode()


def source_identity(source=SOURCE):
    source = Path(source)
    rc, head, error = capture(["git", "rev-parse", "HEAD"], source)
    if rc:
        return {"path": str(source), "head": None, "error": redact(error.decode(errors="replace"))}
    rc, diff, error = capture(["git", "diff", "HEAD", "--binary", "--"], source, timeout=30)
    if rc:
        raise Blocked("Cannot fingerprint tracked source changes")
    rc, names, error = capture(["git", "ls-files", "--others", "--exclude-standard", "-z"], source, timeout=30)
    if rc:
        raise Blocked("Cannot enumerate untracked product sources")
    untracked = {}
    extensions = {".c", ".cc", ".cpp", ".cxx", ".h", ".hpp", ".sql", ".control", ".mk",
                  ".json", ".cmake", ".in", ".def", ".sh", ".py", ".txt"}
    for raw in names.split(b"\0"):
        if not raw:
            continue
        name = os.fsdecode(raw)
        path = source / name
        if name.startswith(("src/", "contrib/og_lake/")) and path.is_file():
            if path.suffix in extensions or path.name in {"Makefile", "GNUmakefile", "CMakeLists.txt"}:
                untracked[name] = sha256(path)
    return {"path": str(source), "head": head.decode().strip(),
            "tracked_diff_sha256": hashlib.sha256(diff).hexdigest(),
            "untracked_product_sources": untracked}


def fingerprint(driver=DRIVER, source=SOURCE, binary_paths=()):
    driver = Path(driver)
    lock = driver / "source-lock.json"
    binaries = {}
    for name in binary_paths:
        path = Path(name).resolve()
        binaries[str(path)] = sha256(path) if path.is_file() else None
    implementation = {}
    for name in ("lake_harness.py", "run_case.py", "run_gate.py", "fixture.py"):
        path = driver / name
        implementation[name] = sha256(path) if path.is_file() else None
    return {"source_lock_sha256": sha256(lock) if lock.is_file() else None,
            "source": source_identity(source), "binaries": binaries,
            "harness_files": implementation}


def validate_run_id(run_id):
    if not RUN_RE.fullmatch(run_id):
        raise Blocked("run_id must contain only letters, numbers, dots, underscores and hyphens")


def validate_case_id(case_id):
    if not CASE_RE.fullmatch(case_id):
        raise Blocked("Invalid test or gate identifier")


class Context:
    def __init__(self, run_id, driver=DRIVER, evidence_root=DEFAULT_EVIDENCE, source=SOURCE):
        validate_run_id(run_id)
        self.run_id = run_id
        self.driver = Path(driver).resolve()
        self.evidence_root = Path(evidence_root).resolve()
        self.source = Path(source).resolve()
        self.run_dir = self.evidence_root / run_id

    def attempt(self, kind, identifier):
        validate_case_id(identifier)
        # A slash in a stable subcase ID is encoded, not treated as a path component.
        name = identifier.replace("/", "%2F")
        path = self.run_dir / kind / name / (dt.datetime.now(dt.timezone.utc).strftime("%H%M%S%f") + "-" + uuid.uuid4().hex[:8])
        path.mkdir(parents=True, exist_ok=False)
        return path


def parser(description):
    result = argparse.ArgumentParser(description=description)
    result.add_argument("--run-id", required=True)
    result.add_argument("--evidence-root", type=Path, default=DEFAULT_EVIDENCE)
    result.add_argument("--driver-root", type=Path, default=DRIVER, help=argparse.SUPPRESS)
    result.add_argument("--source", type=Path, default=SOURCE)
    return result


def context_from(args):
    return Context(args.run_id, args.driver_root, args.evidence_root, args.source)


def registry(ctx):
    data = read_json(ctx.driver / "state/cases.json")
    if data.get("schema_version") != 1 or not isinstance(data.get("cases"), dict):
        raise Blocked("Unsupported case registry schema")
    return data


def expand_command(command, ctx, attempt, result_file):
    if not isinstance(command, list) or not command or not all(isinstance(x, str) for x in command):
        raise Blocked("A case requires a nonempty argv array; shell commands are not supported")
    fields = {"python": sys.executable, "driver": str(ctx.driver), "workspace": str(WORKSPACE),
              "source": str(ctx.source), "run_id": ctx.run_id, "evidence": str(attempt),
              "result": str(result_file)}
    if any("{reference_binary}" in arg for arg in command):
        lock = read_json(ctx.driver / "source-lock.json")
        relative = Path(lock.get("reference_build", {}).get("build_subdir", ""))
        if not str(relative) or str(relative) == "." or relative.is_absolute() or ".." in relative.parts:
            raise Blocked("source-lock does not identify a confined reference build directory")
        fields["reference_binary"] = str(WORKSPACE / ".reference/opengauss-native-lake" / relative / "duckdb")
    try:
        return [arg.format_map(fields) for arg in command]
    except (KeyError, ValueError) as exc:
        raise Blocked(f"Invalid command placeholder: {exc}") from exc


def execute(argv, attempt, timeout, env=None, prefix="process"):
    """Run a process group; a timeout/cancel never leaves the direct child alive."""
    if not isinstance(timeout, (int, float)) or timeout <= 0:
        raise Blocked("timeout_seconds must be positive")
    started = time.monotonic()
    process = None
    timed_out = False
    cancelled = False
    stdout = stderr = b""
    try:
        process = subprocess.Popen(argv, cwd=DRIVER, env=env, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, start_new_session=True)
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except (subprocess.TimeoutExpired, KeyboardInterrupt) as exc:
            timed_out = isinstance(exc, subprocess.TimeoutExpired)
            cancelled = isinstance(exc, KeyboardInterrupt)
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGTERM)
            try:
                stdout, stderr = process.communicate(timeout=2)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
                stdout, stderr = process.communicate()
    except OSError as exc:
        stderr = str(exc).encode()
    finally:
        (attempt / (prefix + ".stdout.log")).write_text(redact(stdout.decode(errors="replace")), encoding="utf-8")
        (attempt / (prefix + ".stderr.log")).write_text(redact(stderr.decode(errors="replace")), encoding="utf-8")
    return {"argv": [redact(arg) for arg in argv],
            "returncode": process.returncode if process else 127,
            "timed_out": timed_out, "cancelled": cancelled,
            "duration_seconds": time.monotonic() - started}


def check_payload(payload):
    if not isinstance(payload, dict) or payload.get("status") not in STATUSES:
        raise ValueError("Test must emit a JSON object with status pass/fail/blocked/skip")
    if payload["status"] == "pass":
        assertions = payload.get("assertions")
        if not isinstance(assertions, list) or not assertions:
            raise ValueError("A successful test requires nonempty explicit assertions")
        if any(not isinstance(a, dict) or not a.get("name") or a.get("status") != "pass" for a in assertions):
            raise ValueError("Every assertion in a passing test must be named and pass")
    return payload


def read_test_payload(path):
    # Broken output from an executed test is a failure, not a missing dependency.
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ValueError(f"Malformed test result: {exc}") from exc


def checked_artifacts(items):
    result = []
    for item in items:
        path = Path(item["path"])
        if not path.is_absolute() or not path.is_file():
            raise ValueError(f"Evidence artifact is missing or not absolute: {path}")
        digest = sha256(path)
        if item.get("sha256") and digest != item["sha256"]:
            raise ValueError(f"Evidence artifact hash mismatch: {path}")
        result.append({"path": str(path), "sha256": digest})
    return result


def script_hashes(argv):
    return {str(Path(arg).resolve()): sha256(arg) for arg in argv[1:]
            if Path(arg).is_file() and Path(arg).suffix in {".py", ".sh", ".sql"}}


def verify_result(path, ctx, require_pass=True):
    path = Path(path).resolve()
    result = read_json(path)
    if result.get("schema_version") != 1 or result.get("type") not in {"case", "gate"}:
        raise Blocked(f"Not a harness result: {path}")
    if require_pass and result.get("status") != "pass":
        raise Blocked(f"Evidence is not passing: {path}")
    if result.get("status") == "pass":
        check_payload(result)
    previous = result.get("provenance", {})
    current = fingerprint(ctx.driver, ctx.source, previous.get("binaries", {}).keys())
    if previous != current:
        raise Blocked(f"Evidence is stale (source/lock/binary/harness changed): {path}")
    for filename, expected in result.get("script_hashes", {}).items():
        if not Path(filename).is_file() or sha256(filename) != expected:
            raise Blocked(f"Evidence script changed: {filename}")
    for artifact in result.get("artifacts", []):
        path = Path(artifact["path"])
        if not path.is_file() or sha256(path) != artifact.get("sha256"):
            raise Blocked(f"Evidence artifact is missing or changed: {path}")
    if result["type"] == "case":
        case = registry(ctx)["cases"].get(result.get("id"))
        if case is None or result.get("contract_sha256") != hash_json(case):
            raise Blocked(f"Evidence case contract changed: {result.get('id')}")
    else:
        gates = read_json(ctx.driver / "state/gates.json").get("gates", {})
        if hash_json(gates.get(result.get("id"))) != result.get("contract_sha256"):
            raise Blocked("Evidence gate contract changed")
        for child in result.get("children", []):
            child_path = Path(child["path"])
            if not child_path.is_file() or sha256(child_path) != child["sha256"]:
                raise Blocked("A gate child result is missing or changed")
            verify_result(child_path, ctx, require_pass=True)
    return result


def check_dependencies(required, ctx):
    if not required:
        return
    deps = read_json(ctx.driver / "state/dependencies.json").get("dependencies", {})
    for dep_id in required:
        entry = deps.get(dep_id)
        if not entry or entry.get("status") != "verified" or not entry.get("evidence"):
            reason = entry.get("reason", "not registered") if entry else "not registered"
            raise Blocked(f"{dep_id} is not verified: {reason}")
        check_lock_fields(entry, ctx)
        for evidence in entry["evidence"]:
            path = Path(evidence["path"])
            if not path.is_file() or sha256(path) != evidence.get("sha256"):
                raise Blocked(f"{dep_id} evidence is missing/changed")
            verify_result(path, ctx)


def check_lock_fields(entry, ctx):
    requirements = entry.get("required_lock_fields", [])
    if not requirements:
        return
    lock = read_json(ctx.driver / "source-lock.json")
    for field in requirements:
        value = lock
        for component in field["path"].split("."):
            value = value.get(component) if isinstance(value, dict) else None
        if value is None or value == "" or value == {}:
            raise Blocked("Source lock field is not supplied: " + field["path"])
        if field.get("kind") == "commit" and not re.fullmatch(r"[0-9a-f]{40}", str(value)):
            raise Blocked("Source lock requires an exact commit: " + field["path"])


def check_runtime(spec, ctx):
    if not spec:
        return None
    pidfile = Path(spec["pidfile"].format(workspace=WORKSPACE, source=ctx.source))
    binary = Path(spec["binary"].format(workspace=WORKSPACE, source=ctx.source)).resolve()
    try:
        pid = int(pidfile.read_text().splitlines()[0])
        executable = Path(f"/proc/{pid}/exe")
        actual = sha256(executable)
        if executable.resolve() != binary or actual != sha256(binary):
            raise Blocked("Running database executable differs from the project installation")
        return {"pid": pid, "pidfile": str(pidfile), "executable": str(binary), "sha256": actual}
    except (OSError, ValueError, IndexError) as exc:
        raise Blocked(f"Project database is not running with a valid pidfile: {exc}") from exc


def runtime_matches(before, after, allow_restart=False):
    """Only registered lifecycle tests may change PID, never executable/path/hash."""
    if before == after:
        return True
    if not allow_restart or not before or not after:
        return False
    return ({key: value for key, value in before.items() if key != "pid"} ==
            {key: value for key, value in after.items() if key != "pid"})


def run_case(case_id, ctx):
    validate_case_id(case_id)
    attempt = ctx.attempt("cases", case_id)
    result = {"schema_version": 1, "type": "case", "id": case_id, "run_id": ctx.run_id,
              "started_at": now(), "status": "blocked", "assertions": [], "reason": "",
              "provenance": fingerprint(ctx.driver, ctx.source), "script_hashes": {}}
    case = None
    env = None
    execution_started = False
    binaries = []
    payload_file = attempt / "test-result.json"
    try:
        if PROFILE != 'debug':
            raise Blocked('Registered case harness is Debug-only; use explicitly profile-aware native entry points for Release')
        case = registry(ctx)["cases"].get(case_id)
        if case is None:
            raise Blocked(f"Unknown case: {case_id}")
        result["contract_sha256"] = hash_json(case)
        if case.get("implementation") != "implemented":
            raise Blocked(case.get("reason", "Case has no implemented executable test"))
        if case.get("children"):
            raise Blocked("Composite case needs a dedicated assertion driver; subcases do not imply parent pass")
        check_dependencies(case.get("requires", []), ctx)
        binary_specs = case.get("required_binaries", [])
        binaries = [str(Path(p).resolve()) for p in
                    (expand_command(binary_specs, ctx, attempt, payload_file) if binary_specs else [])]
        result["provenance"] = fingerprint(ctx.driver, ctx.source, binaries)
        if case.get("source_lock_required", True) and not result["provenance"]["source_lock_sha256"]:
            raise Blocked("source-lock.json is missing")
        for binary, digest in result["provenance"]["binaries"].items():
            if not digest:
                raise Blocked(f"Required binary missing: {binary}")
        result["runtime"] = check_runtime(case.get("runtime"), ctx)
        argv = expand_command(case.get("command"), ctx, attempt, payload_file)
        result["script_hashes"] = script_hashes(argv)
        if case.get("cleanup_command"):
            result["script_hashes"].update(script_hashes(
                expand_command(case["cleanup_command"], ctx, attempt, payload_file)))
        for name in case.get("evidence_files", []):
            path = Path(name.format(driver=ctx.driver, workspace=WORKSPACE, source=ctx.source)).resolve()
            if not path.is_file():
                raise Blocked(f"Required evidence input is missing: {path}")
            result["script_hashes"][str(path)] = sha256(path)
        env = os.environ.copy()
        env.update({"OG_LAKE_RUN_ID": ctx.run_id, "OG_LAKE_RESULT_FILE": str(payload_file),
                    "OG_LAKE_EVIDENCE_DIR": str(attempt), "OG_LAKE_SOURCE": str(ctx.source),
                    "OG_LAKE_DRIVER_ROOT": str(ctx.driver)})
        if case.get("fixture"):
            from fixture import create_fixture_manifest
            fixture_file, manifest = create_fixture_manifest(case, ctx, attempt / "fixture")
            env["OG_LAKE_FIXTURE_MANIFEST"] = str(fixture_file)
            result["fixture_manifest"] = str(fixture_file)
            result["artifacts"] = checked_artifacts([{"path": str(fixture_file)}])
        execution_started = True
        process = execute(argv, attempt, case.get("timeout_seconds", 120), env)
        result["process"] = process
        if process["timed_out"] or process["cancelled"]:
            result.update(status="fail", reason="Test timed out or was cancelled; process group terminated")
        elif process["returncode"] == 2:
            result.update(status="blocked", reason="Test reported an unavailable prerequisite")
            if payload_file.is_file():
                result["test_result"] = read_test_payload(payload_file)
        elif process["returncode"] != 0:
            result.update(status="fail", reason=f"Test exited {process['returncode']}")
            if payload_file.is_file():
                result["test_result"] = read_test_payload(payload_file)
        elif not payload_file.is_file():
            result.update(status="fail", reason="Process exited zero without structured assertions")
        else:
            payload = check_payload(read_test_payload(payload_file))
            result.update(status=payload["status"], assertions=payload.get("assertions", []),
                          reason=payload.get("reason", ""), test_result=payload)
            result.setdefault("artifacts", []).extend(checked_artifacts(payload.get("artifacts", [])))
    except Blocked as exc:
        # A missing prerequisite before execution is blocked. An inspection or
        # evidence error after starting a test must never downgrade a failure.
        result.update(status="fail" if execution_started else "blocked", reason=str(exc))
    except (ValueError, TypeError, KeyError, OSError) as exc:
        result.update(status="fail", reason=f"Invalid test execution/result: {exc}")
    finally:
        if execution_started:
            result["original_test_status"] = result["status"]
            result["original_test_reason"] = result.get("reason", "")
            result["original_test_exit_code"] = result.get("process", {}).get("returncode")
            result["original_test_result"] = result.get("test_result")
            try:
                result["provenance_after"] = fingerprint(ctx.driver, ctx.source, binaries)
                if result["provenance"]["binaries"] != result["provenance_after"]["binaries"]:
                    result.update(status="fail", reason="A required binary changed or disappeared during test execution")
                elif result["provenance"] != result["provenance_after"]:
                    result["provenance_error"] = "Source/lock/harness changed while test was running"
                    if result["status"] != "fail":
                        result.update(status="blocked", reason=result["provenance_error"])
            except (Blocked, OSError, ValueError) as exc:
                result["provenance_error"] = str(exc)
                result.update(status="fail", reason=f"Cannot inspect implementation after execution: {exc}")
            try:
                result["runtime_after"] = check_runtime(case.get("runtime"), ctx)
                if not runtime_matches(result.get("runtime"), result["runtime_after"],
                                       case.get("allow_runtime_restart", False)):
                    result.update(status="fail", reason="Running database changed unexpectedly during test execution")
            except (Blocked, OSError, ValueError) as exc:
                result["runtime_after"] = {"status": "unavailable", "error": str(exc)}
                result.update(status="fail", reason=f"Database lost or invalid after test execution: {exc}")
        # Even a malformed oracle response cannot bypass cleanup.
        if case and env is not None and case.get("cleanup_command"):
            try:
                cleanup_result = execute(expand_command(case["cleanup_command"], ctx, attempt, payload_file),
                                         attempt, case.get("cleanup_timeout_seconds", 30), env, "cleanup")
                result["cleanup"] = cleanup_result
                if cleanup_result["returncode"] != 0 or cleanup_result["timed_out"]:
                    result.update(status="fail", reason="Cleanup failed; inspect evidence before continuing")
            except (Blocked, OSError, ValueError) as exc:
                result.update(status="fail", reason=f"Cleanup could not execute: {exc}")
    result.setdefault("artifacts", []).extend(
        {"path": str(path), "sha256": sha256(path)}
        for path in sorted(attempt.iterdir()) if path.is_file()
    )
    if result["status"] == "pass":
        for filename, expected in result.get("script_hashes", {}).items():
            if not Path(filename).is_file() or sha256(filename) != expected:
                result.update(status="blocked", reason="A test script/input changed during execution")
                break
    result.update(finished_at=now(), exit_code=EXIT[result["status"]])
    path = attempt / "result.json"
    write_json(path, result)
    return result, path


def run_gate(gate_id, ctx, stack=()):
    validate_case_id(gate_id)
    attempt = ctx.attempt("gates", gate_id)
    result = {"schema_version": 1, "type": "gate", "id": gate_id, "run_id": ctx.run_id,
              "started_at": now(), "status": "blocked", "assertions": [], "children": [],
              "provenance": fingerprint(ctx.driver, ctx.source)}
    try:
        gates = read_json(ctx.driver / "state/gates.json").get("gates", {})
        gate = gates.get(gate_id)
        if gate is None:
            raise Blocked(f"Unknown gate: {gate_id}")
        if gate_id in stack:
            raise Blocked("Cyclic gate dependency")
        result["contract_sha256"] = hash_json(gate)
        if not gate.get("cases") and not gate.get("gates"):
            raise Blocked("Empty gate is not a completed capability")
        check_dependencies(gate.get("requires", []), ctx)
        for child_gate in gate.get("gates", []):
            child, path = run_gate(child_gate, ctx, (*stack, gate_id))
            result["children"].append({"id": child_gate, "path": str(path), "sha256": sha256(path), "status": child["status"]})
        for case_id in gate.get("cases", []):
            child, path = run_case(case_id, ctx)
            result["children"].append({"id": case_id, "path": str(path), "sha256": sha256(path), "status": child["status"]})
        states = [child["status"] for child in result["children"]]
        result["status"] = "fail" if "fail" in states else "blocked" if any(s != "pass" for s in states) else "pass"
        result["assertions"] = [{"name": child["id"], "status": child["status"]} for child in result["children"]]
        if result["provenance"] != fingerprint(ctx.driver, ctx.source):
            result["provenance_error"] = "Source/lock/harness changed during gate execution"
            if result["status"] != "fail":
                result.update(status="blocked", reason=result["provenance_error"])
    except Blocked as exc:
        if result["status"] != "fail":
            result.update(status="blocked", reason=str(exc))
        else:
            result["gate_error"] = str(exc)
    except (OSError, ValueError, TypeError, KeyError) as exc:
        result.update(status="fail", reason=str(exc))
    result.update(finished_at=now(), exit_code=EXIT[result["status"]])
    path = attempt / "result.json"
    write_json(path, result)
    return result, path


class Barrier:
    """Local test coordinator only; kernel pause hooks must be wired separately."""
    def __init__(self):
        self._condition = threading.Condition()
        self._arrived = set()
        self._released = set()
        self._cancelled = False

    def arrive(self, name, timeout):
        deadline = time.monotonic() + timeout
        with self._condition:
            self._arrived.add(name)
            self._condition.notify_all()
            while name not in self._released and not self._cancelled:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"Barrier {name}: release timeout")
                self._condition.wait(remaining)
            if self._cancelled:
                raise RuntimeError("Barrier cancelled")

    def wait_arrived(self, name, timeout):
        deadline = time.monotonic() + timeout
        with self._condition:
            while name not in self._arrived and not self._cancelled:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"Barrier {name}: arrival timeout")
                self._condition.wait(remaining)
            if self._cancelled:
                raise RuntimeError("Barrier cancelled")

    def release(self, name):
        with self._condition:
            if name not in self._arrived:
                raise ValueError("Cannot release a barrier before its arrival")
            self._released.add(name)
            self._condition.notify_all()

    def cancel(self):
        with self._condition:
            self._cancelled = True
            self._condition.notify_all()


def print_result(result, path):
    print(json.dumps({"id": result.get("id"), "status": result["status"],
                      "reason": result.get("reason", ""), "result": str(path)}, ensure_ascii=False))
    return result["exit_code"]
