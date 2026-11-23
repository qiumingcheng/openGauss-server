"""Schedule the published native case/gate registry without changing its contracts."""

import json
import subprocess
from pathlib import Path
from uuid import uuid4

CASES = ("registry_harness", "registry", "registry_gates")

CONTRACT_FILES = (
    "state/cases.json",
    "state/gates.json",
    "run_case.py",
    "run_gate.py",
    "lake_harness.py",
    "fixture.py",
)

BOOT_CONFIG = {
    "boot_build_evidence": "OG_LAKE_BOOT_BUILD_EVIDENCE",
    "boot_full_build_evidence": "OG_LAKE_BOOT_FULL_BUILD_EVIDENCE",
    "boot_bootstrap_evidence": "OG_LAKE_BOOT_BOOTSTRAP_EVIDENCE",
}


def load_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def check_owned_debug(ctx):
    """Legacy cases open the workspace Debug socket and some restart it."""
    workspace = Path(ctx.workspace).resolve()
    source = Path(ctx.source).resolve()
    if source != (workspace / ".worktrees/opengauss-native-lake").resolve():
        raise RuntimeError("registry requires the canonical workspace Debug source")
    runtime = workspace / ".runtime/opengauss-native-lake/debug"
    install = workspace / ".install/opengauss-native-lake-debug"
    expected = {
        "project": "opengauss-native-lake",
        "source": str(source),
        "data": str(runtime / "data"),
        "install": str(install),
        "profile": "debug",
        "port": 55560,
    }
    owner = load_json(runtime / "owner.json")
    if owner != expected:
        raise RuntimeError("registry requires the matching owned workspace Debug runtime")
    try:
        pid = int((runtime / "data/postmaster.pid").read_text().splitlines()[0])
        executable = Path(f"/proc/{pid}/exe").resolve(strict=True)
    except (OSError, ValueError, IndexError) as exc:
        raise RuntimeError("start the owned Debug runtime before running the registry") from exc
    if executable != (install / "bin/gaussdb").resolve(strict=True):
        raise RuntimeError("registry Debug executable is outside the owned installation")


def registry_inputs(ctx):
    driver = Path(ctx.driver).resolve()
    workspace = Path(ctx.workspace).resolve()
    if driver != workspace / "integration/opengauss-native-lake":
        raise RuntimeError("registry requires a real workspace execution mirror at integration/opengauss-native-lake")
    for name in CONTRACT_FILES:
        if (driver / name).read_bytes() != (Path(ctx.repo_driver) / name).read_bytes():
            raise RuntimeError(f"workspace execution mirror is stale: {name}")
    cases = load_json(driver / "state/cases.json")
    gates = load_json(driver / "state/gates.json")
    if (cases.get("schema_version") != 1 or gates.get("schema_version") != 1 or
            not isinstance(cases.get("cases"), dict) or not isinstance(gates.get("gates"), dict)):
        raise RuntimeError("unsupported native lake registry or gate schema")
    return driver, cases["cases"], gates["gates"]


def registry_env(ctx, *, need_boot):
    env = dict(ctx.env)
    env.pop("PYTHONOPTIMIZE", None)
    workspace = Path(ctx.workspace)
    env.update(
        OG_LAKE_TEST_PROFILE="debug",
        OG_LAKE_SOURCE=str(ctx.source),
        OG_LAKE_TEST_PORT="55560",
        PGPORT="55560",
        PGHOST=str(workspace / ".runtime/opengauss-native-lake/debug/socket"),
    )
    # Older native cases use USER directly; keep it aligned with the owned
    # Debug bootstrap user selected by the FastCheck job.
    if env.get("PGUSER"):
        env["USER"] = env["PGUSER"]
    options = ctx.config.get("registry", {})
    if not isinstance(options, dict) or options.keys() - BOOT_CONFIG.keys():
        raise RuntimeError("unknown registry configuration key")
    for key, variable in BOOT_CONFIG.items():
        if need_boot and key in options:
            path = Path(ctx.path(options[key]))
            if not path.is_file():
                raise RuntimeError(f"missing registry {key} file: {path}")
            env[variable] = str(path)
    return env


def run_entry(ctx, driver, evidence, kind, identifier, env, *, allow_restart):
    run_id = f"fastcheck-registry-{uuid4().hex[:16]}"
    safe_id = identifier.replace("/", "%2F")
    script = f"run_{kind}.py"
    label = f"{kind}-{identifier.replace('/', '.')}"
    selector = "--id" if kind == "case" else "--gate"
    argv = [ctx.python, str(driver / script), selector, identifier,
            "--source", str(ctx.source), "--run-id", run_id, "--evidence-root", str(evidence)]
    process_error = None
    try:
        # The native harness bounds fixture, test and cleanup execution.
        ctx.run(argv, label, cwd=driver, env=env)
    except (subprocess.SubprocessError, OSError, RuntimeError) as exc:
        process_error = str(exc)

    records = list((evidence / run_id / f"{kind}s" / safe_id).glob("*/result.json"))
    report = {"id": identifier, "kind": kind, "status": "fail"}
    if process_error:
        report["process_error"] = process_error
    if len(records) != 1:
        report["reason"] = f"expected one fresh native receipt, found {len(records)}"
        return report, True

    report["result"] = str(records[0])
    try:
        native = load_json(records[0])
        if (native["schema_version"] != 1 or native["type"] != kind or
                native["id"] != identifier or native["run_id"] != run_id):
            raise RuntimeError("native receipt identity does not match this scheduled invocation")
        status = native["status"]
        if status not in {"pass", "fail", "blocked", "skip"}:
            raise RuntimeError(f"unknown native receipt status: {status}")
    except (OSError, ValueError, KeyError, TypeError, RuntimeError) as exc:
        report["reason"] = str(exc)
        return report, True

    report.update(status=status, reason=native.get("reason", ""),
                  native_exit_code=native.get("exit_code"))
    if status == "pass":
        assertions = native.get("assertions")
        if (process_error or native.get("exit_code") != 0 or not assertions or
                not isinstance(assertions, list) or
                any(not isinstance(a, dict) or not a.get("name") or a.get("status") != "pass"
                    for a in assertions)):
            report.update(status="fail", reason="native pass receipt was incomplete or process failed")
    cleanup = native.get("cleanup") or {}
    unsafe = bool(cleanup and (cleanup.get("returncode") != 0 or
                               cleanup.get("timed_out") or cleanup.get("cancelled")))
    before = native.get("runtime") or {}
    after = native.get("runtime_after") or {}
    if before and "original_test_status" in native:
        fields = before.keys() - ({"pid"} if allow_restart else set())
        unsafe |= not after or any(before[k] != after.get(k) for k in fields)
    unsafe |= bool(native.get("provenance_error"))
    return report, unsafe


def run(case, ctx):
    if case not in CASES:
        raise RuntimeError(f"unknown registry FastCheck case: {case}")
    driver, registry, gates = registry_inputs(ctx)
    if case != "registry_harness":
        check_owned_debug(ctx)
    implemented = [case_id for case_id, spec in sorted(registry.items())
                   if spec.get("implementation") == "implemented"]
    unavailable = [{"id": case_id, "reason": spec.get("reason", "")}
                   for case_id, spec in sorted(registry.items())
                   if spec.get("implementation") != "implemented"]
    if case == "registry":
        kind, ids = "case", implemented
    elif case == "registry_harness":
        kind, ids = "gate", ["GH"]
        if "GH" not in gates:
            raise RuntimeError("native GH gate is missing")
    else:
        kind, ids = "gate", sorted(gates)
    if not ids:
        raise RuntimeError(f"empty native registry selection for {case}")

    env = registry_env(ctx, need_boot=(case != "registry_harness"))
    evidence = Path(ctx.out) / "native-registry"
    evidence.mkdir(exist_ok=False)
    reports = []
    summary = {"case": case, "status": "fail", "implemented_cases": len(implemented),
               "unimplemented_cases": unavailable, "reports": reports}
    summary_path = evidence / "summary.json"
    try:
        for index, identifier in enumerate(ids):
            allow_restart = kind == "case" and registry[identifier].get("allow_runtime_restart", False)
            report, unsafe = run_entry(ctx, driver, evidence, kind, identifier, env,
                                       allow_restart=allow_restart)
            reports.append(report)
            if unsafe:
                summary["stopped_reason"] = "native cleanup or runtime/source identity failed"
                summary["not_run"] = ids[index + 1:]
                break
        if len(reports) == len(ids) and all(r["status"] == "pass" for r in reports):
            if "stopped_reason" not in summary:
                summary["status"] = "pass"
    finally:
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    if summary["status"] != "pass":
        passed = sum(r["status"] == "pass" for r in reports)
        raise RuntimeError(f"native registry {case}: {passed}/{len(ids)} passed; see {summary_path}")
    return summary
