#!/usr/bin/env python3
"""REF-01: run real reference fixtures, retaining reference-only scope."""
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fixture import validate_manifest
from lake_harness import (Blocked, Context, DRIVER, execute, read_json, sha256, write_json)


def main():
    output = Path(os.environ["OG_LAKE_RESULT_FILE"])
    evidence = Path(os.environ["OG_LAKE_EVIDENCE_DIR"])
    run_id = os.environ["OG_LAKE_RUN_ID"]
    ctx = Context(run_id, driver=Path(os.environ.get("OG_LAKE_DRIVER_ROOT", DRIVER)),
                  source=Path(os.environ["OG_LAKE_SOURCE"]))
    result = {"status": "fail", "scope": "reference_only", "assertions": [], "artifacts": []}
    try:
        for profile in ("F", "I", "N", "HISTORY"):
            directory = evidence / profile
            directory.mkdir()
            manifest_file = directory / "manifest.json"
            process = execute(["bash", str(ctx.driver / "reference.sh"), "fixture", "--case", profile,
                               "--run-id", run_id, "--output", str(manifest_file)],
                              directory, 180, os.environ.copy())
            if process["returncode"] == 2:
                raise Blocked("Reference binary/receipt has not been built or is stale")
            if process["returncode"] or process["timed_out"]:
                raise ValueError("Reference fixture failed: " + profile)
            manifest = validate_manifest(read_json(manifest_file), ctx)
            result["assertions"].extend({"name": profile + "/" + item["name"], "status": item["status"]}
                                        for item in manifest["assertions"])
            result["artifacts"].append({"path": str(manifest_file), "sha256": sha256(manifest_file)})
        result["status"] = "pass"
    except Blocked as exc:
        result.update(status="blocked", reason=str(exc))
    except (OSError, ValueError, KeyError) as exc:
        result.update(status="fail", reason=str(exc))
    write_json(output, result)
    return 0 if result["status"] == "pass" else 2 if result["status"] == "blocked" else 1


if __name__ == "__main__":
    raise SystemExit(main())
