#!/usr/bin/env python3
"""RUN-01: adversarial driver checks in temporary directories, with no database."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lake_harness import (Barrier, Blocked, Context, fingerprint, read_json, run_case, run_gate,
                          runtime_matches, sha256, source_identity, verify_result, write_json)


class HarnessTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="og-lake-run01-")
        self.base = Path(self.temporary.name)
        self.driver = self.base / "driver"
        (self.driver / "state").mkdir(parents=True)
        self.source = self.base / "source"
        self.source.mkdir()
        self.ctx = Context("run01", self.driver, self.base / "evidence", self.source)
        write_json(self.driver / "source-lock.json", {"target_format": "1.1"})
        write_json(self.driver / "state/dependencies.json", {
            "dependencies": {"DEP-TEST": {"status": "blocked", "reason": "deliberately missing"}}})
        self.plugin = self.driver / "plugin.py"
        self.plugin.write_text(
            "import json, os, pathlib, subprocess, sys, time\n"
            "mode=sys.argv[1]\n"
            "out=pathlib.Path(os.environ['OG_LAKE_RESULT_FILE'])\n"
            "if mode=='timeout':\n"
            " child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'])\n"
            " pathlib.Path(os.environ['OG_LAKE_EVIDENCE_DIR'],'child.pid').write_text(str(child.pid))\n"
            " time.sleep(60)\n"
            "if mode=='empty': sys.exit(0)\n"
            "if mode=='invalid': out.write_text('{bad'); sys.exit(0)\n"
            "if mode.startswith('kill_runtime_'):\n"
            " import signal\n"
            " os.kill(int(sys.argv[2]),signal.SIGTERM)\n"
            " for unused in range(100):\n"
            "  if not pathlib.Path('/proc',sys.argv[2],'exe').exists(): break\n"
            "  time.sleep(0.001)\n"
            "if mode=='change_binary': pathlib.Path(sys.argv[2]).write_bytes(b'changed')\n"
            "payload={'status':'pass','assertions':[{'name':'fixture assertion','status':'pass'}]}\n"
            "if mode=='no_assertions': payload['assertions']=[]\n"
            "if mode in ('fail','kill_runtime_fail'): payload['status']='fail'\n"
            "if mode=='skip': payload['status']='skip'\n"
            "if mode=='blocked': payload['status']='blocked'\n"
            "out.write_text(json.dumps(payload))\n"
            "sys.exit(1 if mode in ('wrong_exit','kill_runtime_fail') else 2 if mode=='blocked' else 0)\n"
        )
        modes = ("pass", "fail", "empty", "invalid", "no_assertions", "skip", "blocked", "wrong_exit", "timeout")
        self.cases = {("TEST-" + mode.upper()): {
            "implementation": "implemented", "requires": [], "command": [sys.executable, str(self.plugin), mode],
            "timeout_seconds": 0.15 if mode == "timeout" else 5, "source_lock_required": True}
            for mode in modes}
        self.cases["TEST-UNIMPLEMENTED"] = {"implementation": "unimplemented", "reason": "not written"}
        self.cases["TEST-DEPENDENCY"] = dict(self.cases["TEST-PASS"], requires=["DEP-TEST"])
        self.save_cases()
        write_json(self.driver / "state/gates.json", {"gates": {
            "GOOD": {"cases": ["TEST-PASS"]}, "BAD": {"cases": ["TEST-PASS", "TEST-FAIL"]},
            "MISSING": {"cases": ["TEST-PASS", "TEST-UNIMPLEMENTED"]},
            "SKIPPING": {"cases": ["TEST-SKIP"]}, "EMPTY": {"cases": []},
            "CYCLE": {"gates": ["CYCLE"]}}})

    def tearDown(self):
        self.temporary.cleanup()

    def save_cases(self):
        write_json(self.driver / "state/cases.json", {"schema_version": 1, "cases": self.cases})

    def test_restart_exception_does_not_allow_binary_or_owner_changes(self):
        before = {'pid': 100, 'pidfile': '/owned/data/postmaster.pid',
                  'executable': '/owned/bin/gaussdb', 'sha256': 'fixed'}
        after = dict(before, pid=101)
        self.assertFalse(runtime_matches(before, after))
        self.assertTrue(runtime_matches(before, after, True))
        self.assertFalse(runtime_matches(before, dict(after, sha256='other'), True))
        self.assertFalse(runtime_matches(before, dict(after, executable='/user/bin/gaussdb'), True))
        self.assertFalse(runtime_matches(before, dict(after, pidfile='/user/data/postmaster.pid'), True))
        self.assertFalse(runtime_matches(before, None, True))

    def test_real_runtime_loss_cannot_downgrade_execution_failure(self):
        # A real subprocess and /proc identity exercise the same code as the
        # server check. This is a harness test, not a simulated SQL lock test.
        for mode in ('kill_runtime_fail', 'kill_runtime_pass'):
            with self.subTest(mode=mode):
                child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
                try:
                    pidfile = self.base / 'runtime.pid'
                    pidfile.write_text(str(child.pid) + '\n')
                    self.cases['TEST-RUNTIME-LOSS'] = dict(self.cases['TEST-PASS'],
                        runtime={'pidfile': str(pidfile), 'binary': str(Path(sys.executable).resolve())},
                        command=[sys.executable, str(self.plugin), mode, str(child.pid)])
                    self.save_cases()
                    result, _ = run_case('TEST-RUNTIME-LOSS', self.ctx)
                    self.assertEqual(result['status'], 'fail')
                    self.assertEqual(result['exit_code'], 1)
                    self.assertEqual(result['runtime']['pid'], child.pid)
                    self.assertEqual(result['runtime_after']['status'], 'unavailable')
                    expected = 'fail' if mode.endswith('_fail') else 'pass'
                    self.assertEqual(result['original_test_status'], expected)
                    self.assertEqual(result['original_test_result']['status'], expected)
                    self.assertEqual(result['original_test_exit_code'], 1 if expected == 'fail' else 0)
                finally:
                    if child.poll() is None:
                        child.terminate()
                    child.wait(timeout=5)

    def test_missing_runtime_before_execution_is_blocked(self):
        self.cases['TEST-MISSING-RUNTIME'] = dict(self.cases['TEST-PASS'],
            runtime={'pidfile': str(self.base / 'absent.pid'), 'binary': str(Path(sys.executable).resolve())})
        self.save_cases()
        result, _ = run_case('TEST-MISSING-RUNTIME', self.ctx)
        self.assertEqual(result['status'], 'blocked')
        self.assertNotIn('process', result)

    def test_binary_change_during_execution_is_failure(self):
        binary = self.base / 'registered-test-binary'
        binary.write_bytes(b'original')
        self.cases['TEST-BINARY-CHANGED'] = dict(self.cases['TEST-PASS'],
            required_binaries=[str(binary)],
            command=[sys.executable, str(self.plugin), 'change_binary', str(binary)])
        self.save_cases()
        result, _ = run_case('TEST-BINARY-CHANGED', self.ctx)
        self.assertEqual(result['status'], 'fail')
        self.assertEqual(result['original_test_status'], 'pass')
        self.assertEqual(result['original_test_exit_code'], 0)
        self.assertNotEqual(result['provenance']['binaries'], result['provenance_after']['binaries'])

    def test_explicit_assertions_are_required(self):
        for name in ("EMPTY", "INVALID", "NO_ASSERTIONS"):
            with self.subTest(name=name):
                result, _ = run_case("TEST-" + name, self.ctx)
                self.assertEqual(result["exit_code"], 1)

    def test_unknown_missing_and_unimplemented_are_blocked(self):
        for name in ("UNKNOWN", "DEPENDENCY", "UNIMPLEMENTED", "BLOCKED"):
            result, _ = run_case("TEST-" + name, self.ctx)
            self.assertEqual(result["status"], "blocked", name)
            self.assertEqual(result["exit_code"], 2, name)

    def test_nonzero_exit_cannot_be_overridden_by_pass_json(self):
        result, _ = run_case("TEST-WRONG_EXIT", self.ctx)
        self.assertEqual(result["status"], "fail")

    def test_gate_propagates_fail_blocked_and_skip(self):
        for gate, expected in (("GOOD", 0), ("BAD", 1), ("MISSING", 2), ("SKIPPING", 2), ("EMPTY", 2), ("CYCLE", 2)):
            with self.subTest(gate=gate):
                result, _ = run_gate(gate, self.ctx)
                self.assertEqual(result["exit_code"], expected)

    def test_timeout_terminates_children_and_next_case_runs(self):
        result, path = run_case("TEST-TIMEOUT", self.ctx)
        self.assertEqual(result["status"], "fail")
        self.assertTrue(result["process"]["timed_out"])
        pid = int((path.parent / "child.pid").read_text())
        stat = Path(f"/proc/{pid}/stat")
        # A reparented zombie is not executing; init can reap it asynchronously.
        for _ in range(20):
            if not stat.exists() or stat.read_text().split()[2] == "Z":
                break
            time.sleep(0.01)
        self.assertTrue(not stat.exists() or stat.read_text().split()[2] == "Z")
        following, _ = run_case("TEST-PASS", self.ctx)
        self.assertEqual(following["exit_code"], 0)

    def test_cleanup_runs_after_malformed_result(self):
        marker = self.base / "cleaned"
        cleanup = self.driver / "cleanup.py"
        cleanup.write_text("from pathlib import Path\nPath(" + repr(str(marker)) + ").write_text('done')\n")
        self.cases["TEST-INVALID"]["cleanup_command"] = [sys.executable, str(cleanup)]
        self.save_cases()
        result, _ = run_case("TEST-INVALID", self.ctx)
        self.assertEqual(result["status"], "fail")
        self.assertTrue(marker.is_file())

    def test_changed_lock_invalidates_passing_evidence(self):
        result, path = run_case("TEST-PASS", self.ctx)
        verify_result(path, self.ctx)
        write_json(self.driver / "source-lock.json", {"target_format": "1.1", "patch": "changed"})
        with self.assertRaises(Blocked):
            verify_result(path, self.ctx)

    def test_changed_binary_and_script_invalidate_evidence(self):
        binary = self.base / "test-binary"
        binary.write_bytes(b"original")
        self.cases["TEST-PASS"]["required_binaries"] = [str(binary)]
        self.save_cases()
        _, path = run_case("TEST-PASS", self.ctx)
        binary.write_bytes(b"changed")
        with self.assertRaises(Blocked):
            verify_result(path, self.ctx)
        binary.write_bytes(b"original")
        verify_result(path, self.ctx)
        self.plugin.write_text(self.plugin.read_text() + "\n# changed test\n")
        with self.assertRaises(Blocked):
            verify_result(path, self.ctx)

    def test_gate_child_evidence_is_not_replaceable(self):
        _, path = run_gate("GOOD", self.ctx)
        result = verify_result(path, self.ctx)
        child = Path(result["children"][0]["path"])
        child.write_text("{}")
        with self.assertRaises(Blocked):
            verify_result(path, self.ctx)

    def test_raw_evidence_changes_invalidate_result(self):
        _, path = run_case("TEST-PASS", self.ctx)
        verify_result(path, self.ctx)
        (path.parent / "process.stdout.log").write_text("replaced log")
        with self.assertRaises(Blocked):
            verify_result(path, self.ctx)

    def test_task_verification_needs_all_tests_and_deliverables(self):
        script = Path(__file__).resolve().parents[1] / "taskctl.py"
        _, evidence = run_case("TEST-PASS", self.ctx)
        state_path = self.driver / "state/tasks.json"
        task = {"status": "implemented_unverified", "test_ids": ["TEST-PASS"],
                "depends_on": [], "deliverables": ["missing-product.cpp"]}
        write_json(state_path, {"tasks": {"EXAMPLE": task}})
        command = [sys.executable, str(script), "--driver-root", str(self.driver),
                   "--source", str(self.source), "verify", "EXAMPLE", "--result", str(evidence)]
        missing = subprocess.run(command, capture_output=True, check=False)
        self.assertEqual(missing.returncode, 2)
        self.assertNotEqual(read_json(state_path)["tasks"]["EXAMPLE"]["status"], "verified")
        (self.driver / "missing-product.cpp").write_text("test-only deliverable\n")
        complete = subprocess.run(command, capture_output=True, check=False)
        self.assertEqual(complete.returncode, 0, complete.stderr.decode())
        self.assertEqual(read_json(state_path)["tasks"]["EXAMPLE"]["status"], "verified")
        task["test_ids"].append("TEST-UNIMPLEMENTED")
        write_json(state_path, {"tasks": {"EXAMPLE": task}})
        partial = subprocess.run(command, capture_output=True, check=False)
        self.assertEqual(partial.returncode, 2)

    def test_untracked_product_source_participates_in_identity(self):
        def git(*args):
            subprocess.run(["git", "-C", str(self.source), *args], check=True, capture_output=True)
        git("init", "-q")
        git("-c", "user.name=Harness", "-c", "user.email=harness@invalid", "commit", "--allow-empty", "-qm", "baseline")
        before = source_identity(self.source)
        (self.source / "contrib/og_lake").mkdir(parents=True)
        (self.source / "contrib/og_lake/new.cpp").write_text("int value = 1;\n")
        after = source_identity(self.source)
        self.assertNotEqual(before, after)
        self.assertIn("contrib/og_lake/new.cpp", after["untracked_product_sources"])

    def test_barrier_handshake_timeout_and_cancel(self):
        barrier = Barrier()
        worker_errors = []
        def participant():
            try:
                barrier.arrive("read_complete", 1)
            except Exception as exc:
                worker_errors.append(exc)
        thread = threading.Thread(target=participant)
        thread.start()
        barrier.wait_arrived("read_complete", 1)
        barrier.release("read_complete")
        thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertFalse(worker_errors)
        with self.assertRaises(TimeoutError):
            barrier.wait_arrived("missing", 0.01)
        with self.assertRaises(TimeoutError):
            Barrier().arrive("unreleased", 0.01)
        cancelled = Barrier()
        cancelled.cancel()
        with self.assertRaises(RuntimeError):
            cancelled.arrive("cancelled", 0.01)

    def test_invalid_run_id_cannot_escape_evidence_root(self):
        with self.assertRaises(Blocked):
            Context("../escape", self.driver, self.base)

    def test_cli_unknown_case_returns_two(self):
        script = Path(__file__).resolve().parents[1] / "run_case.py"
        proc = subprocess.run([sys.executable, str(script), "--id", "TEST-NO_SUCH_CASE",
                               "--run-id", "cli", "--driver-root", str(self.driver),
                               "--source", str(self.source), "--evidence-root", str(self.base / "evidence")],
                              capture_output=True, check=False)
        self.assertEqual(proc.returncode, 2)


class ReportingResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.assertions = []
    def addSuccess(self, test):
        super().addSuccess(test)
        self.assertions.append({"name": test.id(), "status": "pass"})
    def addFailure(self, test, err):
        super().addFailure(test, err)
        self.assertions.append({"name": test.id(), "status": "fail", "details": str(err[1])})
    def addError(self, test, err):
        super().addError(test, err)
        self.assertions.append({"name": test.id(), "status": "fail", "details": str(err[1])})


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2, resultclass=ReportingResult).run(
        unittest.defaultTestLoader.loadTestsFromTestCase(HarnessTests))
    payload = {"status": "pass" if result.wasSuccessful() else "fail",
               "scope": "harness-only; no SDK, lake DML, or RAC claim",
               "assertions": result.assertions, "tests_run": result.testsRun}
    output = os.environ.get("OG_LAKE_RESULT_FILE")
    if output:
        write_json(output, payload)
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result.wasSuccessful() else 1)
