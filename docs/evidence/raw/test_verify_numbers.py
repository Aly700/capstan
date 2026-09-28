"""Meaningful rejection tests: tamper with each independent source of load evidence."""
import gzip
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest

HERE = Path(__file__).resolve().parent


class EvidenceVerifierTests(unittest.TestCase):
    def setUp(self):
        verifier = HERE / "verify-numbers.py"
        self.assertTrue(verifier.exists(), "numeric verifier must exist")
        spec = importlib.util.spec_from_file_location("numbers", verifier)
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name) / "load"
        source = next((HERE / "historical/load").iterdir())
        shutil.copytree(source, self.folder)

    def test_original_samples_pass(self):
        self.module.verify_load(self.folder)

    def test_http_fault_category_is_not_dropped(self):
        self.assertEqual(self.module.table_counts("| http-503 | 12 |\n| approved | 9 |\n"),
                         {"http-503":12, "approved":9})

    def test_changed_raw_latency_fails(self):
        path = self.folder / "runs.jsonl.gz"
        rows = [json.loads(s) for s in gzip.decompress(path.read_bytes()).splitlines()]
        for row in rows:
            row["latency_ms"] += 1
        path.write_bytes(gzip.compress(("\n".join(map(json.dumps, rows)) + "\n").encode()))
        with self.assertRaises(AssertionError):
            self.module.verify_load(self.folder)

    def test_changed_sql_count_fails(self):
        path = self.folder / "audit.json"
        data = json.loads(path.read_text())
        data["activity_completions"] -= 1
        path.write_text(json.dumps(data))
        with self.assertRaises(AssertionError):
            self.module.verify_load(self.folder)

    def test_changed_emitted_rate_fails(self):
        path = self.folder / "result.json"
        data = json.loads(path.read_text())
        data["runs_per_second"] += 1
        path.write_text(json.dumps(data))
        with self.assertRaises(AssertionError):
            self.module.verify_load(self.folder)

    def test_duplicate_run_sample_fails(self):
        path = self.folder / "runs.jsonl.gz"
        rows = [json.loads(s) for s in gzip.decompress(path.read_bytes()).splitlines()]
        rows[1]["run_id"] = rows[0]["run_id"]
        path.write_bytes(gzip.compress(("\n".join(map(json.dumps, rows)) + "\n").encode()))
        with self.assertRaises(AssertionError):
            self.module.verify_load(self.folder)


if __name__ == "__main__":
    unittest.main()
