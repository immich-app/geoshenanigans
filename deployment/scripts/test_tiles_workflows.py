#!/usr/bin/env python3
"""Tests for the tiles sync workflows' own logic. Run: python3 -m unittest discover -s deployment/scripts -p 'test_*.py'

Needs PyYAML (python3-yaml, on GitHub's Ubuntu runners), bash and jq.
"""
import os
import subprocess
import tempfile
import unittest

import yaml

WORKFLOWS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".github", "workflows")
MAIN = "github.ref == 'refs/heads/main'"
PROD_TOKEN = "secrets.OP_TF_PROD_ENV"
# Every job of these that resolves production 1Password items runs main's code only, dry runs and dispatches
# included: a branch could otherwise run its own code with the production service account.
CREDENTIALED_JOBS = {
    "tiles-sync.yml": frozenset({"resolve", "sync", "report"}),
    "tiles-sync-watchdog.yml": frozenset({"report", "sample"}),
}


def load(name):
    with open(os.path.join(WORKFLOWS, name)) as f:
        return yaml.safe_load(f)


def top_level(expression, operator):
    """Splits a GitHub expression at `operator` where it is outside parentheses and string literals."""
    parts, depth, quoted, start, i = [], 0, False, 0, 0
    while i < len(expression):
        char = expression[i]
        if char == "'":
            quoted = not quoted  # '' (an escaped quote) closes and reopens the literal
        elif not quoted and char == "(":
            depth += 1
        elif not quoted and char == ")":
            depth -= 1
        elif not quoted and depth == 0 and expression.startswith(operator, i):
            parts.append(expression[start:i].strip())
            i += len(operator)
            start = i
            continue
        i += 1
    parts.append(expression[start:].strip())
    return parts


def requires_main(condition):
    """Whether a job condition is false on every ref but main: main is one of its top-level && terms."""
    condition = " ".join(str(condition or "").split())
    if condition.startswith("${{") and condition.endswith("}}"):
        condition = condition[3:-2].strip()
    return len(top_level(condition, "||")) == 1 and MAIN in top_level(condition, "&&")


class ProductionCredentialsTest(unittest.TestCase):
    def test_every_job_that_reads_production_credentials_requires_main_itself(self):
        for name, expected in CREDENTIALED_JOBS.items():
            jobs = load(name)["jobs"]
            credentialed = {job for job, body in jobs.items() if PROD_TOKEN in yaml.safe_dump(body)}
            self.assertEqual(credentialed, expected, name)
            for job in credentialed:
                with self.subTest(workflow=name, job=job):
                    self.assertTrue(requires_main(jobs[job].get("if")), jobs[job].get("if"))

    def test_no_credentialed_step_keeps_a_branch_path(self):
        for name, expected in CREDENTIALED_JOBS.items():
            jobs = load(name)["jobs"]
            for job in expected:
                for step in jobs[job]["steps"]:
                    with self.subTest(workflow=name, job=job, step=step.get("name")):
                        self.assertNotIn("GITHUB_REF", step.get("run", ""))

    def test_a_dispatch_from_another_branch_fails_without_credentials(self):
        for name in CREDENTIALED_JOBS:
            refuse = load(name)["jobs"]["main-only"]
            with self.subTest(workflow=name):
                self.assertEqual(refuse["if"], "github.ref != 'refs/heads/main'")
                self.assertEqual(refuse["permissions"], {})
                self.assertNotIn("secrets.", yaml.safe_dump(refuse))
                self.assertIn("exit 1", refuse["steps"][-1]["run"])

    def test_the_conditions_that_let_branches_in_are_rejected(self):
        # What the jobs had before: a branch's dry run, and the watchdog gated on the schedule alone.
        self.assertFalse(requires_main("github.ref == 'refs/heads/main' || inputs.dry_run"))
        self.assertFalse(requires_main("github.event.schedule != '53 4 * * *'"))
        self.assertFalse(requires_main(
            "github.event.schedule == '53 4 * * *' || github.event_name == 'workflow_dispatch'"))
        self.assertFalse(requires_main("inputs.dry_run || github.ref == 'refs/heads/main' && always()"))
        self.assertFalse(requires_main("(github.ref == 'refs/heads/main' || inputs.dry_run) && always()"))
        self.assertTrue(requires_main("always() && github.ref == 'refs/heads/main' && !inputs.dry_run"))
        self.assertTrue(requires_main(
            "${{ github.ref == 'refs/heads/main' && (github.event.schedule == '53 4 * * *' || inputs.x == 'a||b') }}"))


FAKE_OP = """#!/bin/sh
# op run [options] -- CMD...: runs CMD.
while [ "$1" != -- ]; do shift; done
shift
exec "$@"
"""

FAKE_NPM = """#!/bin/sh
# npm run --silent bunny:replication -- ... --json PATH ...: writes $FAKE_REPORT (when set) to PATH, exits $FAKE_EXIT.
while [ $# -gt 0 ]; do
  if [ "$1" = --json ]; then out=$2; fi
  shift
done
if [ -n "${FAKE_REPORT+set}" ]; then printf '%s' "$FAKE_REPORT" > "$out"; fi
echo "fake bunny:replication stderr" >&2
exit "$FAKE_EXIT"
"""


class SampleCheckStepTest(unittest.TestCase):
    """The watchdog's "Check the sample" step: the job's last step fails it unless the CLI ran the whole check."""

    def run_step(self, exit_code, report=None):
        step = next(s for s in load("tiles-sync-watchdog.yml")["jobs"]["sample"]["steps"] if s.get("id") == "check")
        with tempfile.TemporaryDirectory() as tmp:
            bin_dir = os.path.join(tmp, "bin")
            os.mkdir(bin_dir)
            for tool, text in (("op", FAKE_OP), ("npm", FAKE_NPM)):
                path = os.path.join(bin_dir, tool)
                with open(path, "w") as f:
                    f.write(text)
                os.chmod(path, 0o755)
            script = os.path.join(tmp, "step.sh")
            with open(script, "w") as f:
                f.write(step["run"])
            output = os.path.join(tmp, "github_output")
            env = {key: value for key, value in os.environ.items() if key != "FAKE_REPORT"}
            env.update(PATH=bin_dir + os.pathsep + env["PATH"], RUNNER_TEMP=tmp, GITHUB_OUTPUT=output,
                       FAKE_EXIT=str(exit_code))
            if report is not None:
                env["FAKE_REPORT"] = report
            # GitHub's bash shell: bash --noprofile --norc -eo pipefail {0}
            result = subprocess.run(["bash", "--noprofile", "--norc", "-eo", "pipefail", script], check=False, env=env,
                                    cwd=tmp, capture_output=True, text=True)
            with open(output) as f:
                self.assertEqual(f.read(), f"exit_code={exit_code}\n")
            return result.returncode

    def test_an_error_before_the_check_fails_the_step(self):
        # bunny:replication's top-level handler exits 1 without a report, e.g. when the live manifest's download fails.
        self.assertNotEqual(self.run_step(1), 0)

    def test_findings_with_their_report_pass_to_the_reporter(self):
        self.assertEqual(self.run_step(1, '{"checked": 1800, "missing": 2, "mismatched": 0, "errors": 0}'), 0)
        self.assertEqual(self.run_step(2, '{"checked": 1800, "partial": 40, "lacking": {"SYD": 40}}'), 0)

    def test_an_incomplete_check_fails_the_step(self):
        # DESCRIBE failed after its retries for some positions: the CLI exits 1 with a report, which the reporter
        # pushes as result=error (tiles_sync_report.sample), and the job fails like a check that could not run.
        self.assertNotEqual(self.run_step(1, '{"checked": 1800, "missing": 0, "mismatched": 0, "errors": 3}'), 0)
        self.assertNotEqual(self.run_step(1, '{"checked": 1800, "missing": 1800, "errors": 1800}'), 0)
        self.assertNotEqual(self.run_step(1, '{"checked": 1800, "missing": 2, "mismatched": 0, "errors": 1}'), 0)

    def test_a_clean_check_and_a_skipped_one_pass(self):
        self.assertEqual(self.run_step(0, '{"checked": 1800, "missing": 0}'), 0)
        self.assertEqual(self.run_step(0, '{"skipped": "pending builds 20261118: a sync has to finish first"}'), 0)

    def test_a_report_that_is_not_a_check_fails_the_step(self):
        self.assertNotEqual(self.run_step(0), 0)
        self.assertNotEqual(self.run_step(0, ""), 0)
        self.assertNotEqual(self.run_step(0, "null"), 0)
        self.assertNotEqual(self.run_step(0, "[]"), 0)
        self.assertNotEqual(self.run_step(1, '{"mode": "sample-per-zoom"}'), 0)
        self.assertNotEqual(self.run_step(1, '{"checked": 18'), 0)

    def test_a_usage_error_fails_the_step_even_with_a_report(self):
        self.assertNotEqual(self.run_step(3, '{"checked": 0}'), 0)


if __name__ == "__main__":
    unittest.main()
