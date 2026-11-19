#!/usr/bin/env python3
"""Tests for the tiles sync and Bunny infra workflows' own logic. Run: python3 -m unittest discover -s deployment/scripts
-p 'test_*.py'

Needs PyYAML (python3-yaml, on GitHub's Ubuntu runners), bash and jq.
"""
import json
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


BUNNY_INFRA = "tiles-bunny-infra.yml"
PLAN_ENVIRONMENT = "tiles-bunny-plan"


def install(bin_dir, tools):
    """Writes each {name: script} into bin_dir, executable."""
    os.makedirs(bin_dir, exist_ok=True)
    for tool, text in tools.items():
        path = os.path.join(bin_dir, tool)
        with open(path, "w") as f:
            f.write(text)
        os.chmod(path, 0o755)


def run_bash(script_text, cwd, env):
    """A step's script as GitHub's bash shell runs it: bash --noprofile --norc -eo pipefail {0}."""
    script = os.path.join(cwd, ".step.sh")
    with open(script, "w") as f:
        f.write(script_text)
    try:
        return subprocess.run(["bash", "--noprofile", "--norc", "-eo", "pipefail", script], check=False, env=env,
                              cwd=cwd, capture_output=True, text=True)
    finally:
        os.remove(script)


def step(job, name):
    return next(s for s in job["steps"] if s.get("name") == name)


class BunnyInfraCredentialsTest(unittest.TestCase):
    """tiles-bunny-infra.yml: branch runs check without credentials; their plan runs only once a reviewer approves it."""

    def setUp(self):
        self.workflow = load(BUNNY_INFRA)
        self.jobs = self.workflow["jobs"]

    def test_only_the_plan_and_main_apply_read_production_credentials(self):
        credentialed = {job for job, body in self.jobs.items() if PROD_TOKEN in yaml.safe_dump(body)}
        self.assertEqual(credentialed, {"plan", "apply"})
        self.assertNotIn("secrets.OP_", yaml.safe_dump(self.jobs["check"]))
        self.assertTrue(requires_main(self.jobs["apply"]["if"]))
        # pull_request runs a fork's code without the repository's secrets; pull_request_target would give them.
        self.assertEqual(set(self.workflow[True]), {"workflow_dispatch", "pull_request", "push"})

    def test_the_branch_plan_waits_for_approval_and_checks_it_is_required_before_any_secret(self):
        plan = self.jobs["plan"]
        self.assertFalse(requires_main(plan["if"]))
        self.assertEqual(plan["environment"], PLAN_ENVIRONMENT)
        self.assertNotIn("secrets.", yaml.safe_dump({k: v for k, v in plan.items() if k != "steps"}))
        guard = plan["steps"][0]
        self.assertEqual(guard["name"], "Check the environment requires approval")
        self.assertEqual(guard["env"]["PLAN_ENVIRONMENT"], PLAN_ENVIRONMENT)
        self.assertNotIn("secrets.", yaml.safe_dump(guard))
        self.assertEqual(plan["permissions"], {"actions": "read", "contents": "read"})


FAKE_GH = """#!/bin/sh
# gh api PATH: writes its arguments to $FAKE_GH_ARGS, prints $FAKE_ENVIRONMENT and exits $FAKE_GH_EXIT.
printf '%s\\n' "$*" > "$FAKE_GH_ARGS"
printf '%s' "$FAKE_ENVIRONMENT"
exit "${FAKE_GH_EXIT:-0}"
"""

REVIEWERS = {"type": "required_reviewers", "prevent_self_review": True,
             "reviewers": [{"type": "User", "reviewer": {"login": "maintainer"}}]}


class PlanEnvironmentStepTest(unittest.TestCase):
    """The plan job's first step: it stops the job unless its environment makes a reviewer approve each run."""

    def run_step(self, environment, gh_exit=0):
        guard = step(load(BUNNY_INFRA)["jobs"]["plan"], "Check the environment requires approval")
        with tempfile.TemporaryDirectory() as tmp:
            install(os.path.join(tmp, "bin"), {"gh": FAKE_GH})
            args = os.path.join(tmp, "gh_args")
            env = dict(os.environ, **guard["env"])
            env.update(PATH=os.path.join(tmp, "bin") + os.pathsep + env["PATH"], GH_TOKEN="token",
                       GITHUB_REPOSITORY="immich-app/geoshenanigans", FAKE_GH_ARGS=args, FAKE_GH_EXIT=str(gh_exit),
                       FAKE_ENVIRONMENT=environment if isinstance(environment, str) else json.dumps(environment))
            result = run_bash(guard["run"], tmp, env)
            with open(args) as f:
                self.assertEqual(f.read(), f"api repos/immich-app/geoshenanigans/environments/{PLAN_ENVIRONMENT}\n")
            return result

    def test_an_environment_with_a_reviewer_passes(self):
        for rules in ([REVIEWERS], [{"type": "wait_timer", "wait_timer": 5}, REVIEWERS],
                      [{"type": "branch_policy"}, {**REVIEWERS, "prevent_self_review": False}]):
            with self.subTest(rules=rules):
                self.assertEqual(self.run_step({"name": PLAN_ENVIRONMENT, "protection_rules": rules}).returncode, 0)

    def test_an_environment_that_lets_runs_through_fails(self):
        # What GitHub creates when a job names an environment nobody set up: no protection rules.
        for environment in ({"name": PLAN_ENVIRONMENT, "protection_rules": []}, {"name": PLAN_ENVIRONMENT},
                            {"protection_rules": [{"type": "wait_timer", "wait_timer": 30}, {"type": "branch_policy"}]},
                            {"protection_rules": [{**REVIEWERS, "reviewers": []}]},
                            {"protection_rules": [{"type": "required_reviewers"}]}):
            with self.subTest(environment=environment):
                result = self.run_step(environment)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(f"::error::The {PLAN_ENVIRONMENT} environment has no required reviewers", result.stdout)

    def test_an_unreadable_environment_fails(self):
        self.assertNotEqual(self.run_step('{"message": "Not Found"}', gh_exit=1).returncode, 0)
        self.assertNotEqual(self.run_step("not json").returncode, 0)


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
