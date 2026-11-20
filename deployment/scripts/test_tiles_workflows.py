#!/usr/bin/env python3
"""Tests for the tiles sync and Bunny infra workflows' own logic. Run: python3 -m unittest discover -s deployment/scripts
-p 'test_*.py'

Needs PyYAML (python3-yaml, on GitHub's Ubuntu runners), bash, git and jq.
"""
import glob
import json
import os
import re
import shutil
import subprocess
import tempfile
import textwrap
import unittest

import yaml

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
WORKFLOWS = os.path.join(ROOT, ".github", "workflows")
MAIN = "github.ref == 'refs/heads/main'"
PROD_TOKEN = "secrets.OP_TF_PROD_ENV"
# The service account that reads the tf_bunny vault (the Bunny account key, the Globalping token): a secret of the
# tiles-bunny-plan and tiles-bunny environments only (deployment/modules/bunny/tiles/README.md).
BUNNY_TOKEN = "secrets.OP_TF_BUNNY_ENV"
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
# Deploys main only (its branch policy), with the Bunny token for main's jobs.
MAIN_ENVIRONMENT = "tiles-bunny"
BUNNY_SECRETS = ("BUNNY_API_KEY", "GLOBALPING_TOKEN")
# An environment expression that names tiles-bunny on one condition and no environment otherwise.
CONDITIONAL_ENVIRONMENT = re.compile(r"^\$\{\{ (.+) && 'tiles-bunny' \|\| '' \}\}$")


def workflows():
    names = (os.path.basename(path) for path in glob.glob(os.path.join(WORKFLOWS, "*.yml")))
    return {name: load(name) for name in names}


def env_files_named(job):
    """The files a job's `op run --env-file=...` resolves, as repository paths (an env file's name is unique)."""
    names = set()
    for item in job.get("steps", []):
        for match in re.finditer(r"--env-file=\"?([^\"\s)]+)\"?", item.get("run", "")):
            names.add(os.path.basename(match.group(1)))
    paths = {}
    for name in names:
        found = [path for path in glob.glob(os.path.join(ROOT, "deployment", "**", name), recursive=True)
                 if os.path.isfile(path)]
        assert len(found) == 1, (name, found)
        paths[name] = found[0]
    return paths


def read(path):
    with open(path) as f:
        return f.read()


def names_bunny_secrets(text):
    return "tf_bunny" in text or any(secret in text for secret in BUNNY_SECRETS)


class BunnyKeyTest(unittest.TestCase):
    """The Bunny account key and the Globalping token: only jobs that pass an environment holding the token that reads
    them (reviewers, or main only) get either; pull requests' plans get stand-ins."""

    def test_the_env_file_every_plan_resolves_names_neither(self):
        self.assertFalse(names_bunny_secrets(read(os.path.join(ROOT, "deployment", ".env"))))
        worker = read(os.path.join(ROOT, "deployment", "tiles-worker-bunny.env"))
        for secret in BUNNY_SECRETS:
            self.assertIn(f"op://tf_bunny/{secret}/password", worker)

    def test_the_bunny_secrets_live_in_tf_bunny_only(self):
        files = [os.path.join(WORKFLOWS, name) for name in workflows()]
        files += glob.glob(os.path.join(ROOT, "deployment", "**", "*.env"), recursive=True)
        files.append(os.path.join(ROOT, "deployment", ".env"))
        for path in files:
            for ref in re.findall(r"op://([^/\s'\"]+)/([^/\s'\"]+)", read(path)):
                if ref[1] in BUNNY_SECRETS:
                    with self.subTest(path=path, ref=ref):
                        self.assertEqual(ref[0], "tf_bunny")

    def test_only_jobs_in_a_bunny_environment_reach_the_bunny_secrets(self):
        reaching = set()
        for name, workflow in workflows().items():
            self.assertNotIn("tf_bunny", yaml.safe_dump(workflow.get("env", {})), name)
            self.assertNotIn(BUNNY_TOKEN, yaml.safe_dump(workflow.get("env", {})), name)
            for job_name, job in workflow["jobs"].items():
                text = yaml.safe_dump(job)
                files = env_files_named(job)
                bunny_files = sorted(file for file, path in files.items() if names_bunny_secrets(read(path)))
                if not (BUNNY_TOKEN in text or "tf_bunny" in text or bunny_files):
                    continue
                reaching.add((name, job_name))
                with self.subTest(workflow=name, job=job_name):
                    environment = job.get("environment")
                    self.assertIn(BUNNY_TOKEN, text)
                    if environment == PLAN_ENVIRONMENT:
                        self.assertEqual((name, job_name), (BUNNY_INFRA, "plan"))
                        continue
                    if environment == MAIN_ENVIRONMENT:
                        self.assertTrue(requires_main(job.get("if")), job.get("if"))
                        continue
                    # Only on the condition that names the environment: its token, and the env file and items it reads.
                    match = CONDITIONAL_ENVIRONMENT.match(str(environment))
                    self.assertIsNotNone(match, environment)
                    conditions = re.findall(r"\$\{\{ (.+?) && " + re.escape(BUNNY_TOKEN), text)
                    self.assertEqual(conditions, [match.group(1)] * text.count(BUNNY_TOKEN))
        self.assertEqual(reaching, {
            (BUNNY_INFRA, "plan"),
            (BUNNY_INFRA, "apply"),
            ("tiles-worker.yml", "deploy-terragrunt"),
            ("tiles-deploy.yml", "activate"),
            ("tiles-sync.yml", "sync"),
        })

    def test_the_main_jobs_resolve_the_bunny_secrets_only_in_prod(self):
        worker = load("tiles-worker.yml")["jobs"]["deploy-terragrunt"]
        self.assertEqual(worker["env"]["ENVIRONMENT"], "${{ github.ref == 'refs/heads/main' && 'prod' || 'dev' }}")
        self.assertEqual(worker["environment"], "${{ github.ref == 'refs/heads/main' && 'tiles-bunny' || '' }}")
        activate = load("tiles-deploy.yml")["jobs"]["activate"]
        self.assertEqual(activate["environment"], "${{ inputs.environment == 'prod' && 'tiles-bunny' || '' }}")
        for job, name in ((worker, "Deploy All"), (activate, "Apply tiles worker")):
            with self.subTest(step=name):
                script = step(job, name)["run"]
                self.assertIn('if [ "$ENVIRONMENT" = prod ]; then', script)
                self.assertIn("tiles-worker-bunny.env", script.split('if [ "$ENVIRONMENT" = prod ]; then')[1])
        sync = load("tiles-sync.yml")["jobs"]["sync"]
        self.assertEqual(sync["environment"], "${{ !inputs.dry_run && 'tiles-bunny' || '' }}")
        self.assertEqual(step(sync, "Sync")["env"]["BUNNY_API_KEY"],
                         "${{ !inputs.dry_run && 'op://tf_bunny/BUNNY_API_KEY/password' || '' }}")

    def test_pull_request_plans_get_stand_ins(self):
        workflow = load("tiles-worker.yml")
        self.assertIn("pull_request", workflow[True])
        plan = workflow["jobs"]["plan-terragrunt"]
        self.assertNotIn("environment", plan)
        self.assertEqual(set(env_files_named(plan)), {".env"})
        env = step(plan, "Plan All")["env"]
        for secret in BUNNY_SECRETS:
            self.assertEqual(env[f"TF_VAR_{secret.lower()}"], "plan-stand-in")

    def test_the_exposure_through_the_cloudflare_token_is_written_down_and_in_the_go_live_checklist(self):
        """The worker's Bunny key and Globalping token are out of every state, but a pull request's plan reads the
        Cloudflare account token, which can deploy code that reads Worker secrets: the README says so, the go-live
        step that puts the key on the worker points at the maintainers' recorded acceptance, and no comment claims
        otherwise."""
        readme = read(os.path.join(ROOT, "deployment", "modules", "bunny", "tiles", "README.md"))
        section = readme.split("### What this doesn't cover\n", 1)[1].split("\n#", 1)[0]
        self.assertIn("prod_cloudflare_api_keys", section)
        self.assertIn("`BUNNY_API_KEY` and `GLOBALPING_TOKEN`", section)
        checklist = readme.split("## Go-live checklist\n", 1)[1]
        step_3 = re.search(r"\n3\. \*\*Copy `pull_zone_id`\*\*.*?(?=\n4\. )", checklist, re.S).group(0)
        self.assertIn("The maintainers accepted this exposure", section)
        self.assertIn("The maintainers accepted that exposure (What this doesn't cover, above)", step_3)
        self.assertNotIn("neither value is in any state", readme)
        for path in (os.path.join(WORKFLOWS, "tiles-worker.yml"), os.path.join(ROOT, "deployment", ".env"),
                     os.path.join(ROOT, "deployment", "tiles-worker-bunny.env"),
                     os.path.join(ROOT, "deployment", "modules", "cloudflare", "tiles-worker", "workers.tf")):
            with self.subTest(path=os.path.relpath(path, ROOT)):
                text = read(path)
                self.assertNotIn("never gets the Bunny account key", text)
                self.assertIn("What this doesn't cover", text)

    def test_the_check_accepts_only_what_it_should(self):
        self.assertIsNotNone(CONDITIONAL_ENVIRONMENT.match("${{ inputs.x == 'prod' && 'tiles-bunny' || '' }}"))
        self.assertIsNone(CONDITIONAL_ENVIRONMENT.match("${{ inputs.x && 'tiles-bunny-plan' || '' }}"))
        self.assertIsNone(CONDITIONAL_ENVIRONMENT.match("${{ inputs.x && 'tiles-bunny' || 'other' }}"))
        self.assertTrue(names_bunny_secrets('export TF_VAR_bunny_api_key="op://tf_prod/BUNNY_API_KEY/password"'))
        self.assertFalse(names_bunny_secrets('export TF_VAR_env=$ENVIRONMENT'))


# The storage zones' read-write passwords (1Password item names), which can overwrite or delete every tile and the sync's
# state: in tf_bunny, beside the account key. Jobs that only read get each zone's read-only password, from tf_prod.
READ_WRITE_PASSWORDS = ("BUNNY_STORAGE_PASSWORD", "BUNNY_STATE_PASSWORD")
READ_ONLY_PASSWORDS = {"BUNNY_STORAGE_PASSWORD": "BUNNY_STORAGE_READONLY_PASSWORD",
                       "BUNNY_STATE_PASSWORD": "BUNNY_STATE_READONLY_PASSWORD"}
OP_REF = re.compile(r"op://([^/\s'\"]+)/([^/\s'\"]+)")


def env_settings():
    """Every (workflow, job, step, variable, value) a workflow, job or step `env:` sets (step None for job and workflow
    level, job None for workflow level)."""
    for name, workflow in workflows().items():
        for variable, value in (workflow.get("env") or {}).items():
            yield name, None, None, variable, str(value)
        for job_name, job in workflow["jobs"].items():
            for variable, value in (job.get("env") or {}).items():
                yield name, job_name, None, variable, str(value)
            for item in job.get("steps", []):
                for variable, value in (item.get("env") or {}).items():
                    yield name, job_name, item.get("name"), variable, str(value)


class BunnyStoragePasswordTest(unittest.TestCase):
    """OP_TF_PROD_ENV is a repository secret every same-repository pull request's Tiles Worker plan gets, so tf_prod
    holds only the zones' read-only passwords; the read-write ones are in tf_bunny, which only the tiles-bunny
    environments' service account reads, and only Tiles Sync's sync step (not a dry run) resolves them."""

    def test_the_read_write_passwords_live_in_tf_bunny_and_the_read_only_ones_in_tf_prod(self):
        files = [os.path.join(WORKFLOWS, name) for name in workflows()]
        files += glob.glob(os.path.join(ROOT, "deployment", "**", "*.env"), recursive=True)
        files.append(os.path.join(ROOT, "deployment", ".env"))
        seen = set()
        for path in files:
            for vault, item in OP_REF.findall(read(path)):
                with self.subTest(path=os.path.relpath(path, ROOT), ref=(vault, item)):
                    if item in READ_WRITE_PASSWORDS:
                        self.assertEqual(vault, "tf_bunny")
                    elif item in READ_ONLY_PASSWORDS.values():
                        self.assertEqual(vault, "tf_prod")
                    seen.add(item)
        self.assertLessEqual({*READ_WRITE_PASSWORDS, *READ_ONLY_PASSWORDS.values()}, seen)

    def test_only_the_sync_step_in_the_tiles_bunny_environment_gets_a_read_write_password(self):
        sync = load("tiles-sync.yml")["jobs"]["sync"]
        condition = CONDITIONAL_ENVIRONMENT.match(sync["environment"]).group(1)
        reaching = set()
        for name, job, step_name, variable, value in env_settings():
            refs = OP_REF.findall(value)
            if not any(item in READ_WRITE_PASSWORDS for _, item in refs):
                if variable in READ_ONLY_PASSWORDS and refs:
                    with self.subTest(workflow=name, job=job, step=step_name, variable=variable):
                        self.assertEqual(value, f"op://tf_prod/{READ_ONLY_PASSWORDS[variable]}/password")
                continue
            reaching.add((name, job, step_name, variable))
            with self.subTest(workflow=name, job=job, step=step_name, variable=variable):
                # On the very condition that puts the job in tiles-bunny; otherwise the read-only one, or none.
                self.assertIn(value, (
                    f"${{{{ {condition} && 'op://tf_bunny/{variable}/password' || "
                    f"'op://tf_prod/{READ_ONLY_PASSWORDS[variable]}/password' }}}}",
                    f"${{{{ {condition} && 'op://tf_bunny/{variable}/password' || '' }}}}",
                ))
        self.assertEqual(reaching, {
            ("tiles-sync.yml", "sync", "Sync", "BUNNY_STATE_PASSWORD"),
            ("tiles-sync.yml", "sync", "Sync", "BUNNY_STORAGE_PASSWORD"),
        })
        self.assertEqual(step(sync, "Sync")["env"]["OP_SERVICE_ACCOUNT_TOKEN"],
                         f"${{{{ {condition} && {BUNNY_TOKEN} || {PROD_TOKEN} }}}}")

    def test_the_jobs_that_only_read_get_the_read_only_passwords(self):
        watchdog = load("tiles-sync-watchdog.yml")["jobs"]
        sync = load("tiles-sync.yml")["jobs"]
        for job, name, variables in (
                (sync["resolve"], "Read the live build", ("BUNNY_STATE_PASSWORD",)),
                (sync["sync"], "Check the decision", ("BUNNY_STATE_PASSWORD",)),
                (watchdog["report"], "Read the live build", ("BUNNY_STATE_PASSWORD",)),
                (watchdog["sample"], "Check the sample", ("BUNNY_STATE_PASSWORD", "BUNNY_STORAGE_PASSWORD"))):
            for variable in variables:
                with self.subTest(step=name, variable=variable):
                    self.assertEqual(step(job, name)["env"][variable],
                                     f"op://tf_prod/{READ_ONLY_PASSWORDS[variable]}/password")


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
        credentialed = {job for job, body in self.jobs.items() if BUNNY_TOKEN in yaml.safe_dump(body)}
        self.assertEqual(credentialed, {"plan", "apply"})
        self.assertNotIn("secrets.OP_", yaml.safe_dump(self.jobs["check"]))
        self.assertTrue(requires_main(self.jobs["apply"]["if"]))
        # Each reads its token inside its environment, which holds it: none comes from a repository secret, so a branch
        # whose workflow drops the environment gets nothing.
        self.assertEqual(self.jobs["plan"]["environment"], PLAN_ENVIRONMENT)
        self.assertEqual(self.jobs["apply"]["environment"], MAIN_ENVIRONMENT)
        self.assertNotIn(PROD_TOKEN, yaml.safe_dump(self.workflow))
        self.assertNotIn("secrets.", yaml.safe_dump(self.workflow.get("env", {})))
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


GIT_ENV = {"GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1", "GIT_AUTHOR_NAME": "t",
           "GIT_AUTHOR_EMAIL": "t@example.invalid", "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.invalid"}


class MainCheckoutTest(unittest.TestCase):
    """A repository whose main gets commits, and a run's checkout of one of them, for the apply jobs' checks that the
    run's commit still has main's configuration. Subclasses give initial_files()."""

    def initial_files(self):
        """{path: text} of main's first commits."""
        raise NotImplementedError

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.env = dict(os.environ, **GIT_ENV)
        self.origin = os.path.join(self.tmp.name, "origin.git")
        self.main = os.path.join(self.tmp.name, "main")
        self.git(self.tmp.name, "init", "--quiet", "--bare", "--initial-branch=main", self.origin)
        self.git(self.tmp.name, "init", "--quiet", "--initial-branch=main", self.main)
        self.git(self.main, "remote", "add", "origin", self.origin)
        for path, text in self.initial_files().items():
            self.commit(path, text)

    def git(self, cwd, *args):
        return subprocess.run(["git", *args], cwd=cwd, env=self.env, check=True, capture_output=True,
                              text=True).stdout.strip()

    def commit(self, path, text):
        full = os.path.join(self.main, path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w") as f:
            f.write(text + "\n")
        self.git(self.main, "add", path)
        self.git(self.main, "commit", "--quiet", "-m", f"{path}: {text}")
        self.git(self.main, "push", "--quiet", "origin", "main")
        return self.git(self.main, "rev-parse", "HEAD")

    def checkout(self, sha):
        """The run's checkout: actions/checkout fetches the one commit, with origin pointing at the repository."""
        workspace = tempfile.mkdtemp(prefix=f"workspace-{sha[:7]}-", dir=self.tmp.name)
        self.git(self.tmp.name, "init", "--quiet", workspace)
        self.git(workspace, "remote", "add", "origin", "file://" + self.origin)
        self.git(workspace, "fetch", "--quiet", "--depth=1", "origin", sha)
        self.git(workspace, "checkout", "--quiet", "--detach", sha)
        return workspace

    def run_check(self, check, sha, **env):
        """Runs a check step's script in the checkout of sha: what it wrote to GITHUB_OUTPUT, and its stdout."""
        workspace = self.checkout(sha)
        output = os.path.join(self.tmp.name, "github_output")
        with open(output, "w"):
            pass
        result = run_bash(check["run"], workspace, dict(self.env, GITHUB_OUTPUT=output, GITHUB_SHA=sha, **env))
        self.assertEqual(result.returncode, 0, result.stderr)
        with open(output) as f:
            return f.read(), result.stdout

    def head(self):
        return self.git(self.main, "rev-parse", "HEAD")


class ApplyFreshnessStepTest(MainCheckoutTest):
    """The Bunny unit's apply job's check that the run's commit still has main's configuration: a re-run of an older
    main run, or one delayed behind a newer commit's, must not put the older configuration back."""

    def initial_files(self):
        paths = self.trigger_files() + ["tiles/src/index.ts", "deployment/modules/cloudflare/tiles-worker/a.tf"]
        return {path: "first" for path in paths}

    def trigger_files(self):
        """A file under each path that triggers the workflow on main."""
        paths = load(BUNNY_INFRA)[True]["push"]["paths"]
        return [path.removesuffix("/**") + "/tiles/main.tf" if path.endswith("/**") else path for path in paths]

    def run_step(self, sha):
        return self.run_check(step(load(BUNNY_INFRA)["jobs"]["apply"], "Check this commit has main's configuration"),
                              sha)

    def test_main_itself_applies(self):
        head = self.git(self.main, "rev-parse", "HEAD")
        self.assertEqual(self.run_step(head)[0], "current=true\n")

    def test_an_older_commit_applies_when_main_changed_only_other_files(self):
        old = self.git(self.main, "rev-parse", "HEAD")
        self.commit("tiles/src/index.ts", "newer")
        self.commit("deployment/modules/cloudflare/tiles-worker/a.tf", "newer")
        self.assertEqual(self.run_step(old)[0], "current=true\n")

    def test_an_older_commit_skips_when_main_changed_any_path_that_triggers_the_workflow(self):
        for path in self.trigger_files():
            with self.subTest(path=path):
                old = self.git(self.main, "rev-parse", "HEAD")
                head = self.commit(path, f"newer {path}")
                output, stdout = self.run_step(old)
                self.assertEqual(output, "current=false\n")
                self.assertIn(f"::warning::main ({head}) has another Bunny configuration than {old}", stdout)

    def test_the_trigger_paths_are_the_same_for_pull_requests_and_main(self):
        triggers = load(BUNNY_INFRA)[True]
        self.assertEqual(triggers["pull_request"]["paths"], triggers["push"]["paths"])
        self.assertIn("deployment/modules/bunny/tiles/main.tf", self.trigger_files())


WORKER_TFVARS = "deployment/modules/cloudflare/tiles-worker/tiles.{}.tfvars.json"


def worker_tfvars(key="20261101", **settings):
    return json.dumps({"pmtiles_deployment_key": key, "storage_prefix": "", **settings})


class WorkerApplyFreshnessTest(MainCheckoutTest):
    """Tiles Worker's apply jobs' check that the run's commit still has main's configuration: a re-run of an older
    main run, or one delayed behind a newer commit's, must not put an older deployment key, worker code or Bunny
    setting back, even with main's deployment key."""

    # A file of each kind of input the apply on main reads.
    INPUTS = ("tiles/src/index.ts", "tiles/package-lock.json", "deployment/modules/cloudflare/tiles-worker/workers.tf",
              "deployment/tiles-worker-bunny.env", "deployment/root.hcl", ".mise/config.toml", ".mise/mise.lock",
              ".github/workflows/tiles-worker.yml")
    OTHERS = ("README.md", "geocoder/src/main.cpp", "o11y/alerts/geoshenanigans-tiles.yaml")

    # Each file's text is a comment, which a mise config (that the step's tools may read) takes too.
    def initial_files(self):
        files = {path: "# first" for path in self.INPUTS + self.OTHERS}
        files.update({WORKER_TFVARS.format(env): worker_tfvars() for env in ("prod", "dev")})
        return files

    def check(self, job):
        return step(load("tiles-worker.yml")["jobs"][job], "Check this commit has main's configuration")

    def run_job(self, job, sha, ref="refs/heads/main", environment="prod"):
        return self.run_check(self.check(job), sha, GITHUB_REF=ref, ENVIRONMENT=environment)

    def test_the_steps_that_apply_wait_for_the_check(self):
        jobs = load("tiles-worker.yml")["jobs"]
        for job, name in (("deploy-terragrunt", "Deploy All"), ("deploy-dev-worker", "Deploy dev worker")):
            with self.subTest(job=job):
                self.assertEqual(self.check(job)["id"], "current")
                for each in ("Get build artifact", name):
                    self.assertEqual(step(jobs[job], each)["if"], "steps.current.outputs.current == 'true'")

    def test_main_itself_applies(self):
        for job, environment in (("deploy-terragrunt", "prod"), ("deploy-dev-worker", "dev")):
            with self.subTest(job=job):
                self.assertEqual(self.run_job(job, self.head(), environment=environment)[0], "current=true\n")

    def test_an_older_commit_applies_when_main_changed_only_other_files(self):
        old = self.head()
        for path in self.OTHERS:
            self.commit(path, "# newer")
        for job, environment in (("deploy-terragrunt", "prod"), ("deploy-dev-worker", "dev")):
            with self.subTest(job=job):
                self.assertEqual(self.run_job(job, old, environment=environment)[0], "current=true\n")

    def test_an_older_commit_skips_when_main_changed_a_bunny_setting_with_the_same_key(self):
        old = self.head()
        for settings in ({"bunny_pull_zone_id": "12345"}, {"bunny_pull_zone_id": "12345",
                                                           "bunny_public_url": "https://tiles-bunny.example.com"}):
            head = self.commit(WORKER_TFVARS.format("prod"), worker_tfvars(**settings))
            for job, environment in (("deploy-terragrunt", "prod"), ("deploy-dev-worker", "dev")):
                with self.subTest(job=job, settings=settings):
                    output, stdout = self.run_job(job, old, environment=environment)
                    self.assertEqual(output, "current=false\n")
                    self.assertIn(f"::warning::main ({head}) deploys other {environment} inputs than {old}", stdout)

    def test_an_older_commit_skips_when_main_changed_any_input(self):
        for path in self.INPUTS + (WORKER_TFVARS.format("prod"), WORKER_TFVARS.format("dev")):
            old = self.head()
            self.commit(path, f"# newer {path}" if path not in (WORKER_TFVARS.format("prod"), WORKER_TFVARS.format("dev"))
                        else worker_tfvars(storage_prefix="newer/"))
            for job, environment in (("deploy-terragrunt", "prod"), ("deploy-dev-worker", "dev")):
                with self.subTest(path=path, job=job):
                    self.assertEqual(self.run_job(job, old, environment=environment)[0], "current=false\n")

    def test_a_dispatch_from_a_branch_deploys_its_own_code_to_dev_unless_main_moved_the_key(self):
        branch = self.commit("tiles/src/index.ts", "# a branch's code")
        self.git(self.main, "reset", "--quiet", "--hard", "HEAD~1")
        self.git(self.main, "push", "--quiet", "--force", "origin", "main")
        self.commit("deployment/modules/cloudflare/tiles-worker/workers.tf", "# main moved on")
        self.assertEqual(self.run_job("deploy-terragrunt", branch, "refs/heads/feature", "dev")[0], "current=true\n")
        self.commit(WORKER_TFVARS.format("dev"), worker_tfvars("20261118"))
        output, stdout = self.run_job("deploy-terragrunt", branch, "refs/heads/feature", "dev")
        self.assertEqual(output, "current=false\n")
        self.assertIn("This commit deploys dev tiles 20261101 but main now has 20261118", stdout)


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


def evaluate(expression, context):
    """Evaluates the GitHub expressions these workflows use: string literals, context names, ==, !=, &&, || and
    parentheses, with GitHub's && and || returning an operand."""
    expression = " ".join(str(expression).split())
    if expression.startswith("${{") and expression.endswith("}}"):
        expression = expression[3:-2].strip()
    parts = top_level(expression, "||")
    if len(parts) > 1:
        for part in parts:
            value = evaluate(part, context)
            if value:
                return value
        return value
    parts = top_level(expression, "&&")
    if len(parts) > 1:
        for part in parts:
            value = evaluate(part, context)
            if not value:
                return value
        return value
    for operator, equal in (("!=", False), ("==", True)):
        parts = top_level(expression, operator)
        if len(parts) == 2:
            return (evaluate(parts[0], context) == evaluate(parts[1], context)) == equal
    if expression.startswith("(") and expression.endswith(")") and top_level(expression[1:-1], ")") == [
            expression[1:-1].strip()]:
        return evaluate(expression[1:-1], context)
    if expression.startswith("'") and expression.endswith("'"):
        return expression[1:-1].replace("''", "'")
    return context.get(expression, "")


def interpolate(text, context):
    """A string with ${{ }} expressions in it, as GitHub expands it."""
    return re.sub(r"\$\{\{(.+?)\}\}", lambda match: str(evaluate(match.group(1), context)), text)


WATCHDOG = "tiles-sync-watchdog.yml"
REPORT_SCHEDULE = "37 */2 * * *"
SAMPLE_SCHEDULE = "53 4 * * *"


class WatchdogConcurrencyTest(unittest.TestCase):
    """A newer run's report replaces an older one's, and a newer sample check an older one's, but a report never cancels
    a sample check (a scheduled report during a dispatch's sample check), nor does a refusal off main cancel main's."""

    RUNS = {
        "report": {"github.ref": "refs/heads/main", "github.event_name": "schedule",
                   "github.event.schedule": REPORT_SCHEDULE, "github.run_id": "1"},
        "sample": {"github.ref": "refs/heads/main", "github.event_name": "schedule",
                   "github.event.schedule": SAMPLE_SCHEDULE, "github.run_id": "2"},
        "dispatch": {"github.ref": "refs/heads/main", "github.event_name": "workflow_dispatch", "github.run_id": "3"},
        "branch": {"github.ref": "refs/heads/feature", "github.event_name": "workflow_dispatch", "github.run_id": "4"},
    }

    def setUp(self):
        self.workflow = load(WATCHDOG)
        self.jobs = self.workflow["jobs"]

    def groups(self, run):
        """{job: its concurrency group} for the jobs a run runs; the workflow's own group under None."""
        context = self.RUNS[run]
        groups = {}
        if "concurrency" in self.workflow:
            groups[None] = interpolate(self.workflow["concurrency"]["group"], context)
        for name, job in self.jobs.items():
            if job.get("if") is not None and not evaluate(job["if"], context):
                continue
            concurrency = job.get("concurrency")
            groups[name] = interpolate(concurrency["group"], context) if concurrency else None
        return groups

    def test_the_jobs_each_run_runs(self):
        self.assertEqual(set(self.groups("report")), {"report"})
        self.assertEqual(set(self.groups("sample")), {"sample"})
        self.assertEqual(set(self.groups("dispatch")), {"report", "sample"})
        self.assertEqual(set(self.groups("branch")), {"main-only"})

    def test_a_scheduled_report_replaces_only_the_dispatch_report(self):
        dispatch, report = self.groups("dispatch"), self.groups("report")
        self.assertNotIn(None, dispatch)
        self.assertEqual(report["report"], dispatch["report"])
        self.assertNotIn(report["report"], set(dispatch.values()) - {dispatch["report"]})
        for job in ("report", "sample"):
            self.assertIs(self.jobs[job]["concurrency"]["cancel-in-progress"], True)

    def test_the_daily_sample_check_replaces_only_the_dispatch_check(self):
        dispatch, sample = self.groups("dispatch"), self.groups("sample")
        self.assertEqual(sample["sample"], dispatch["sample"])
        self.assertNotEqual(sample["sample"], dispatch["report"])

    def test_a_refusal_off_main_shares_no_group_with_main(self):
        self.assertEqual(self.groups("branch"), {"main-only": None})
        on_main = {group for run in ("report", "sample", "dispatch") for group in self.groups(run).values()}
        # Even the jobs a run off main skips have groups of their own.
        context = self.RUNS["branch"]
        for job in ("report", "sample"):
            self.assertNotIn(interpolate(self.jobs[job]["concurrency"]["group"], context), on_main)

    def test_the_evaluator(self):
        context = {"github.ref": "refs/heads/main", "github.run_id": "7", "github.event.schedule": SAMPLE_SCHEDULE}
        self.assertEqual(evaluate("github.ref == 'refs/heads/main' && 'main' || github.run_id", context), "main")
        self.assertEqual(evaluate("github.ref != 'refs/heads/main' && 'x' || github.run_id", context), "7")
        self.assertFalse(evaluate(
            "github.ref == 'refs/heads/main' && (github.event.schedule != '53 4 * * *' || github.event_name == 'x')",
            context))
        self.assertEqual(interpolate("a-${{ github.run_id }}-b", context), "a-7-b")


FAKE_CURL_SECRETS = """#!/bin/bash
# curl ... [-X METHOD] [-H @FILE | -H HEADER]... [--data-binary @-] [--retry N] [--write-out FORMAT] URL: logs the call
# to $FAKE_LOG as a JSON line (its key, "METHOD PATH" with PATH after $FAKE_PREFIX and / ? = and spaces as _, the
# arguments, header lines and body), prints the answer in $FAKE_DIR/KEY.TURN (TURN 2 for the second call with that key)
# or else $FAKE_DIR/KEY, and exits with the number in that file's .exit, if any. The answer's HTTP status is the number
# in its .status, else 200 (000, no answer, with an .exit); FORMAT's %{http_code} prints it after the answer. As curl
# does, --retry N calls again, up to N times, while curl timed out or found no address (exit 28, 6 or 5) or the status
# is 408, 429, 500, 502, 503 or 504, each answer printed after the one before.
args=("$@")
method=GET
body=""
headers=()
retries=0
write_out=""
for ((i = 0; i < ${#args[@]}; i++)); do
  case "${args[i]}" in
    -X) method=${args[i+1]} ;;
    -H) if [[ "${args[i+1]}" == @* ]]; then headers+=("$(cat "${args[i+1]#@}")"); else headers+=("${args[i+1]}"); fi ;;
    --data-binary) if [ "${args[i+1]}" = @- ]; then body=$(cat); fi ;;
    --retry) retries=${args[i+1]} ;;
    -w | --write-out) write_out=${args[i+1]} ;;
  esac
done
url=${args[-1]}
key=$(printf '%s %s' "$method" "${url#"$FAKE_PREFIX"}" | tr '/?= ' '____')
http_code='%{http_code}'
while :; do
  turn=$(($(jq -r --arg key "$key" 'select(.key == $key) | .key' "$FAKE_LOG" 2>/dev/null | wc -l) + 1))
  jq -cn --arg key "$key" --arg method "$method" --arg url "$url" --arg body "$body" \\
    --argjson headers "$(jq -cn '$ARGS.positional' --args -- ${headers[@]+"${headers[@]}"})" \\
    '{key: $key, method: $method, url: $url, body: $body, headers: $headers, args: $ARGS.positional}' --args -- "$@" \\
    >> "$FAKE_LOG"
  code=0
  status=200
  answer=""
  for each in "$FAKE_DIR/$key.$turn" "$FAKE_DIR/$key"; do
    if [ -f "$each" ]; then answer=$each; break; fi
  done
  if [ -n "$answer" ]; then
    cat "$answer"
    if [ -f "$answer.exit" ]; then code=$(cat "$answer.exit"); status=000; fi
    if [ -f "$answer.status" ]; then status=$(cat "$answer.status"); fi
  else
    printf '%s' '{"success": false, "errors": [{"code": 7003, "message": "No route for that URI"}]}'
  fi
  case "$code $status" in
    "0 408" | "0 429" | "0 50"[0234] | "5 "* | "6 "* | "28 "*)
      if [ "$retries" -gt 0 ]; then
        retries=$((retries - 1))
        continue
      fi ;;
  esac
  printf '%s' "${write_out//"$http_code"/$status}"
  exit "$code"
done
"""

PUT_SECRETS = os.path.join(ROOT, "deployment", "modules", "cloudflare", "tiles-worker", "put-worker-secrets.sh")
SCRIPT_API = "https://api.cloudflare.com/client/v4/accounts/account/workers/scripts/tiles-prod"


def cloudflare(result=None, success=True, errors=()):
    return json.dumps({"success": success, "errors": list(errors), "messages": [], "result": result})


# The plan's time (plantimestamp), which workers.tf passes as PLANNED_AT.
PLANNED_AT = "2026-11-19T17:00:00Z"


def deployments(*versions, since_plan=1):
    """The script's deployments, the latest (serving) first, each of one version: the first `since_plan` made after the
    plan, a minute apart (the apply's upload), the others before it, an hour apart."""
    def created_on(i):
        if i < since_plan:
            return f"2026-11-19T17:0{since_plan - i}:30.418244Z"
        return f"2026-11-19T{16 - (i - since_plan):02d}:59:59.5Z"
    return cloudflare({"deployments": [{"id": f"d-{version}", "strategy": "percentage", "created_on": created_on(i),
                                        "versions": [{"version_id": version, "percentage": 100}]}
                                       for i, version in enumerate(versions)]})


def version(version_id, secrets):
    """A version whose bindings are a plain text one, O11Y_TOKEN and the given secrets."""
    bindings = [{"name": "DEPLOYMENT_KEY", "type": "plain_text", "text": "20261101"},
                {"name": "O11Y_TOKEN", "type": "secret_text"}]
    bindings += [{"name": name, "type": "secret_text"} for name in secrets]
    return cloudflare({"id": version_id, "number": 7, "resources": {"bindings": bindings}})


# As after a prod apply's upload: the latest deployment, v-upload, has neither Bunny secret; v-before, which served
# before it, has both.
AFTER_UPLOAD = {
    "GET /deployments": deployments("v-upload", "v-before", "v-older"),
    "GET /versions/v-upload": version("v-upload", []),
    "GET /versions/v-before": version("v-before", ["BUNNY_API_KEY", "GLOBALPING_TOKEN"]),
    "PUT /secrets": cloudflare({"name": "x", "type": "secret_text"}),
    "POST /deployments?force=true": cloudflare({"id": "d-restored"}),
}
REFUSED = cloudflare(success=False, errors=[{"code": 10000, "message": "Authentication error"}])


def fake_key(call):
    """FAKE_CURL_SECRETS' key for "METHOD PATH"."""
    return re.sub(r"[/?= ]", "_", call)


class PutWorkerSecretsTest(unittest.TestCase):
    """put-worker-secrets.sh, which puts the worker's Bunny secrets through Cloudflare's API after every apply, so
    that OpenTofu's state never holds them (workers.tf, terraform_data.bunny_secrets), and puts back the version that
    served before the apply's upload when it can't."""

    def run_script(self, answers=None, **overrides):
        """Runs the script against the fake API's answers ({"METHOD PATH": answer, or a list of them by turn, an
        answer a body, (body, curl's exit status) or (body, curl's exit status, HTTP status)}): its result and the
        calls it made. Its waits between tries take no time."""
        with tempfile.TemporaryDirectory() as tmp:
            install(os.path.join(tmp, "bin"), {"curl": FAKE_CURL_SECRETS, "sleep": "#!/bin/sh\n"})
            answer_dir = os.path.join(tmp, "answers")
            os.makedirs(answer_dir)
            for key, answer in (AFTER_UPLOAD if answers is None else answers).items():
                name = fake_key(key)
                turns = answer if isinstance(answer, list) else [answer]
                for turn, each in enumerate(turns, 1):
                    body, status, http_status = (each + (None,))[:3] if isinstance(each, tuple) else (each, 0, None)
                    path = os.path.join(answer_dir, f"{name}.{turn}" if isinstance(answer, list) else name)
                    with open(path, "w") as f:
                        f.write(body)
                    if status:
                        with open(path + ".exit", "w") as f:
                            f.write(str(status))
                    if http_status:
                        with open(path + ".status", "w") as f:
                            f.write(str(http_status))
            log = os.path.join(tmp, "log")
            env = dict(os.environ, PATH=os.path.join(tmp, "bin") + os.pathsep + os.environ["PATH"], FAKE_LOG=log,
                       FAKE_DIR=answer_dir, FAKE_PREFIX=SCRIPT_API, ACCOUNT_ID="account", SCRIPT="tiles-prod",
                       SECRETS="BUNNY_API_KEY GLOBALPING_TOKEN", CLOUDFLARE_API_TOKEN="cf-token-1",
                       PLANNED_AT=PLANNED_AT, BUNNY_API_KEY="bunny key 2", GLOBALPING_TOKEN='globalping "3"')
            env.update(overrides)
            result = subprocess.run(["bash", PUT_SECRETS], env=env, cwd=tmp, capture_output=True, text=True,
                                    check=False)
            calls = []
            if os.path.exists(log):
                with open(log) as f:
                    calls = [json.loads(line) for line in f]
            return result, calls

    def assert_nothing_leaks(self, result, calls):
        """Neither the token nor a value is on curl's command line, which other processes can read, or in the output;
        every call carries the token in its header file."""
        for call in calls:
            self.assertIn("Authorization: Bearer cf-token-1", call["headers"])
            self.assertTrue(call["url"].startswith(SCRIPT_API + "/"), call["url"])
            for secret in ("cf-token-1", "bunny key 2", 'globalping "3"'):
                self.assertNotIn(secret, " ".join(call["args"]))
        for secret in ("cf-token-1", "bunny key 2", 'globalping "3"'):
            self.assertNotIn(secret, result.stdout + result.stderr)

    def restores(self, calls):
        return [json.loads(call["body"]) for call in calls if call["key"] == fake_key("POST /deployments?force=true")]

    def test_puts_each_secret_from_its_variable(self):
        result, calls = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "Should a secret not go on, tiles-prod goes back to version v-before, which "
                                        "served before this apply's upload\nPut BUNNY_API_KEY on tiles-prod\n"
                                        "Put GLOBALPING_TOKEN on tiles-prod\n")
        puts = [call for call in calls if call["method"] == "PUT"]
        self.assertEqual(len(puts), 2)
        for put, name, value in zip(puts, ("BUNNY_API_KEY", "GLOBALPING_TOKEN"), ("bunny key 2", 'globalping "3"')):
            with self.subTest(secret=name):
                self.assertEqual(put["url"], SCRIPT_API + "/secrets")
                self.assertEqual(json.loads(put["body"]), {"name": name, "text": value, "type": "secret_text"})
        self.assertEqual(self.restores(calls), [])
        self.assert_nothing_leaks(result, calls)

    def test_an_empty_secret_or_an_odd_name_stops_before_any_request(self):
        for overrides, message in (({"GLOBALPING_TOKEN": ""}, "GLOBALPING_TOKEN is empty"),
                                   ({"SECRETS": "BUNNY_API_KEY", "BUNNY_API_KEY": ""}, "BUNNY_API_KEY is empty"),
                                   ({"SECRETS": "BUNNY_API_KEY bunny-api-key"}, "Not a secret name: bunny-api-key")):
            with self.subTest(**overrides):
                result, calls = self.run_script(**overrides)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(message, result.stderr)
                self.assertEqual(calls, [])

    def test_a_refused_put_puts_back_the_version_that_served_before_the_upload(self):
        result, calls = self.run_script(dict(AFTER_UPLOAD, **{"PUT /secrets": REFUSED}))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Could not put BUNNY_API_KEY on tiles-prod", result.stderr)
        self.assertIn("Authentication error", result.stderr)
        # No second secret goes on after the first failed, and the worker goes back to v-before at once, forced past
        # Cloudflare's check that its secrets changed since.
        self.assertEqual([call["key"] for call in calls][-2:],
                         [fake_key("PUT /secrets"), fake_key("POST /deployments?force=true")])
        self.assertEqual(len([call for call in calls if call["method"] == "PUT"]), 1)
        restore = self.restores(calls)
        self.assertEqual(len(restore), 1)
        self.assertEqual(restore[0]["strategy"], "percentage")
        self.assertEqual(restore[0]["versions"], [{"version_id": "v-before", "percentage": 100}])
        self.assertIn("BUNNY_API_KEY", restore[0]["annotations"]["workers/message"])
        self.assertIn("Deployed version v-before of tiles-prod again", result.stderr)
        self.assert_nothing_leaks(result, calls)

    def test_a_put_that_fails_halfway_or_gets_no_answer_puts_it_back_too(self):
        for name, put in (("second refused", [AFTER_UPLOAD["PUT /secrets"], REFUSED]),
                          ("not json", "<html>bad gateway</html>"),
                          ("curl failed", ("", 7))):
            with self.subTest(name):
                result, calls = self.run_script(dict(AFTER_UPLOAD, **{"PUT /secrets": put}))
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual([body["versions"][0]["version_id"] for body in self.restores(calls)], ["v-before"])
                self.assertIn("Deployed version v-before of tiles-prod again", result.stderr)
        self.assertIn("no API response", result.stderr)

    def test_a_put_that_goes_on_when_tried_again_puts_nothing_back(self):
        """What curl's --retry tries again is tried again, and only the last try's answer counts: with --retry, curl
        writes each try's answer after the one before, so a 502's page or a timed out answer's start, then the JSON
        of a PUT that went on, would read as no answer and put back the version before."""
        put = AFTER_UPLOAD["PUT /secrets"]
        for name, transient in (("bad gateway", ("<html>502 Bad Gateway</html>", 0, 502)),
                                ("timed out", ('{"success": tr', 28))):
            with self.subTest(name):
                result, calls = self.run_script(dict(AFTER_UPLOAD, **{"PUT /secrets": [transient, put, put]}))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(self.restores(calls), [])
                self.assertIn("Put BUNNY_API_KEY on tiles-prod\nPut GLOBALPING_TOKEN on tiles-prod\n", result.stdout)
                puts = [call for call in calls if call["method"] == "PUT"]
                self.assertEqual([json.loads(call["body"])["name"] for call in puts],
                                 ["BUNNY_API_KEY", "BUNNY_API_KEY", "GLOBALPING_TOKEN"])
                self.assertEqual(puts[0]["body"], puts[1]["body"])
                self.assert_nothing_leaks(result, calls)

    def test_a_put_that_keeps_failing_is_tried_four_times_then_put_back(self):
        result, calls = self.run_script(
            dict(AFTER_UPLOAD, **{"PUT /secrets": ("<html>503 Service Unavailable</html>", 0, 503)}))
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len([call for call in calls if call["method"] == "PUT"]), 4)
        self.assertEqual([body["versions"][0]["version_id"] for body in self.restores(calls)], ["v-before"])
        self.assertIn("Could not put BUNNY_API_KEY on tiles-prod: no API response", result.stderr)

    def test_a_put_that_fails_when_the_upload_dropped_nothing_changes_nothing(self):
        # No upload this apply: the serving version still has both secrets, so a failed PUT leaves it as it is.
        answers = dict(AFTER_UPLOAD, **{"GET /versions/v-upload": version("v-upload", ["BUNNY_API_KEY",
                                                                                     "GLOBALPING_TOKEN"]),
                                        "PUT /secrets": REFUSED})
        result, calls = self.run_script(answers)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.restores(calls), [])
        self.assertNotIn("goes back", result.stdout)

    def test_nothing_is_put_back_that_had_none_of_the_secrets(self):
        # The first apply with the secrets: the version before had neither, so going back turns nothing on.
        answers = dict(AFTER_UPLOAD, **{"GET /versions/v-before": version("v-before", []), "PUT /secrets": REFUSED})
        result, calls = self.run_script(answers)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.restores(calls), [])
        self.assertIn("Nothing to go back to", result.stderr)

    def test_the_version_before_counts_if_it_had_any_secret_the_upload_dropped(self):
        # Prod's pull zone id came with this apply: only the probe's token was on before, and going back keeps it on.
        answers = dict(AFTER_UPLOAD, **{"GET /versions/v-before": version("v-before", ["GLOBALPING_TOKEN"]),
                                        "PUT /secrets": REFUSED})
        _, calls = self.run_script(answers)
        self.assertEqual([body["versions"][0]["version_id"] for body in self.restores(calls)], ["v-before"])

    def test_without_the_deployments_the_secrets_still_go_on_and_nothing_is_put_back(self):
        for name, answers in (
                ("deployments unread", {"GET /deployments": REFUSED}),
                ("deployments not json", {"GET /deployments": ("", 7)}),
                ("one deployment", {"GET /deployments": deployments("v-upload")}),
                ("split traffic", {"GET /deployments": cloudflare({"deployments": [
                    {"id": "d-split", "created_on": "2026-11-19T17:01:00Z",
                     "versions": [{"version_id": "v-upload", "percentage": 50},
                                  {"version_id": "v-before", "percentage": 50}]},
                    {"id": "d-before", "created_on": "2026-11-19T16:00:00Z",
                     "versions": [{"version_id": "v-before", "percentage": 100}]}]})}),
                ("no creation times", {"GET /deployments": cloudflare({"deployments": [
                    {"id": f"d-{version}", "versions": [{"version_id": version, "percentage": 100}]}
                    for version in ("v-upload", "v-before")]})}),
                ("version unread", {"GET /versions/v-before": REFUSED})):
            with self.subTest(name):
                result, calls = self.run_script(dict(AFTER_UPLOAD, **answers))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(len([call for call in calls if call["method"] == "PUT"]), 2)
                result, calls = self.run_script(dict(AFTER_UPLOAD, **answers, **{"PUT /secrets": REFUSED}))
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(self.restores(calls), [])

    def test_nothing_is_put_back_when_this_apply_uploaded_nothing(self):
        """An earlier apply uploaded v-upload and could put no secret, nor go back; this one uploads nothing (the
        refresh finds no secrets to drop). v-before served before that earlier apply, not this one: going back to it
        would bring back its older code and deployment key, so a failed PUT leaves v-upload serving."""
        for name, listed in (("the latest from before the plan", deployments("v-upload", "v-before", since_plan=0)),
                             ("two since the plan", deployments("v-upload", "v-before", since_plan=2))):
            with self.subTest(name):
                result, calls = self.run_script(dict(AFTER_UPLOAD, **{"GET /deployments": listed,
                                                                      "PUT /secrets": REFUSED}))
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(self.restores(calls), [])
                self.assertNotIn("goes back", result.stdout)
                self.assertIn("Nothing to go back to", result.stderr)
                # Without the failure the secrets still go on.
                result, calls = self.run_script(dict(AFTER_UPLOAD, **{"GET /deployments": listed}))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(len([call for call in calls if call["method"] == "PUT"]), 2)

    def test_it_goes_back_to_the_latest_deployment_before_the_upload_however_they_are_listed(self):
        # Cloudflare only says the first one listed is the latest; v-before is the one that served until the upload.
        listed = cloudflare({"deployments": [
            {"id": f"d-{version}", "created_on": created_on, "versions": [{"version_id": version, "percentage": 100}]}
            for version, created_on in (("v-upload", "2026-11-19T17:00:01Z"), ("v-older", "2026-11-19T15:00:00.9Z"),
                                        ("v-before", "2026-11-19T16:59:59.000001+00:00"))]})
        _, calls = self.run_script(dict(AFTER_UPLOAD, **{"GET /deployments": listed, "PUT /secrets": REFUSED}))
        self.assertEqual([body["versions"][0]["version_id"] for body in self.restores(calls)], ["v-before"])

    def test_the_apply_passes_the_plan_time(self):
        """PLANNED_AT is the plantimestamp that replaces terraform_data.bunny_secrets on every apply: the plan's time,
        before any upload of the apply it belongs to."""
        workers = read(os.path.join(os.path.dirname(PUT_SECRETS), "workers.tf"))
        block = re.search(r'resource "terraform_data" "bunny_secrets" \{.*?\n\}', workers, re.S).group(0)
        self.assertRegex(block, r"\n  triggers_replace = \[plantimestamp\(\)\]\n")
        self.assertRegex(block, r"\n      PLANNED_AT\s+= self\.triggers_replace\[0\]\n")

    def test_a_refused_restore_says_the_worker_runs_without_them(self):
        answers = dict(AFTER_UPLOAD, **{"PUT /secrets": REFUSED, "POST /deployments?force=true": REFUSED})
        result, calls = self.run_script(answers)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(self.restores(calls)), 1)
        self.assertIn("Could not deploy version v-before of tiles-prod again", result.stderr)
        self.assertIn("runs without the secrets not put", result.stderr)

    def test_missing_settings_fail(self):
        self.assertNotEqual(self.run_script(CLOUDFLARE_API_TOKEN="")[0].returncode, 0)
        self.assertNotEqual(self.run_script(SECRETS="")[0].returncode, 0)
        for planned in ("", "yesterday"):
            with self.subTest(PLANNED_AT=planned):
                result, calls = self.run_script(PLANNED_AT=planned)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(calls, [])



class BunnyKeyCommandLineTest(unittest.TestCase):
    """The Bunny account key never goes on a command line, which any process on the machine can read
    (/proc/<pid>/cmdline): the module's provisioner and the README's commands give it to curl as a header file, as
    put-worker-secrets.sh does with Cloudflare's token."""

    MODULE = os.path.join(ROOT, "deployment", "modules", "bunny", "tiles")

    def test_no_access_key_header_is_expanded_into_an_argument(self):
        for path in sorted(glob.glob(os.path.join(self.MODULE, "*.tf"))) + [os.path.join(self.MODULE, "README.md")]:
            with self.subTest(path=os.path.relpath(path, ROOT)):
                self.assertNotRegex(read(path), r"""-H\s+["']?AccessKey:\s*\$""")

    def test_the_publish_provisioner_sends_the_key_in_a_header_file(self):
        script = read(os.path.join(self.MODULE, "script.tf"))
        block = re.search(r'resource "terraform_data" "publish_gzip" \{.*?\n\}', script, re.S).group(0)
        # Process substitution needs bash: local-exec's default, sh -c, would fail on it.
        self.assertIn('interpreter = ["bash", "-c"]', block)
        command = textwrap.dedent(re.search(r"command\s*=\s*<<-EOT\n(.*?)\n\s*EOT", block, re.S).group(1))
        with tempfile.TemporaryDirectory() as tmp:
            install(os.path.join(tmp, "bin"), {"curl": FAKE_CURL_SECRETS})
            log = os.path.join(tmp, "log")
            env = dict(os.environ, PATH=os.path.join(tmp, "bin") + os.pathsep + os.environ["PATH"], FAKE_LOG=log,
                       FAKE_DIR=tmp, FAKE_PREFIX="https://api.bunny.net", BUNNY_API_KEY="bunny key 2")
            result = subprocess.run(["bash", "-c", command.replace("${self.input}", "1234")], env=env, cwd=tmp,
                                    capture_output=True, text=True, check=False)
            with open(log) as f:
                calls = [json.loads(line) for line in f]
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([call["url"] for call in calls], ["https://api.bunny.net/compute/script/1234/publish"])
        self.assertIn("AccessKey: bunny key 2", calls[0]["headers"])
        self.assertNotIn("bunny key 2", " ".join(calls[0]["args"]))

class BunnyStateTest(unittest.TestCase):
    """The Bunny unit's state holds the storage zones' passwords and the middleware script's deployment key, and its
    backend credential is the one every pull request's plan resolves: it is encrypted with a passphrase in tf_bunny,
    and nothing else reads it."""

    MODULE = os.path.join(ROOT, "deployment", "modules", "bunny", "tiles")

    def test_the_state_and_plans_are_encrypted_with_a_tf_bunny_passphrase(self):
        encryption = read(os.path.join(self.MODULE, "encryption.tf"))
        self.assertIn("passphrase = var.state_passphrase", encryption)
        for block in ("state", "plan"):
            with self.subTest(block=block):
                body = re.search(r"\n    " + block + r" \{\n(.*?)\n    \}", encryption, re.S)
                self.assertIsNotNone(body)
                self.assertIn("method   = method.aes_gcm.state", body.group(1))
                self.assertIn("enforced = true", body.group(1))
        self.assertNotIn('method "unencrypted"', encryption)
        self.assertNotIn("fallback", encryption)
        self.assertIn('export TF_VAR_state_passphrase="op://tf_bunny/BUNNY_TILES_STATE_PASSPHRASE/password"',
                      read(os.path.join(self.MODULE, "bunny.env")))

    def test_no_other_unit_reads_the_bunny_state(self):
        readers = []
        for path in glob.glob(os.path.join(ROOT, "deployment", "**", "*.tf"), recursive=True) + glob.glob(
                os.path.join(ROOT, "deployment", "**", "*.hcl"), recursive=True):
            if "bunny_tiles" in read(path) and os.path.dirname(path) != self.MODULE:
                readers.append(os.path.relpath(path, ROOT))
        self.assertEqual(readers, [])
        self.assertIn('schema_name = "${local.env}_bunny_tiles"', read(os.path.join(self.MODULE, "terragrunt.hcl")))

    def test_the_check_job_runs_the_encryption_test_with_the_tools_of_mise(self):
        """use-mise puts only the shims on PATH, and a shim run outside the checkout finds no tofu version: the
        step runs under mise exec, with REQUIRE_TOFU so that a missing tofu fails it."""
        steps = load("tiles-bunny-infra.yml")["jobs"]["check"]["steps"]
        step = next(step for step in steps if "BunnyStateTest" in step.get("run", ""))
        self.assertRegex(step["run"], r"^mise exec -- python3 -m unittest .*-k BunnyStateTest$")
        self.assertEqual(str(step["env"]["REQUIRE_TOFU"]), "1")

    # Tiles Bunny Infra's check job sets REQUIRE_TOFU, so that there a missing tofu fails instead of skipping.
    @unittest.skipUnless(shutil.which("tofu") or os.environ.get("REQUIRE_TOFU"), "needs tofu (mise)")
    def test_the_state_is_written_encrypted(self):
        """The module's encryption.tf on a local backend: the state file holds ciphertext, which only the passphrase
        reads back."""
        marker = "storage-zone-password-stand-in"
        with tempfile.TemporaryDirectory() as tmp:
            module = os.path.join(tmp, "module")
            os.mkdir(module)
            shutil.copy(os.path.join(self.MODULE, "encryption.tf"), module)
            with open(os.path.join(module, "main.tf"), "w") as f:
                f.write('variable "state_passphrase" {\n  type      = string\n  sensitive = true\n}\n\n'
                        f'resource "terraform_data" "zone" {{\n  input = "{marker}"\n}}\n')
            env = dict(os.environ, TF_DATA_DIR=os.path.join(tmp, "data"), TF_IN_AUTOMATION="1",
                       TF_VAR_state_passphrase="correct-passphrase-for-tests")

            # Run from the checkout, pointed at the module with -chdir: tofu may be only mise's shim (as on a runner,
            # where use-mise puts the shims on PATH), which finds its version in the repo's mise config by the working
            # directory, and the temporary directory is outside the repo.
            def tofu(*args, **overrides):
                command = [*args, "-input=false"] if args[0] != "state" else list(args)
                return subprocess.run(["tofu", f"-chdir={module}", *command], cwd=ROOT, env=dict(env, **overrides),
                                      capture_output=True, text=True, check=False)

            initialised = tofu("init")
            self.assertEqual(initialised.returncode, 0, initialised.stderr)
            applied = tofu("apply", "-auto-approve")
            self.assertEqual(applied.returncode, 0, applied.stderr)
            state = read(os.path.join(module, "terraform.tfstate"))
            self.assertNotIn(marker, state)
            self.assertIn("encrypted_data", json.loads(state))
            self.assertIn(marker, tofu("state", "pull").stdout)
            wrong = tofu("plan", TF_VAR_state_passphrase="another-passphrase-for-tests")
            self.assertNotEqual(wrong.returncode, 0)
            self.assertIn("decryption failed", wrong.stderr + wrong.stdout)


FAKE_CURL_WORKER = """#!/bin/bash
# The worker behind tiles.<env>.immich.cloud serves $FAKE_KEY; tiles.immich.cloud is Bunny, which sends no deployment
# key and whose TileJSON's tile URLs have no ?v=. Logs each URL to $FAKE_LOG.
url=${@: -1}
host=${url#https://}
host=${host%%/*}
echo "$url" >> "$FAKE_LOG"
if [ "$host" = tiles.immich.cloud ]; then key=""; else key=$FAKE_KEY; fi
case " $* " in
  *" -D - "*)
    printf 'HTTP/2 200\\r\\ncontent-type: application/json\\r\\n'
    [ -n "$key" ] && printf 'pmtiles-deployment-key: %s\\r\\n' "$key"
    printf '\\r\\n'
    ;;
  *" -w "*) echo "200" ;;
  *)
    if [ -n "$key" ]; then v="?v=$key"; else v=""; fi
    printf '{"tiles": ["https://%s/v1/{z}/{x}/{y}.mvt%s"]}' "$host" "$v"
    ;;
esac
"""


class WorkerHostnameTest(unittest.TestCase):
    """Tiles Deploy's checks ask the worker on its own hostname: from the cutover on, prod's public one is Bunny's,
    which sends no deployment key, and the rollback runbook dispatches Tiles Deploy before pointing it back."""

    def workspace(self, tmp):
        os.makedirs(os.path.join(tmp, "deployment", "scripts"))
        shutil.copy(os.path.join(ROOT, "deployment", "scripts", "tiles-served-key.sh"),
                    os.path.join(tmp, "deployment", "scripts"))
        install(os.path.join(tmp, "bin"), {"curl": FAKE_CURL_WORKER, "sleep": "#!/bin/sh\n"})
        log = os.path.join(tmp, "log")
        env = dict(os.environ, PATH=os.path.join(tmp, "bin") + os.pathsep + os.environ["PATH"], FAKE_LOG=log,
                   FAKE_KEY="20261118")
        return env, log

    def test_served_key_reads_each_workers_own_hostname(self):
        for environment in ("prod", "dev"):
            with self.subTest(environment=environment), tempfile.TemporaryDirectory() as tmp:
                env, log = self.workspace(tmp)
                result = subprocess.run(["bash", "deployment/scripts/tiles-served-key.sh", environment], cwd=tmp,
                                        env=env, capture_output=True, text=True, check=False)
                self.assertEqual((result.returncode, result.stdout), (0, "20261118\n"), result.stderr)
                self.assertEqual(read(log), f"https://tiles.{environment}.immich.cloud/v1.json\n")

    def test_activation_checks_the_worker_while_the_public_hostname_is_bunnys(self):
        check = step(load("tiles-deploy.yml")["jobs"]["activate"], "Check live endpoint")
        with tempfile.TemporaryDirectory() as tmp:
            env, log = self.workspace(tmp)
            summary = os.path.join(tmp, "summary")
            env.update(ENVIRONMENT="prod", DEPLOYMENT_KEY="20261118", GITHUB_STEP_SUMMARY=summary)
            result = run_bash(check["run"], tmp, env)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            hosts = {line.split("/")[2] for line in read(log).splitlines()}
            self.assertEqual(hosts, {"tiles.prod.immich.cloud"})
            self.assertEqual(read(summary), "- https://tiles.prod.immich.cloud now serves 20261118\n")


class CutoverTlsTest(unittest.TestCase):
    """The pull zone turns on TLS (and force_ssl) only for the names in tls_hostnames, and Bunny requests a managed
    certificate over HTTP, which fails until the name resolves to the zone: the cutover's switch lists
    tiles.immich.cloud in tls_hostnames after its CNAME, and checks HTTPS before the pull request that points the
    sync and the probe at it."""

    def test_the_switch_gets_the_public_hostname_its_certificate_after_the_dns_change(self):
        module = os.path.join(ROOT, "deployment", "modules", "bunny", "tiles")
        pullzone = read(os.path.join(module, "pullzone.tf"))
        self.assertIn("tls_enabled = contains(var.tls_hostnames, each.value)", pullzone)
        readme = read(os.path.join(module, "README.md"))
        cutover = readme.split("### The cutover\n", 1)[1].split("\n### ", 1)[0]
        switch = re.search(r"\n2\. \*\*Switch\*\*.*", cutover, re.S).group(0)
        cname = switch.index("add a DNS-only (not proxied) CNAME `tiles` at `cdn_domain`")
        tls = switch.find("`tiles.immich.cloud` to `tls_hostnames`")
        self.assertGreater(tls, cname, "the switch never lists tiles.immich.cloud in tls_hostnames after its CNAME")
        public_url = switch.index("`BUNNY_PUBLIC_URL`")
        self.assertLess(tls, public_url)
        self.assertIn("curl -fsSI https://tiles.immich.cloud/v1.json", switch[tls:public_url])


if __name__ == "__main__":
    unittest.main()
