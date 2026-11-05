# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Offline regression tests. No API calls, credentials or checkout of PR code."""

import contextlib
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import urllib.error
from unittest import mock

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location(
    "pr_triage", ROOT / "dev-tools/scripts/pr-triage-llm.py"
)
TRIAGE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(TRIAGE)


def changed_file(name, status="modified", patch="@@ -1 +1 @@\n-old\n+new"):
    return {"filename": name, "status": status, "patch": patch, "additions": 1, "deletions": 1}


PRODUCTION = changed_file("solr/core/src/java/Production.java")


MODEL_ENV = {"LLM_BASE_URL": "https://example.invalid/v1", "LLM_MODEL": "test", "LLM_API_KEY": "fake"}


def load_config():
    return json.loads((ROOT / ".github/pr-triage/judgements.json").read_text(encoding="utf-8"))


class TriageTest(unittest.TestCase):
    def setUp(self):
        self.cfg = {
            "min_confidence": "medium",
            "judgements": [{
                "id": "needs_changelog", "title": "Changelog entry",
                "question": "Does this warrant a release note?",
                "evidence_files": ["changelog/unreleased/**"],
            }],
        }
        self.pr = {
            "title": "Fix search results", "body": "A description.",
            "base": {"ref": "main", "sha": "b" * 40, "repo": {"full_name": "apache/solr"}},
            "head": {"sha": "a" * 40, "repo": {
                "full_name": "contributor/solr", "owner": {"type": "User"},
            }},
            "maintainer_can_modify": True,
        }
        self.answers = {"needs_changelog": {
            "verdict": True, "confidence": "high", "reason": "Changes search results.",
        }}

    def run_main(self, files, answers=None, dry_run=False):
        # Judgeable files are needed for the model to be asked at all.
        files = [PRODUCTION, *files]
        return self.run_main_exact(files, answers, dry_run)

    def run_main_exact(self, files, answers=None, dry_run=False):
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            (temp / "config.json").write_text(json.dumps(self.cfg), encoding="utf-8")
            (temp / "pr.json").write_text(json.dumps(self.pr), encoding="utf-8")
            (temp / "files.json").write_text(json.dumps(files), encoding="utf-8")
            args = ["pr-triage-llm.py", "--config", str(temp / "config.json"),
                    "--pr", str(temp / "pr.json"), "--files", str(temp / "files.json"),
                    "--out", str(temp / "result.json")]
            if dry_run:
                args.append("--dry-run")
            content = json.dumps(self.answers) if answers is None else answers
            output = io.StringIO()
            with mock.patch("sys.argv", args), contextlib.redirect_stdout(output):
                with mock.patch.object(TRIAGE, "call_model", return_value=(content, {})) as model:
                    TRIAGE.main()
            result_path = temp / "result.json"
            result = json.loads(result_path.read_text(encoding="utf-8")) if result_path.exists() else None
            return result, model, output.getvalue()

    def test_deleted_evidence_is_missing(self):
        result, _, _ = self.run_main([changed_file("changelog/unreleased/fix.yml", "removed")])
        row = result["results"]["needs_changelog"]
        self.assertFalse(row["included"])
        self.assertTrue(row["mismatch"])
        self.assertEqual(1, result["mismatches"])
        self.assertIn("missing", result["comment"])

    def test_present_evidence_statuses(self):
        for status in ("added", "modified", "renamed", "copied"):
            with self.subTest(status=status):
                result, _, _ = self.run_main([changed_file("changelog/unreleased/fix.yml", status)])
                self.assertTrue(result["results"]["needs_changelog"]["included"])
                self.assertEqual(0, result["mismatches"])

    def test_missing_answer_is_unknown(self):
        result, _, _ = self.run_main([], '{"unrelated": {}}')
        row = result["results"]["needs_changelog"]
        self.assertIsNone(row["warranted"])
        self.assertIsNone(row["mismatch"])
        self.assertIn("unknown", result["comment"])
        self.assertIn("human review", result["comment"])

    def test_invalid_answer_fields_are_unknown(self):
        for patch in ({"verdict": "false"}, {"verdict": 0}, {"confidence": "certain"},
                      {"confidence": []}, {"reason": []}, {"reason": "  "}):
            with self.subTest(patch=patch):
                answer = dict(self.answers["needs_changelog"], **patch)
                result, _, _ = self.run_main([], json.dumps({"needs_changelog": answer}))
                self.assertIsNone(result["results"]["needs_changelog"]["warranted"])

    def test_malformed_answer_is_unknown(self):
        for content in ('{"needs_changelog":true}', '{"needs_changelog":{}}'):
            with self.subTest(content=content):
                result, _, _ = self.run_main([], content)
                self.assertIsNone(result["results"]["needs_changelog"]["warranted"])

    def test_unusable_reply_posts_nothing(self):
        for content in ("not JSON", "[]", "null", "{}", '{"needs_changelog":{},"needs_changelog":{}}'):
            with self.subTest(content=content):
                result, _, output = self.run_main([], content)
                self.assertIsNone(result)
                self.assertIn("::warning::", output)

    def test_unreachable_model_posts_nothing(self):
        failures = (urllib.error.URLError("down"), TimeoutError(), ValueError("bad json"))
        for failure in failures:
            with self.subTest(failure=failure), mock.patch.dict("os.environ", MODEL_ENV):
                with mock.patch.object(TRIAGE.urllib.request, "urlopen", side_effect=failure):
                    with self.assertRaises(TRIAGE.NoAnswer):
                        TRIAGE.call_model([], {})

    def test_error_shaped_or_truncated_response_is_no_answer(self):
        for body in ({"error": "overloaded"}, {"choices": []},
                     {"choices": [{"message": {"content": "{"}, "finish_reason": "length"}]},
                     {"choices": [{"message": {"content": " "}}]}):
            with self.subTest(body=body), mock.patch.dict("os.environ", MODEL_ENV):
                with mock.patch.object(TRIAGE.urllib.request, "urlopen", return_value=io.StringIO(json.dumps(body))):
                    with self.assertRaises(TRIAGE.NoAnswer):
                        TRIAGE.call_model([], {})

    def test_only_evidence_files_changed_posts_nothing(self):
        self.cfg["judgements"][0]["evidence_files"] += ["docs/**"]
        files = [changed_file("changelog/unreleased/fix.yml"), changed_file("docs/guide.adoc")]
        result, model, _ = self.run_main_exact(files)
        self.assertIsNone(result)
        model.assert_not_called()

    def test_comment_does_not_emit_active_model_markup(self):
        self.answers["needs_changelog"]["reason"] = (
            '<img src="x"> [click](https://example.org) ![image](x) '
            '@someone @org/team `code` **bold** | cell\nnew\rline &lt;b&gt;'
        )
        result, _, _ = self.run_main([])
        reason = result["results"]["needs_changelog"]["reason"]
        for active in ("<img", "[click]", "![image]", "@someone", "@org/team", "`", "**", "|", "\n", "\r", "https://"):
            self.assertNotIn(active, reason)
        self.assertIn("&#60;img", reason)
        self.assertIn("&#64;\u200bsomeone", reason)
        self.assertIn("&#124;", reason)
        self.assertIn(reason, result["comment"])

    def test_reason_is_bounded_and_cannot_reintroduce_entities(self):
        self.answers["needs_changelog"]["reason"] = "&lt;script&gt; #123 www.example.org\u202e" + "x" * 1000
        result, _, _ = self.run_main([])
        reason = result["results"]["needs_changelog"]["reason"]
        self.assertIn("&#38;lt", reason)
        self.assertIn("&#35;\u200b123", reason)
        self.assertNotIn("www.example.org", reason)
        self.assertNotIn("\u202e", reason)
        self.assertLess(len(reason), 1000)

    def test_fenced_json_is_accepted(self):
        content = "```json\n" + json.dumps(self.answers) + "\n```"
        result, _, _ = self.run_main([], content)
        self.assertTrue(result["results"]["needs_changelog"]["warranted"])

    def test_unknown_answer_does_not_suppress_valid_answers(self):
        self.cfg["judgements"].append({"id": "other", "title": "Other", "question": "Other?"})
        result, _, _ = self.run_main([])
        self.assertEqual(1, result["unanswered"])
        self.assertEqual(1, result["mismatches"])
        self.assertTrue(result["results"]["needs_changelog"]["warranted"])

    def test_reverse_mismatch_and_low_confidence(self):
        answer = self.answers["needs_changelog"]
        answer.update(verdict=False, confidence="low")
        result, _, _ = self.run_main([changed_file("changelog/unreleased/fix.yml")])
        self.assertTrue(result["results"]["needs_changelog"]["mismatch"])
        self.assertEqual(0, result["mismatches"])
        self.assertIn("❔", result["comment"])
        self.assertIn("probably not needed", result["comment"])

    def test_non_main_notice_precedes_model_table(self):
        self.pr["base"]["ref"] = "branch_9x"
        result, _, _ = self.run_main([])
        self.assertIn("Prefer PRs against `main`", result["comment"])
        self.assertLess(result["comment"].index("Prefer PRs"), result["comment"].index("Warranted?"))

    def test_main_does_not_warn(self):
        result, _, _ = self.run_main([])
        self.assertNotIn("Prefer PRs", result["comment"])

    def test_personal_fork_edits_disabled(self):
        self.pr["maintainer_can_modify"] = False
        result, _, _ = self.run_main([])
        self.assertIn("Consider enabling", result["comment"])
        self.assertIn("(optional)", result["comment"])

    def test_access_notice_exceptions(self):
        for case in ("allowed", "same_repo", "org", "missing_repo", "unknown"):
            with self.subTest(case=case):
                pr = copy.deepcopy(self.pr)
                pr["maintainer_can_modify"] = False
                if case == "allowed":
                    pr["maintainer_can_modify"] = True
                elif case == "same_repo":
                    pr["head"]["repo"]["full_name"] = "apache/solr"
                elif case == "org":
                    pr["head"]["repo"]["owner"]["type"] = "Organization"
                elif case == "missing_repo":
                    pr["head"]["repo"] = None
                else:
                    pr.pop("maintainer_can_modify")
                comment = "\n".join(TRIAGE.metadata_notices(pr))
                self.assertNotIn("Consider enabling", comment)
                if case == "org":
                    self.assertIn("unavailable", comment)

    def test_metadata_is_escaped(self):
        self.pr["base"]["ref"] = "branch/@someone/[click]"
        result, _, _ = self.run_main([])
        self.assertNotIn("@someone", result["comment"])
        self.assertNotIn("[click]", result["comment"])

    def test_hidden_evidence_is_absent_from_prompt(self):
        files = [changed_file("changelog/unreleased/secret.yml", patch="HIDDEN EVIDENCE"),
                 changed_file("solr/core/src/java/Search.java", patch="PRODUCTION CHANGE")]
        _, model, _ = self.run_main(files)
        messages = model.call_args[0][0]
        self.assertNotIn("HIDDEN EVIDENCE", str(messages))
        self.assertNotIn("secret.yml", str(messages))
        self.assertIn("PRODUCTION CHANGE", str(messages))

    def test_dry_run_never_calls_model(self):
        result, model, output = self.run_main([], dry_run=True)
        model.assert_not_called()
        self.assertIsNone(result)
        self.assertIn("Does this warrant", output)

    def test_incomplete_files_refused_before_model(self):
        self.pr["changed_files"] = 3001
        result, model, output = self.run_main([])
        self.assertIsNone(result)
        model.assert_not_called()
        self.assertIn("Incomplete changed-file list", output)

    def test_skip_when_included_does_not_ask_the_model(self):
        self.cfg["judgements"].append({
            "id": "needs_jira", "title": "Jira", "question": "Jira?",
            "evidence_title": r"\bSOLR-[1-9][0-9]*\b", "skip_when_included": True})
        self.pr["title"] = "SOLR-123: fix"
        result, model, _ = self.run_main([])
        self.assertNotIn("needs_jira", str(model.call_args[0][0]))
        self.assertEqual(0, result["unanswered"])
        self.assertIn("Already present; not judged.", result["comment"])
        self.pr["title"] = "fix"
        result, model, _ = self.run_main([])
        self.assertIn("needs_jira", str(model.call_args[0][0]))
        self.assertEqual(1, result["unanswered"])

    def test_all_satisfied_skips_the_model(self):
        self.cfg["judgements"] = [{
            "id": "needs_jira", "title": "Jira", "question": "Jira?",
            "evidence_title": r"\bSOLR-[1-9][0-9]*\b", "skip_when_included": True}]
        self.pr["title"] = "SOLR-123: fix"
        result, model, _ = self.run_main([])
        self.assertIsNone(result)
        model.assert_not_called()

    def changelog_type(self, suggested, entry_type=None):
        self.cfg["judgements"] = [j for j in load_config()["judgements"] if j["id"] == "needs_changelog"]
        self.answers = {"needs_changelog": {
            "verdict": suggested, "confidence": "high", "reason": "Because."}}
        files = []
        if entry_type:
            patch = f"@@ -0,0 +1,3 @@\n+title: x\n+type: {entry_type} # added, changed, fixed\n+authors:"
            files.append(changed_file("changelog/unreleased/x.yml", "added", patch))
        result, model, _ = self.run_main(files)
        return result, model

    def test_changelog_type_differing_from_entry_is_highlighted(self):
        result, model = self.changelog_type("added", "fixed")
        row = result["results"]["needs_changelog"]
        self.assertTrue(row["mismatch"])
        self.assertEqual(1, result["mismatches"])
        self.assertIn("differs from the entry", result["comment"])
        self.assertIn("| added | fixed |", result["comment"])
        prompt = str(model.call_args[0][0])
        self.assertIn("exactly one of: added, changed, fixed", prompt)
        self.assertNotIn("type: fixed", prompt)

    def test_changelog_type_agreeing_with_entry(self):
        result, _ = self.changelog_type("fixed", "fixed")
        self.assertFalse(result["results"]["needs_changelog"]["mismatch"])
        self.assertEqual(0, result["mismatches"])

    def test_changelog_type_without_entry_is_missing(self):
        result, _ = self.changelog_type("changed")
        self.assertTrue(result["results"]["needs_changelog"]["mismatch"])
        self.assertIn("| changed | no entry |", result["comment"])
        self.assertIn("missing", result["comment"])

    def test_changelog_not_worthy(self):
        result, _ = self.changelog_type("(not-worthy)")
        self.assertFalse(result["results"]["needs_changelog"]["mismatch"])
        self.assertEqual(0, result["mismatches"])
        result, _ = self.changelog_type("(not-worthy)", "other")
        self.assertTrue(result["results"]["needs_changelog"]["mismatch"])
        self.assertIn("probably not needed", result["comment"])

    def test_changelog_type_outside_choices_is_unknown(self):
        result, _ = self.changelog_type("bugfix", "fixed")
        self.assertEqual(1, result["unanswered"])
        self.assertIsNone(result["results"]["needs_changelog"]["warranted"])

    def test_changelog_title_is_input_to_every_judgement(self):
        self.cfg["summary_from"] = load_config()["summary_from"]
        patch = ("@@ -0,0 +1,4 @@\n+title: Removed the foo handler.\n+  Use bar instead.\n"
                 "+type: removed\n+authors:")
        _, model, _ = self.run_main([changed_file("changelog/unreleased/x.yml", "added", patch)])
        prompt = str(model.call_args[0][0])
        self.assertIn("Changelog entry title: Removed the foo handler. Use bar instead.", prompt)
        self.assertNotIn("type: removed", prompt)

    def test_changelog_title_block_scalar_marker_is_dropped(self):
        self.cfg["summary_from"] = load_config()["summary_from"]
        patch = "@@ -0,0 +1,3 @@\n+title: >\n+  Fold this\n+  text.\n+type: fixed"
        _, model, _ = self.run_main([changed_file("changelog/unreleased/x.yml", "added", patch)])
        self.assertIn("Changelog entry title: Fold this text.", str(model.call_args[0][0]))

    def test_changelog_title_on_following_lines(self):
        self.cfg["summary_from"] = load_config()["summary_from"]
        patch = "@@ -0,0 +1,4 @@\n+title:\n+  First part\n+  second part.\n+type: fixed"
        _, model, _ = self.run_main([changed_file("changelog/unreleased/x.yml", "added", patch)])
        self.assertIn("Changelog entry title: First part second part.", str(model.call_args[0][0]))

    def test_no_changelog_title_without_entry(self):
        self.cfg["summary_from"] = load_config()["summary_from"]
        _, model, _ = self.run_main([])
        self.assertNotIn("Changelog entry title:", str(model.call_args[0][0]))

    def test_exact_revision_is_reported(self):
        result, _, _ = self.run_main([])
        self.assertIn("Assessed commit `" + "a" * 40 + "`", result["comment"])

    def test_configured_jira_evidence(self):
        cfg = load_config()
        j = next(j for j in cfg["judgements"] if j["id"] == "needs_jira")
        for title, expected in (("SOLR-12345: fix", True), ("solr-12345: fix", False),
                                ("SOLR-XXXXX", False), ("SOLR-0 fix", False), ("small cleanup", False)):
            with self.subTest(title=title):
                self.assertEqual(expected, TRIAGE.evidence_included({"title": title}, [], j))

    def test_configured_evidence_families(self):
        cfg = load_config()
        samples = {
            "needs_ref_guide": "solr/solr-ref-guide/modules/query-guide/pages/search.adoc",
            "needs_upgrade_notes": "solr/solr-ref-guide/modules/upgrade-notes/pages/changes.adoc",
            "needs_tests": "solr/core/src/test/org/apache/solr/SearchTest.java",
        }
        for j in cfg["judgements"]:
            if j["id"] in samples:
                with self.subTest(judgement=j["id"]):
                    f = changed_file(samples[j["id"]])
                    self.assertTrue(TRIAGE.evidence_included({}, [f], j))
                    f["status"] = "removed"
                    self.assertFalse(TRIAGE.evidence_included({}, [f], j))
        guide = next(j for j in cfg["judgements"] if j["id"] == "needs_ref_guide")
        self.assertFalse(TRIAGE.evidence_included({}, [changed_file(samples["needs_upgrade_notes"])], guide))
        tests = next(j for j in cfg["judgements"] if j["id"] == "needs_tests")
        for name in ("ui/shared/src/commonTest/kotlin/Test.kt", "dev-tools/scripts/tests/test_pr_triage.py"):
            self.assertTrue(TRIAGE.evidence_included({}, [changed_file(name)], tests))

    def test_diff_budget_prioritises_production_and_excludes_patches(self):
        java = changed_file("solr/core/src/java/Search.java", patch="JAVA")
        test = changed_file("README.md", patch="TEST")
        lock = changed_file("ui/gradle.lockfile", patch="LOCK")
        binary = changed_file("image.png", patch=None)
        budget = len("--- solr/core/src/java/Search.java (modified)\nJAVA\n")
        diff, omitted = TRIAGE.build_diff([test, lock, binary, java], {
            "max_diff_chars": budget, "exclude_patches": ["**/*.lockfile"],
        })
        self.assertIn("JAVA", diff)
        self.assertNotIn("TEST", diff)
        self.assertCountEqual([test["filename"], lock["filename"], binary["filename"]], omitted)
        self.assertLessEqual(len(diff), budget)

    def test_model_request_is_mocked_at_http_boundary(self):
        response = io.StringIO(json.dumps({"choices": [{"message": {"content": "{}"}}]}))
        with mock.patch.dict("os.environ", MODEL_ENV), mock.patch.object(TRIAGE.urllib.request, "urlopen", return_value=response) as http:
            content, _ = TRIAGE.call_model([], {})
        self.assertEqual("{}", content)
        request = http.call_args[0][0]
        self.assertEqual("https://example.invalid/v1/chat/completions", request.full_url)
        self.assertEqual({"type": "json_object"}, json.loads(request.data)["response_format"])

    def test_response_header_says_checked_once(self):
        result, _, _ = self.run_main([])
        self.assertIn("not updated on later pushes", result["comment"])

    def test_verdicts_file_replaces_model(self):
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            (temp / "config.json").write_text(json.dumps(self.cfg), encoding="utf-8")
            (temp / "v.json").write_text(json.dumps(self.answers), encoding="utf-8")
            args = ["pr-triage-llm.py", "--config", str(temp / "config.json"),
                    "--verdicts", str(temp / "v.json"), "--out", str(temp / "out.json"),
                    "--from-git", "main"]
            with mock.patch("sys.argv", args), contextlib.redirect_stdout(io.StringIO()):
                with mock.patch.object(TRIAGE, "pr_from_git", return_value=(self.pr, [PRODUCTION])):
                    with mock.patch.object(TRIAGE, "call_model") as model:
                        TRIAGE.main()
            model.assert_not_called()
            result = json.loads((temp / "out.json").read_text(encoding="utf-8"))
            self.assertTrue(result["results"]["needs_changelog"]["warranted"])


class FromGitTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.repo = Path(self.directory.name)
        self.git("init", "-q", "-b", "main")
        self.write("src/Old.java", "class Old {}\n")
        self.write("src/Gone.java", "class Gone {}\n")
        self.commit("base")

    def git(self, *args):
        env = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@e", GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@e")
        subprocess.run(["git", *args], cwd=self.repo, env=env, check=True, capture_output=True)

    def write(self, name, text):
        path = self.repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def commit(self, message):
        self.git("add", "-A")
        self.git("commit", "-q", "-m", message)

    def triage_input(self):
        cwd = os.getcwd()
        os.chdir(self.repo)
        try:
            return TRIAGE.pr_from_git("main")
        finally:
            os.chdir(cwd)

    def test_commits_staged_unstaged_and_untracked(self):
        self.git("switch", "-q", "-c", "feature")
        self.write("src/Old.java", "class Old { int x; }\n")
        self.commit("SOLR-1: change Old\n\nBecause reasons.")
        self.git("rm", "-q", "src/Gone.java")
        self.write("src/Staged.java", "class Staged {}\n")
        self.git("add", "src/Staged.java")
        self.write("changelog/unreleased/fix.yml", "title: x\n")
        pr, files = self.triage_input()
        self.assertEqual("SOLR-1: change Old", pr["title"])
        self.assertIn("Because reasons.", pr["body"])
        self.assertEqual(
            {"src/Old.java": "modified", "src/Gone.java": "removed",
             "src/Staged.java": "added", "changelog/unreleased/fix.yml": "added"},
            {f["filename"]: f["status"] for f in files})
        old = next(f for f in files if f["filename"] == "src/Old.java")
        self.assertTrue(old["patch"].startswith("@@"))
        self.assertEqual(1, old["additions"])

    def test_detects_the_branch_it_stemmed_from(self):
        self.git("switch", "-q", "-c", "branch_10x")
        self.write("src/Ten.java", "class Ten {}\n")
        self.commit("on 10x")
        self.git("switch", "-q", "-c", "feature")
        self.write("src/Feature.java", "class Feature {}\n")
        self.commit("feature work")
        self.git("switch", "-q", "main")
        self.write("src/Main.java", "class Main {}\n")
        self.commit("main moves on")
        self.git("switch", "-q", "feature")
        cwd = os.getcwd()
        os.chdir(self.repo)
        try:
            self.assertEqual("branch_10x", TRIAGE.detect_base())
            self.git("switch", "-q", "-c", "from-main", "main")
            self.assertEqual("main", TRIAGE.detect_base())
        finally:
            os.chdir(cwd)

    def test_no_commits_uses_branch_name(self):
        self.git("switch", "-q", "-c", "my-branch")
        self.write("src/New.java", "class New {}\n")
        pr, files = self.triage_input()
        self.assertEqual("my-branch", pr["title"])
        self.assertEqual(["src/New.java"], [f["filename"] for f in files])


if __name__ == "__main__":
    unittest.main()
