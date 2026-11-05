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
from pathlib import Path
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
            "needs_changelog": "changelog/unreleased/fix.yml",
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


if __name__ == "__main__":
    unittest.main()
