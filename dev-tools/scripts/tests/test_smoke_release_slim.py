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

"""Offline checks for slim and full release smoke paths."""

import io
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import smokeTestRelease as smoke


class ReleaseSmokeTest(unittest.TestCase):
    def setUp(self):
        self.previous_cwd = os.getcwd()
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(os.chdir, self.previous_cwd)
        self.java = types.SimpleNamespace(version="21", java_home="/fake-java")
        self.notice = mock.patch.object(smoke, "SOLR_NOTICE", "fixture")
        self.license = mock.patch.object(smoke, "SOLR_LICENSE", "fixture")
        self.notice.start()
        self.license.start()
        self.addCleanup(self.notice.stop)
        self.addCleanup(self.license.stop)

    def layout(self, name, full, example):
        root = Path(self.temp.name) / name
        root.mkdir()
        for filename in ("LICENSE.txt", "NOTICE.txt", "README.txt", "CHANGELOG.md"):
            (root / filename).write_text("fixture", encoding="utf-8")
        dirs = ["bin", "docker", "licenses", "server", "lib"]
        if full:
            dirs += ["modules", "cross-dc-manager"]
        if example:
            dirs.append("example")
        for name in dirs:
            (root / name).mkdir()
        return root

    def verify_layout(self, root, artifact):
        with mock.patch.object(smoke, "checkAllJARs"), mock.patch.object(
            smoke, "testChangelogMd"
        ), mock.patch.object(smoke, "testSolrSlim") as slim, mock.patch.object(
            smoke, "testSolrExample"
        ) as full:
            smoke.verifyBinaryUnpacked(self.java, artifact, str(root), "11.0.0", "skip")
            return slim.call_count, full.call_count

    def test_release_layouts_and_dispatch(self):
        slim = self.layout("slim", full=False, example=False)
        full = self.layout("full", full=True, example=True)
        unexpected = self.layout("unexpected", full=False, example=True)
        missing = self.layout("missing", full=True, example=False)
        self.assertEqual((1, 0), self.verify_layout(slim, "solr-11.0.0-slim.tgz"))
        self.assertEqual((0, 1), self.verify_layout(full, "solr-11.0.0.tgz"))
        with self.assertRaisesRegex(RuntimeError, "unexpected files/dirs"):
            self.verify_layout(unexpected, "solr-11.0.0-slim.tgz")
        with self.assertRaisesRegex(RuntimeError, 'file "example" is missing'):
            self.verify_layout(missing, "solr-11.0.0.tgz")

    def test_slim_starts_indexes_queries_and_stops(self):
        responses = [
            {"responseHeader": {"status": 0}},
            {"response": {"numFound": 1, "docs": [{"id": "slim-smoke"}]}},
            {"responseHeader": {"status": 0}},
        ]
        bodies = [io.BytesIO(json.dumps(value).encode("utf-8")) for value in responses]
        with mock.patch.object(smoke, "cygwin", False), mock.patch.object(
            smoke, "find_available_port", return_value=8983
        ), mock.patch.object(smoke.subprocess, "call", return_value=0) as command, mock.patch.object(
            smoke.urllib.request, "urlopen", side_effect=bodies
        ) as urlopen:
            smoke.testSolrSlim(self.temp.name, "/fake-java")

        commands = [call.args[0] for call in command.call_args_list]
        self.assertEqual(["bin/solr", "start", "--user-managed", "-p", "8983"], commands[0])
        self.assertEqual(
            ["bin/solr", "create", "-c", "smoke", "--solr-url", "http://localhost:8983/solr"],
            commands[1],
        )
        self.assertEqual(["bin/solr", "stop", "-p", "8983"], commands[2])
        update = urlopen.call_args_list[0].args[0]
        self.assertEqual([{"id": "slim-smoke"}], json.loads(update.data))
        self.assertIn("/smoke/update?commit=true", update.full_url)
        self.assertIn("/smoke/select?", urlopen.call_args_list[1].args[0])
        self.assertIn("/api/cores?", urlopen.call_args_list[2].args[0])

    def test_slim_stops_after_create_failure(self):
        with mock.patch.object(smoke, "cygwin", False), mock.patch.object(
            smoke, "find_available_port", return_value=8983
        ), mock.patch.object(smoke.subprocess, "call", side_effect=[0, 1, 0]) as command:
            with self.assertRaisesRegex(RuntimeError, "Failed to create"):
                smoke.testSolrSlim(self.temp.name, "/fake-java")
        self.assertEqual(["bin/solr", "stop", "-p", "8983"], command.call_args_list[-1].args[0])

    def test_slim_stops_after_query_failure(self):
        responses = [
            io.BytesIO(b'{"responseHeader":{"status":0}}'),
            io.BytesIO(b'{"response":{"numFound":0,"docs":[]}}'),
        ]
        with mock.patch.object(smoke, "cygwin", False), mock.patch.object(
            smoke, "find_available_port", return_value=8983
        ), mock.patch.object(smoke.subprocess, "call", return_value=0) as command, mock.patch.object(
            smoke.urllib.request, "urlopen", side_effect=responses
        ):
            with self.assertRaisesRegex(RuntimeError, "Failed to query"):
                smoke.testSolrSlim(self.temp.name, "/fake-java")
        self.assertEqual(["bin/solr", "stop", "-p", "8983"], command.call_args_list[-1].args[0])

    def test_slim_does_not_stop_an_unstarted_server(self):
        with mock.patch.object(smoke, "cygwin", False), mock.patch.object(
            smoke, "find_available_port", return_value=8983
        ), mock.patch.object(smoke.subprocess, "call", return_value=1) as command:
            with self.assertRaisesRegex(RuntimeError, "Failed to start"):
                smoke.testSolrSlim(self.temp.name, "/fake-java")
        self.assertEqual(1, command.call_count)

    def test_slim_ignores_inherited_solr_configuration(self):
        inherited = {
            "SOLR_HOME": "/existing/home",
            "SOLR_DATA_HOME": "/existing/data",
            "SOLR_INCLUDE": "/existing/solr.in.sh",
            "SOLR_JAVA_HOME": "/other-java",
            "SOLR_CONNECTION": "https://existing.example/solr",
            "ZK_HOST": "existing.example:2181",
            "STOP_PORT": "7983",
            "JAVA_TOOL_OPTIONS": "-Dsolr.solr.home=/existing/home",
        }
        with mock.patch.dict(os.environ, inherited), mock.patch.object(
            smoke, "cygwin", False
        ), mock.patch.object(smoke, "find_available_port", return_value=8983), mock.patch.object(
            smoke.subprocess, "call", return_value=1
        ) as command:
            with self.assertRaisesRegex(RuntimeError, "Failed to start"):
                smoke.testSolrSlim(self.temp.name, "/fake-java")

        env = command.call_args.kwargs["env"]
        self.assertEqual(os.path.join(self.temp.name, "server", "solr"), env["SOLR_HOME"])
        self.assertEqual(os.path.join(self.temp.name, "bin", "solr.in.sh"), env["SOLR_INCLUDE"])
        self.assertEqual(os.path.join(self.temp.name, "bin"), env["SOLR_PID_DIR"])
        self.assertEqual("/fake-java", env["JAVA_HOME"])
        for name in ("SOLR_DATA_HOME", "SOLR_JAVA_HOME", "SOLR_CONNECTION", "ZK_HOST",
                     "STOP_PORT", "JAVA_TOOL_OPTIONS"):
            self.assertNotIn(name, env)

    def test_cygwin_uses_windows_paths_for_native_launcher(self):
        def native_path(args, **kwargs):
            self.assertEqual(["cygpath", "-w"], args[:2])
            return "C:\\converted\\" + os.path.basename(args[2]) + "\n"

        with mock.patch.object(smoke, "cygwin", True), mock.patch.object(
            smoke, "find_available_port", return_value=8983
        ), mock.patch.object(smoke.subprocess, "check_output", side_effect=native_path), mock.patch.object(
            smoke.subprocess, "call", return_value=1
        ) as command:
            with self.assertRaisesRegex(RuntimeError, "Failed to start"):
                smoke.testSolrSlim(self.temp.name, "/fake-java")
        env = command.call_args.kwargs["env"]
        self.assertEqual("C:\\converted\\fake-java", env["JAVA_HOME"])
        self.assertEqual("C:\\converted\\solr.in.cmd", env["SOLR_INCLUDE"])
        self.assertIn("bin/solr.cmd start --user-managed", command.call_args.args[0])

    def test_port_probe_checks_http_stop_and_zookeeper_ports(self):
        sockets = [mock.MagicMock() for _ in range(3)]
        for sock in sockets:
            sock.__enter__.return_value = sock
        with mock.patch.object(smoke.random, "randint", return_value=8983), mock.patch.object(
            smoke.socket, "socket", side_effect=sockets
        ):
            self.assertEqual(8983, smoke.find_available_port())
        self.assertEqual([8983, 9983, 7983], [sock.bind.call_args.args[0][1] for sock in sockets])

    def test_port_probe_retries_when_stop_port_is_busy(self):
        sockets = [mock.MagicMock() for _ in range(6)]
        for sock in sockets:
            sock.__enter__.return_value = sock
        sockets[2].bind.side_effect = OSError("stop port occupied")
        with mock.patch.object(smoke.random, "randint", side_effect=[8983, 8984]), mock.patch.object(
            smoke.socket, "socket", side_effect=sockets
        ):
            self.assertEqual(8984, smoke.find_available_port(max_attempts=2))


if __name__ == "__main__":
    unittest.main()
