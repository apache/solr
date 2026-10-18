#!/usr/bin/env python3
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

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from changes2html import write_changes_html


class ChangesToHtmlTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.changelog = self.root / 'CHANGELOG.md'
        self.changelog.write_text(
            '[11.0.0] - 2026-10-18\n\n'
            '### Added (1 change)\n\n'
            '- [SOLR-123](https://issues.apache.org/jira/browse/SOLR-123) '
            'A sample release note (Contributor)\n',
            encoding='utf-8',
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_writes_html_and_release_stylesheets(self):
        output_dir = self.root / 'release' / 'changes'

        html_file = write_changes_html(self.changelog, output_dir)

        self.assertEqual(output_dir / 'Changes.html', html_file)
        html = html_file.read_text(encoding='utf-8')
        self.assertIn('Apache Solr Release Notes', html)
        self.assertIn('Release 11.0.0', html)
        self.assertIn('SOLR-123', html)
        for stylesheet in (
            'ChangesFancyStyle.css',
            'ChangesSimpleStyle.css',
            'ChangesFixedWidthStyle.css',
        ):
            self.assertTrue((output_dir / stylesheet).is_file(), stylesheet)

    def test_missing_changelog_fails(self):
        with self.assertRaises(FileNotFoundError):
            write_changes_html(self.root / 'missing.md', self.root / 'release')

    def test_cli_without_output_dir_writes_to_stdout(self):
        script = Path(__file__).resolve().parent / 'changes2html.py'

        result = subprocess.run(
            [sys.executable, str(script), str(self.changelog)],
            capture_output=True,
            text=True,
            cwd=self.root,
            check=False,
        )

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn('Apache Solr Release Notes', result.stdout)
        self.assertIn('SOLR-123', result.stdout)
        created = sorted(path.name for path in self.root.iterdir())
        self.assertEqual(['CHANGELOG.md'], created)

if __name__ == '__main__':
    unittest.main()

    unittest.main()
