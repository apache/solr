<!--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 -->

# bin/solr Tests

This directory contains tests for the `bin/solr` command-line scripts.

All tests in this project use the [BATS](https://bats-core.readthedocs.io/en/stable/index.html) framework.

## Running Tests

Tests can be run via `./gradlew integrationTests` or using gradle abbreviation `./gradlew iTest`.
 This will download libraries to your `.gradle` directory, assemble the binary distribution,
 and run the full test suite.

Individual test files can be selected by specifying the `--tests [test_file.bats]` option.
 The `--tests` option may be repeated to select multiple test files to run.
 Wildcarding or specifying individual test methods is currently not supported.

Tests run Solr on a randomly assigned port number, so these tests should be able to run even if you have Solr
running on the host already. To force tests to start Solr on a specific port, set the `bats.port` gradle
property or system property, e.g. `./gradlew integrationTests -Pbats.port=8983`.

## Writing Tests

Our tests should all `load bats_helper` which provides a `common_setup`
function that test files can call from their own `setup()` function.

Test are defined as `@test "description of the test" { ... }`
 with statements in the function body. Test names can include
 letters, numbers, and spaces. They cannot include special
 characters like dashes or underscores, so JIRA issue numbers
 and command line flags are not valid test names. For more detail
 about BATS features, please consult the documentation.

Some tests will start clusters or create collections,
 please take care to delete any resources that you create.
 They will not be cleaned for you automatically.
 The example `test_bats.bats` shows how to properly do setup and teardown,
 along with debug advice and explanations of some subtle traps to avoid.

It is recommended that you install and run `shellcheck` to verify your test scripts and catch common mistakes before committing your changes.

## Limitations

1. Currently this test suite is only available for \*nix environments. Although, newer
   versions of Windows may be able to run these scripts.
2. Tests written in bash are both slow, and harder to maintain than traditional
   JUnit tests.  If a test _can_ be written as a JUnit test, it should be.  This
   suite should only be used to test things that cannot be tested by JUnit.
