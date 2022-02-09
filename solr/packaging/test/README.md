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

Rough draft version - install BATS using their guide. We require the following BATS modules:
* bats-core
* bats-support
* bats-assert

From the project root, run `./gradlew assemble` - this will build all of the jars and assemble the binary distribution.

Run `bats solr/packaging/test` to run the whole suite, or refer to a single file to run only that test suite. Refer to the BATS documentation for additional flags and debugging advice.

## Writing Tests

Our tests should all `load bats_helper` which provides a `common_setup` function that test files can
call from their own `setup()` function.

Some tests will start clusters or create collections, please take care to delete any resources that you create. They will not be cleaned for you automatically.

It is recommended that you install and run `shellcheck` to verify your test scripts and catch common mistakes before committing your changes.

## Limitations

1. Currently this test suite is only available for \*nix environments. Although, newer
   versions of Windows may be able to run these scripts.
2. Tests written in bash are both slow, and harder to maintain than traditional
   JUnit tests.  If a test _can_ be written as a JUnit test, it should be.  This
   suite should only be used to test things that cannot be tested by JUnit.
