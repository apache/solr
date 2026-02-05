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
# AGENTS.md for Apache Solr

While README.md and CONTRIBUTING.md are mainly written for humans, this file is a condensed knowledge base for coding agents on the Solr codebase. For background, see https://agents.md

## Licensing and Dependencies

- Follow Apache Software Foundation licensing rules, avoid adding a dependency with a banned license
- Always apply the Apache License to new source files
- We use gradle version-catalog toml file to encode dependency versions, never add versions directly to build.gradle files
- We use the "cutterslade-analyze" plugin which requires explicit recording of all dependencies in a build.gradle file
- Always run "gradlew updateLicenses resolveAndLockAll --write-locks" after adding or changing a dependency. See dev-docs/gradle-help/dependencies.txt for more info

## Build and Development Workflow

- When done or preparing to commit changes to java source files, be sure to run `gradlew tidy` to format the code
- Always run "gradlew check -x test" before declaring a feature done
- Respect our .editorconfig

## Code Quality and Best Practices

- Use the project's custom EnvUtils to read system properties. It auto converts env.var SOLR_FOO_BAR to system property solr.foo.bar
- If you add slf4j log calls, make sure to wrap in logger.isXxxEnabled() clause. Be careful to not add unnecessary logging
- Validate user input. For file paths, always call myCoreContainer.assertPathAllowed(myPath) before using

## Testing

- When adding new unit tests, avoid using the old TestHarness and consider using EmbeddedSolrServerTestRule
- See dev-docs/gradle-help/tests.txt for hints on running tests

## Documentation

- For major or breaking changes, add a prominent note in reference guide major-changes-in-solr-X.adoc
- Always consider whether a reference-guide page needs updating due to the new/changed features. Target audience is end user
- For changes to build system and other developer-focused changes, consider updating or adding docs in dev-docs/ folder
- Keep all documentation including javadoc concise
- New classes should have some javadocs
- Changes should not have code comments communicating the change, which are instead great comments to leave for code review / commentary

## Changelog

- We use logchange as changelog tool. Add a new file changelog/unreleased/describe-your-feature.yml for each change, see dev-docs/changelog.adoc
