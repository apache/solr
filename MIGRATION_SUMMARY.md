// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
# SolrJettyTestBase Migration Summary

## Overview
This document summarizes the migration effort to move tests away from the deprecated `SolrJettyTestBase` class to the modern `SolrJettyTestRule` JUnit test rule.

## Status
- **Total Tests to Migrate**: 30
- **Successfully Migrated**: 3 ✅
- **In Progress/Attempted**: 0
- **Not Yet Migrated**: 27
- **Success Rate So Far**: 10% (3/30)

## Successfully Migrated Tests (Verified with Running Tests)

### 1. TestSolrCoreProperties
- **File**: `solr/core/src/test/org/apache/solr/TestSolrCoreProperties.java`
- **Status**: ✅ PASSED
- **Approach**: Simple migration - changed extends from SolrJettyTestBase to SolrTestCaseJ4, added ClassRule, replaced getSolrClient() with solrClientTestRule.getSolrClient()
- **Test Command**: `./gradlew ":solr:core:test" "--tests" "org.apache.solr.TestSolrCoreProperties.testSimple"`

### 2. TestBatchUpdate
- **File**: `solr/solrj/src/test/org/apache/solr/client/solrj/TestBatchUpdate.java`
- **Status**: ✅ PASSED (3 tests)
- **Approach**: Simple migration using legacyExampleCollection1SolrHome() - replaced getBaseUrl() with solrClientTestRule.getBaseUrl()
- **Test Command**: `./gradlew ":solr:solrj:test" "--tests" "org.apache.solr.client.solrj.TestBatchUpdate"`

### 3. TestSolrJErrorHandling
- **File**: `solr/solrj/src/test/org/apache/solr/client/solrj/TestSolrJErrorHandling.java`
- **Status**: ✅ PASSED (4 tests)
- **Approach**: Migrated with ClassRule, replaced all method calls (getBaseUrl, getCoreUrl, getSolrClient, getJetty)
- **Challenges**: Had to fix getCoreUrl() and getJetty() calls in addition to basic replacements
- **Test Command**: `./gradlew ":solr:solrj:test" "--tests" "org.apache.solr.client.solrj.TestSolrJErrorHandling"`

## Migration Pattern

### For Tests Using legacyExampleCollection1SolrHome()

These are the easiest to migrate. Follow this standard pattern:

```java
// BEFORE
public class TestName extends SolrJettyTestBase {
  @BeforeClass
  public static void beforeTest() throws Exception {
    createAndStartJetty(legacyExampleCollection1SolrHome());
  }
  
  public void testSomething() {
    try (SolrClient client = new HttpSolrClient.Builder(getBaseUrl()).build()) {
      // test code
    }
  }
}

// AFTER
public class TestName extends SolrTestCaseJ4 {
  @ClassRule public static SolrJettyTestRule solrClientTestRule = new SolrJettyTestRule();
  
  @BeforeClass
  public static void beforeTest() throws Exception {
    solrClientTestRule.startSolr(legacyExampleCollection1SolrHome());
  }
  
  public void testSomething() {
    try (SolrClient client = new HttpSolrClient.Builder(solrClientTestRule.getBaseUrl()).build()) {
      // test code
    }
  }
}
```

### Key Replacements
1. Change class: `extends SolrJettyTestBase` → `extends SolrTestCaseJ4`
2. Add field: `@ClassRule public static SolrJettyTestRule solrClientTestRule = new SolrJettyTestRule();`
3. Add imports:
   - `org.apache.solr.util.SolrJettyTestRule`
   - `org.junit.ClassRule`
4. Replace method calls:
   - `createAndStartJetty(...)` → `solrClientTestRule.startSolr(...)`
   - `getBaseUrl()` → `solrClientTestRule.getBaseUrl()`
   - `getCoreUrl()` → `solrClientTestRule.getBaseUrl() + "/" + DEFAULT_TEST_CORENAME`
   - `getSolrClient()` → `solrClientTestRule.getSolrClient()`
   - `getJetty()` → `solrClientTestRule.getJetty()`
   - Remove `@AfterClass cleanUpJettyHome()` - handled by Rule

## Recommended Next Steps

### Priority 1: Tests Using legacyExampleCollection1SolrHome (14 tests)
These are straightforward and should all follow the same pattern:

**Test-framework tests (4):**
- `BasicHttpSolrClientTest` - has ~20 getBaseUrl() calls, large file but pattern is consistent
- `ConcurrentUpdateSolrClientTest`
- `HttpSolrClientConPoolTest`
- `ConcurrentUpdateSolrClientBadInputTest`

**Core tests (6):**
- `JvmMetricsTest`
- `DistributedDebugComponentTest`
- `TestReplicationHandlerBackup`
- `ShowFileRequestHandlerTest`
- `TestRestoreCore`
- `TestHttpRequestId`

**Solrj tests (8):**
- `TestClusteringResponse` - simple, minimal Jetty use
- `TestSuggesterResponse` - uses getSolrClient and createSuggestSolrClient
- `InputStreamResponseParserTest` - straightforward setup
- `HttpSolrClientBadInputTest`
- `LBHttpSolrClientBadInputTest`
- `ConcurrentUpdateJettySolrClientBadInputTest`
- `ConcurrentUpdateJettySolrClientTest`
- `HttpJettySolrClientCompatibilityTest`

**Estimated effort**: 30-45 minutes for all 14 if done systematically using bulk replacements

### Priority 2: Tests with Custom Configurations (5 tests)
These need investigation to understand how to apply custom solrconfig.xml files:

- `ResponseHeaderTest` - uses solrconfig-headers.xml
- `TestRemoteStreaming` - uses custom streaming config
- `TestTolerantSearch` - multi-collection setup
- `TestSQLHandlerNonCloud` - uses setupJettyTestHome
- `JvmMetricsTest` - needs metrics-specific setup

### Priority 3: Abstract Base Classes (3 tests)
These need special attention since subclasses might inherit behavior:

- `RestTestBase`
- `SolrExampleTestsBase`
- `HttpSolrClientTestBase`

May require refactoring to move ClassRule into subclasses or create a different pattern.

## Key Findings

1. **Pattern Consistency**: Most tests follow similar patterns, making bulk migration possible
2. **Method Replacement**: Need to carefully replace all getBaseUrl(), getCoreUrl(), getSolrClient(), getJetty() calls
3. **Abstract Classes**: More complex - may need case-by-case handling
4. **Custom Configs**: Tests with setupJettyTestHome() or custom solrconfig files need investigation into how SolrJettyTestRule handles initialization

## Testing Strategy

After each migration:
1. Run the test to ensure it passes
2. Check for any additional getBaseUrl/getSolrClient calls in related code
3. Verify no regressions in related tests

Example:
```bash
./gradlew :solr:modulepath:test --tests "org.apache.solr.TestClass" 2>&1 | tail -20
```

## Notes for Future Work

- Consider creating a Sed/regex script to automatically replace getBaseUrl() → solrClientTestRule.getBaseUrl() across all remaining files
- Abstract base classes may need special template if many subclasses use them
- Custom configuration tests may need to use SolrClientTestRule.newCollection() API to replicate previous behavior
