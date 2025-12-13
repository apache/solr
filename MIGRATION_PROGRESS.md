# SolrJettyTestBase Migration Progress

## Overall Status
- **Total tests initially requiring migration**: 30
- **Migrations completed**: 8 ✅ (26.7%)
- **Remaining to migrate**: 22
- **Success rate**: 100% of attempted migrations pass

## Successfully Migrated Tests (8 ✅)

### Solrj Tests (3)
1. ✅ **TestBatchUpdate** - solr/solrj/src/test/org/apache/solr/client/solrj/TestBatchUpdate.java
   - 3 tests passed
   - Pattern: Basic getBaseUrl() replacement
   
2. ✅ **TestSolrJErrorHandling** - solr/solrj/src/test/org/apache/solr/client/solrj/TestSolrJErrorHandling.java
   - 4 tests passed
   - Pattern: Multiple method replacements (getBaseUrl, getCoreUrl, getJetty)
   
3. ✅ **HttpSolrClientBadInputTest** - solr/solrj/src/test/org/apache/solr/client/solrj/impl/HttpSolrClientBadInputTest.java
   - 1 test passed
   - Pattern: getHttpSolrClient() with getBaseUrl()
   
4. ✅ **LBHttpSolrClientBadInputTest** - solr/solrj/src/test/org/apache/solr/client/solrj/impl/LBHttpSolrClientBadInputTest.java
   - 1 test passed
   - Pattern: LBHttpSolrClient.Builder with getBaseUrl()

### Core Tests (4)
5. ✅ **TestSolrCoreProperties** - solr/core/src/test/org/apache/solr/TestSolrCoreProperties.java
   - Pattern: Simple getSolrClient() replacement

6. ✅ **TestHttpRequestId** - solr/core/src/test/org/apache/solr/handler/TestHttpRequestId.java
   - 4 tests passed
   - Pattern: Single getBaseUrl() in HttpJettySolrClient.Builder
   
7. ✅ **TestRestoreCore** - solr/core/src/test/org/apache/solr/handler/TestRestoreCore.java
   - 3 tests passed
   - Pattern: No SolrJettyTestBase static methods used (manual setup)
   
8. ✅ **ShowFileRequestHandlerTest** - solr/core/src/test/org/apache/solr/handler/admin/ShowFileRequestHandlerTest.java
   - 11 tests passed
   - Pattern: Multiple getSolrClient() calls with DEFAULT_TEST_CORENAME

## Migration Pattern (Proven Successful)

### Step 1: Update Imports
```java
// Add
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.ClassRule;

// Remove
// (SolrJettyTestBase import not needed if using SolrTestCaseJ4)
```

### Step 2: Change Class Declaration
```java
// Before
public class TestName extends SolrJettyTestBase {

// After
public class TestName extends SolrTestCaseJ4 {
  @ClassRule public static SolrJettyTestRule solrClientTestRule = new SolrJettyTestRule();
```

### Step 3: Update BeforeClass Method
```java
// Before
@BeforeClass
public static void beforeTest() throws Exception {
  createAndStartJetty(legacyExampleCollection1SolrHome());
}

// After
@BeforeClass
public static void beforeTest() throws Exception {
  solrClientTestRule.startSolr(legacyExampleCollection1SolrHome());
}
```

### Step 4: Replace Method Calls Throughout Tests
- `getBaseUrl()` → `solrClientTestRule.getBaseUrl()`
- `getSolrClient()` → `solrClientTestRule.getSolrClient(DEFAULT_TEST_CORENAME)`
- `getJetty()` → `solrClientTestRule.getJetty()`
- `getCoreUrl()` → `solrClientTestRule.getBaseUrl() + "/" + DEFAULT_TEST_CORENAME`
- Remove `@AfterClass cleanUpJettyHome()` methods (Rule handles cleanup)

## Remaining Tests (22)

### Priority 1: Straightforward Migrations (14 tests)

These follow the simple pattern and should migrate easily:

**Solrj Tests (6)**
- [ ] ConcurrentUpdateJettySolrClientBadInputTest - solrj/src/test/org/apache/solr/client/solrj/jetty/
- [ ] ConcurrentUpdateJettySolrClientTest - solrj/src/test/org/apache/solr/client/solrj/jetty/
- [ ] HttpJettySolrClientCompatibilityTest - solrj/src/test/org/apache/solr/client/solrj/jetty/
- [ ] TestClusteringResponse - solrj/src/test/org/apache/solr/client/solrj/response/
- [ ] TestSuggesterResponse - solrj/src/test/org/apache/solr/client/solrj/response/
- [ ] InputStreamResponseParserTest - solrj/src/test/org/apache/solr/client/solrj/response/

**Core/Handler Tests (2)**
- [ ] JvmMetricsTest - core/src/test/org/apache/solr/metrics/
- [ ] ResponseHeaderTest - core/src/test/org/apache/solr/servlet/

**Estimated effort**: 20-30 minutes (all straightforward)

### Priority 2: Tests with Custom Configurations (5 tests)

These require investigation and may need custom handling:

- [ ] DistributedDebugComponentTest - uses setupJettyTestHome() for multi-collection setup
- [ ] TestRemoteStreaming - uses setupJettyTestHome() + custom streaming config
- [ ] TestTolerantSearch - multi-collection test setup
- [ ] TestSQLHandlerNonCloud - uses setupJettyTestHome()
- [ ] TestReplicationHandlerBackup - uses setupJettyTestHome()

**Key Issue**: These tests use `setupJettyTestHome()` which copies pre-configured collections. SolrJettyTestRule may need investigation into how to apply similar custom configurations.

**Estimated effort**: 1.5-2 hours (investigation + fixes)

### Priority 3: Abstract Base Classes (3 tests)

These are abstract base classes with subclasses inheriting behavior - may require refactoring:

- [ ] RestTestBase - abstract base for REST tests
- [ ] SolrExampleTestsBase - abstract base for SolrJ example tests
- [ ] HttpSolrClientTestBase - abstract base for HTTP client tests
- [ ] CacheHeaderTestBase - abstract base for cache header tests

**Strategy Options**:
1. Move @ClassRule into each subclass
2. Create a factory pattern for initialization
3. Modify abstract class to provide the rule to subclasses

**Estimated effort**: 1-1.5 hours (refactoring + testing)

## Lessons Learned

1. **Method Consistency**: All tests that use the simple pattern (`legacyExampleCollection1SolrHome()`) are standardized and follow the same migration
2. **ClassRule Benefits**: JUnit ClassRule handles all lifecycle management automatically - no need for @AfterClass
3. **Collection Name**: `DEFAULT_TEST_CORENAME` (which is "collection1") must be passed to `getSolrClient()`
4. **Jetty Config**: When JettyConfig needed, use 3-arg `startSolr(path, properties, config)` method
5. **Custom Setup**: Tests with manual Jetty setup don't need migration if they don't use SolrJettyTestBase static methods

## Testing Strategy

After migrating each test:
```bash
./gradlew ":solr:MODULE:test" "--tests" "full.class.path.ClassName" 2>&1 | tail -20
```

Expected output:
```
:solr:module:test (SUCCESS): N test(s)
BUILD SUCCESSFUL in Xs
```

## Next Steps Recommended

1. **Quick wins** (Priority 1): Migrate the 6 remaining straightforward solrj tests (20-30 min)
2. **Handler tests** (Priority 1): Migrate the 2 core handler tests (5-10 min)
3. **Investigation** (Priority 2): Study setupJettyTestHome() pattern and custom configs (30-60 min)
4. **Abstract classes** (Priority 3): Determine refactoring approach (30 min planning + 30-60 min implementation)

## Critical Notes

- ✅ Do NOT change `createTempDir()` to `LuceneTestCase.createTempDir()` (requirement enforced)
- ✅ All completed migrations have 100% test pass rate
- ✅ ClassRule cleanup is automatic and reliable
- ✅ Pattern is consistent across all straightforward tests
- ⚠️ Custom configuration tests may need deeper investigation
- ⚠️ Abstract base class strategy needs decision before proceeding
