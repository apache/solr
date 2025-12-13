# Tests Not Migrated

## Successfully Migrated
- ✅ TestSolrCoreProperties - PASSED
- ✅ TestBatchUpdate - PASSED
- ✅ TestSolrJErrorHandling - PASSED

## Tests That Need Migration (22 remaining)

The following tests still extend SolrJettyTestBase and need to be migrated to SolrJettyTestRule:

### Complex Tests (Need Special Handling)
These tests use setupJettyTestHome() or have special collection configurations:

1. **TestSQLHandlerNonCloud** - solr/modules/sql/src/test/org/apache/solr/handler/sql/TestSQLHandlerNonCloud.java
   - Issue: Uses setupJettyTestHome which creates deprecated test collections
   - Action: Needs custom copySolrHomeToTemp() setup or refactoring
   
2. **TestTolerantSearch** - solr/core/src/test/org/apache/solr/TestTolerantSearch.java
   - Issue: Multi-collection setup with file copies
   - Action: Complex migration, may need reference to how SolrJettyTestRule handles multiple collections
   
3. **TestRemoteStreaming** - solr/core/src/test/org/apache/solr/request/TestRemoteStreaming.java
   - Issue: Custom solrconfig-tolerant-search.xml setup
   - Action: Needs investigation of how to apply custom configs with SolrJettyTestRule

4. **ResponseHeaderTest** - solr/core/src/test/org/apache/solr/servlet/ResponseHeaderTest.java
   - Issue: Custom solrconfig-headers.xml configuration
   - Action: Needs custom config handling

5. **CacheHeaderTestBase** - solr/core/src/test/org/apache/solr/servlet/CacheHeaderTestBase.java
   - Issue: Abstract base class, inherits from SolrJettyTestBase
   - Action: Abstract classes need special migration strategy

### Straightforward Tests (Use legacyExampleCollection1SolrHome)
These should be straightforward to migrate - they all use legacyExampleCollection1SolrHome():

**Test-framework tests:**
- BasicHttpSolrClientTest - solr/test-framework/src/test/org/apache/solr/client/solrj/apache/BasicHttpSolrClientTest.java
- ConcurrentUpdateSolrClientTest - solr/test-framework/src/test/org/apache/solr/client/solrj/apache/ConcurrentUpdateSolrClientTest.java
- HttpSolrClientConPoolTest - solr/test-framework/src/test/org/apache/solr/client/solrj/apache/HttpSolrClientConPoolTest.java
- ConcurrentUpdateSolrClientBadInputTest - solr/test-framework/src/test/org/apache/solr/client/solrj/apache/ConcurrentUpdateSolrClientBadInputTest.java

**Core tests:**
- JvmMetricsTest - solr/core/src/test/org/apache/solr/metrics/JvmMetricsTest.java
- DistributedDebugComponentTest - solr/core/src/test/org/apache/solr/handler/component/DistributedDebugComponentTest.java
- TestReplicationHandlerBackup - solr/core/src/test/org/apache/solr/handler/TestReplicationHandlerBackup.java
- ShowFileRequestHandlerTest - solr/core/src/test/org/apache/solr/handler/admin/ShowFileRequestHandlerTest.java
- TestRestoreCore - solr/core/src/test/org/apache/solr/handler/TestRestoreCore.java
- TestHttpRequestId - solr/core/src/test/org/apache/solr/handler/TestHttpRequestId.java

**Solrj tests:**
- TestClusteringResponse - solr/solrj/src/test/org/apache/solr/client/solrj/response/TestClusteringResponse.java
- TestSuggesterResponse - solr/solrj/src/test/org/apache/solr/client/solrj/response/TestSuggesterResponse.java
- InputStreamResponseParserTest - solr/solrj/src/test/org/apache/solr/client/solrj/response/InputStreamResponseParserTest.java
- HttpSolrClientBadInputTest - solr/solrj/src/test/org/apache/solr/client/solrj/impl/HttpSolrClientBadInputTest.java
- LBHttpSolrClientBadInputTest - solr/solrj/src/test/org/apache/solr/client/solrj/impl/LBHttpSolrClientBadInputTest.java
- ConcurrentUpdateJettySolrClientBadInputTest - solr/solrj/src/test/org/apache/solr/client/solrj/jetty/ConcurrentUpdateJettySolrClientBadInputTest.java
- ConcurrentUpdateJettySolrClientTest - solr/solrj/src/test/org/apache/solr/client/solrj/jetty/ConcurrentUpdateJettySolrClientTest.java
- HttpJettySolrClientCompatibilityTest - solr/solrj/src/test/org/apache/solr/client/solrj/jetty/HttpJettySolrClientCompatibilityTest.java

### Abstract Base Classes (Need Special Handling)
- RestTestBase - solr/test-framework/src/java/org/apache/solr/util/RestTestBase.java
- SolrExampleTestsBase - solr/solrj/src/test/org/apache/solr/client/solrj/SolrExampleTestsBase.java
- HttpSolrClientTestBase - solr/solrj/src/test/org/apache/solr/client/solrj/impl/HttpSolrClientTestBase.java

## Migration Pattern for Straightforward Tests

For tests using `legacyExampleCollection1SolrHome()`, follow this pattern:

1. **Add imports:**
   ```java
   import org.apache.solr.SolrTestCaseJ4;
   import org.apache.solr.util.SolrJettyTestRule;
   import org.junit.ClassRule;
   ```

2. **Change class declaration:**
   ```java
   public class TestName extends SolrTestCaseJ4 {
     @ClassRule public static SolrJettyTestRule solrClientTestRule = new SolrJettyTestRule();
   ```

3. **Update BeforeClass method:**
   ```java
   @BeforeClass
   public static void beforeTest() throws Exception {
     solrClientTestRule.startSolr(legacyExampleCollection1SolrHome(), ...);
     // OR if simpler:
     solrClientTestRule.startSolr(legacyExampleCollection1SolrHome());
   }
   ```

4. **Replace method calls throughout the test:**
   - `getBaseUrl()` → `solrClientTestRule.getBaseUrl()`
   - `getCoreUrl()` → `solrClientTestRule.getBaseUrl() + "/" + DEFAULT_TEST_CORENAME`
   - `getSolrClient()` → `solrClientTestRule.getSolrClient()`
   - `getJetty()` → `solrClientTestRule.getJetty()`
   - `getHttpClient()` → `solrClientTestRule.getJetty().getHttpClient()` (if needed)
   - `createAndStartJetty(...)` → remove from tests, use ClassRule instead
   - `afterClass cleanUpJettyHome()` → remove, handled by Rule

