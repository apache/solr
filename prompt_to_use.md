The class #file:solr/test-framework/src/java/org/apache/solr/SolrJettyTestBase.java has been deprecated in favour of using #file:solr/test-framework/src/java/org/apache/solr/util/SolrJettyTestRule.java.

I want to migrate our tests away from #file:solr/test-framework/src/java/org/apache/solr/SolrJettyTestBase.java.   Please go through the code base and migrate each test one by one.  I want you to migrate each test, running the unit test after each one.  If you can't successfully migrate it, then I want you to write it out to "tests_not_migrated.md" and move on.

Please look at #file:solr/core/src/test/org/apache/solr/response/TestPrometheusResponseWriter.java as an example of good use of #file:solr/test-framework/src/java/org/apache/solr/util/SolrJettyTestRule.java test rule.

Please do not change createTempDir() method to LuceneTestCase.createTempDir().

Please do not create hard coded urls like 127.0.0.1 in the tests.

We use the try-with-resources pattern to make sure resouces such as httpclients, solrclients, cores etc are closed.  Please use that where possible and skip manually closing the resource.  Unless of course we run into issues with the `ObjectReleaseTracker`.