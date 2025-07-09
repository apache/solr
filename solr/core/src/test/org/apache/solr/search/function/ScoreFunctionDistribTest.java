package org.apache.solr.search.function;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.junit.BeforeClass;
import org.junit.Test;

public class ScoreFunctionDistribTest extends SolrCloudTestCase {

  private static final String COLLECTION = "coll1";

  @BeforeClass
  public static void configureSolr() throws Exception {
    configureCluster(2).addConfig("config1", configset("cloud-minimal")).configure();

    prepareCollection();
  }

  private static void prepareCollection() throws Exception {
    var client = cluster.getSolrClient();
    var createResponse =
        CollectionAdminRequest.createCollection(COLLECTION, "config1", 2, 1).process(client);
    assertEquals(0, createResponse.getStatus());

    cluster.waitForActiveCollection(COLLECTION, 10, TimeUnit.SECONDS);

    var updateResponse =
        new UpdateRequest()
            .add(
                List.of(
                    sdoc("id", "1", "text_s", "foo"),
                    sdoc("id", "2", "text_s", "bar"),
                    sdoc("id", "3", "text_s", "qux"),
                    sdoc("id", "4", "text_s", "asd"),
                    sdoc("id", "101", "text_s", "random text"),
                    sdoc("id", "102", "text_s", "random text"),
                    sdoc("id", "103", "text_s", "random text"),
                    sdoc("id", "104", "text_s", "random text"),
                    sdoc("id", "105", "text_s", "random text"),
                    sdoc("id", "106", "text_s", "random text"),
                    sdoc("id", "107", "text_s", "random text"),
                    sdoc("id", "108", "text_s", "random text"),
                    sdoc("id", "109", "text_s", "random text")))
            .commit(client, COLLECTION);
    assertEquals(0, updateResponse.getStatus());
  }

  @Test
  public void testScoreFunction_boostQuery() throws Exception {
    var resp =
        cluster
            .getSolrClient()
            .query(
                COLLECTION,
                params(
                    "q",
                    "{!boost b=if(lte(score,2),2.5,1)}foo^=1 bar^=2 qux^=3 asd^=4",
                    "df",
                    "text_s",
                    "fl",
                    "id,score"));

    assertJSONEquals(
        """
            {
              "numFound": 4,
              "start": 0,
              "maxScore": 5.0,
              "numFoundExact": true,
              "docs": [
                {
                  "id": "2",
                  "score": 5.0
                },
                {
                  "id": "4",
                  "score": 4.0
                },
                {
                  "id": "3",
                  "score": 3.0
                },
                {
                  "id": "1",
                  "score": 2.5
                }
              ]
            }""",
        resp.getResults().jsonStr());
  }

  @Test
  public void testScoreFunction_postFilter() throws Exception {
    var resp =
        cluster
            .getSolrClient()
            .query(
                COLLECTION,
                params(
                    "q",
                    "foo^=1 bar^=2 qux^=3 asd^=4",
                    "df",
                    "text_s",
                    "fl",
                    "id,score",
                    "fq",
                    "{!frange l=2 u=3 cache=false}score"));

    assertJSONEquals(
        """
            {
              "numFound": 2,
              "start": 0,
              "maxScore": 3.0,
              "numFoundExact": true,
              "docs": [
                {
                  "id": "3",
                  "score": 3.0
                },
                {
                  "id": "2",
                  "score": 2.0
                }
              ]
            }""",
        resp.getResults().jsonStr());
  }

  @Test
  public void testScoreFunction_pseudoField() throws Exception {
    var resp =
        cluster
            .getSolrClient()
            .query(
                COLLECTION,
                params(
                    "q",
                    "foo^=1 bar^=2 qux^=3",
                    "df",
                    "text_s",
                    "preFetchDocs",
                    "0", // TODO FIXME handle pre-fetching introduced in SOLR-17775
                    "fl",
                    "id,score,custom:add(1,score,score)"));

    assertJSONEquals(
        """
            {
              "numFound": 3,
              "start": 0,
              "maxScore": 3.0,
              "numFoundExact": true,
              "docs": [
                {
                  "id": "3",
                  "score": 3.0,
                  "custom": 7.0
                },
                {
                  "id": "2",
                  "score": 2.0,
                  "custom": 5.0
                },
                {
                  "id": "1",
                  "score": 1.0,
                  "custom": 3.0
                }
              ]
            }""",
        resp.getResults().jsonStr());

    // error if scores not enabled
    assertThrows(
        SolrException.class,
        () ->
            cluster
                .getSolrClient()
                .query(
                    COLLECTION,
                    params(
                        "q",
                        "foo^=1 bar^=2 qux^=3",
                        "df",
                        "text_s",
                        "fl",
                        "id,custom:add(1,score,score)")));
  }
}
