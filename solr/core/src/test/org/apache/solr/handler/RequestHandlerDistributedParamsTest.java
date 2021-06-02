package org.apache.solr.handler;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.params.ShardParams;

public class RequestHandlerDistributedParamsTest extends SolrTestCaseJ4 {

  private RequestHandlerBase handler = null;

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Before
  public void setUp() {
    handler = new RequestHandlerBase() {
        // empty implementation
        public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {

        }

        public String getDescription() {
          return "test";
        }
    };
  }

  @Test
  public void shouldAppendParamsInNonDistributedRequest() {
    // given
    NamedList<String> appends = new NamedList<>();
    appends.add("foo", "appended");
    NamedList<NamedList<String>> init = new NamedList<>();
    init.add("appends", appends);
    handler.init(init);

    // when
    SolrQueryRequest req = req();
    handler.handleRequest(req, new SolrQueryResponse());

    // then
    assertEquals(req.getParams().get("foo"), "appended");
  }

  @Test
  public void shouldNotAppendParamsInDistributedRequest() {
    // given
    NamedList<String> appends = new NamedList<>();
    appends.add("foo", "appended");
    NamedList<NamedList<String>> init = new NamedList<>();
    init.add("appends", appends);
    handler.init(init);

    // when
    SolrQueryRequest req = req(ShardParams.IS_SHARD, "true");
    handler.handleRequest(req, new SolrQueryResponse());

    // then
    assertNull(req.getParams().get("foo"));
  }
}
