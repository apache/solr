/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr;

import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import javax.xml.xpath.XPathExpressionException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.InputStreamResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.BaseTestHarness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrXPathTestCase extends SolrTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Executes a query using SolrClient and validates the XML response against XPath expressions.
   * This provides a similar interface to assertQ() in SolrTestCaseJ4 but works with SolrClient.
   *
   * @param client the SolrClient to use for the request
   * @param req the query parameters
   * @param tests XPath expressions to validate against the response
   * @see SolrTestCaseJ4#assertQ(String, SolrQueryRequest, String...)
   */
  public static void assertQ(SolrClient client, QueryRequest req, String... tests) {
    try {

      // Process request and extract raw response
      QueryResponse rsp = req.process(client);
      NamedList<Object> rawResponse = rsp.getResponse();
      InputStream stream = (InputStream) rawResponse.get("stream");
      String response = new String(stream.readAllBytes(), StandardCharsets.UTF_8);

      String results = BaseTestHarness.validateXPath(response, tests);

      if (results != null) {
        String msg =
            "REQUEST FAILED: xpath="
                + results
                + "\n\txml response was: "
                + response
                + "\n\trequest was:"
                + req.getQueryParams();

        fail(msg);
      }
    } catch (XPathExpressionException e1) {
      throw new RuntimeException("XPath is invalid", e1);
    } catch (Throwable e3) {
      log.error("REQUEST FAILED: {}", req.getParams(), e3);
      throw new RuntimeException("Exception during query", e3);
    }
  }

  /**
   * Instance method that delegates to the static assertQ using the instance's SolrClient. This
   * provides a convenient way to call assertQ from instance test methods.
   *
   * @param req the query parameters
   * @param tests XPath expressions to validate against the response
   */
  public void assertQ(QueryRequest req, String... tests) {
    assertQ(getSolrClient(), req, tests);
  }

  /**
   * Generates a QueryRequest
   *
   * @see SolrTestCaseJ4#req(String...)
   */
  public static QueryRequest req(String... q) {
    ModifiableSolrParams params = new ModifiableSolrParams();

    if (q.length == 1) {
      params.set("q", q);
    }
    if (q.length % 2 != 0) {
      throw new RuntimeException(
          "The length of the string array (query arguments) needs to be even");
    }
    for (int i = 0; i < q.length; i += 2) {
      params.set(q[i], q[i + 1]);
    }

    params.set("wt", "xml");
    params.set("indent", params.get("indent", "off"));

    QueryRequest req = new QueryRequest(params);
    String path = params.get("qt");
    if (path != null) {
      req.setPath(path);
    }
    req.setResponseParser(new InputStreamResponseParser("xml"));
    return req;
  }

  public SolrClient getSolrClient() {
    throw new RuntimeException("This method needs to be overridden");
  }
}
