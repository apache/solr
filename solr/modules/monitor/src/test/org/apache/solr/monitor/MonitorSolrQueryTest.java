/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.solr.monitor;

import static org.apache.solr.monitor.MonitorConstants.MONITOR_DOCUMENTS_KEY;
import static org.apache.solr.monitor.MonitorConstants.MONITOR_OUTPUT_KEY;
import static org.apache.solr.monitor.MonitorConstants.MONITOR_QUERIES_KEY;
import static org.apache.solr.monitor.MonitorConstants.REVERSE_SEARCH_PARAM_NAME;
import static org.apache.solr.monitor.MonitorConstants.WRITE_TO_DOC_LIST_KEY;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class MonitorSolrQueryTest extends BaseDistributedSearchTestCase {

  @Test
  @ShardsFixed(num = 3)
  public void testMonitorQuery() throws Exception {
    del("*:*");
    index(id, Integer.toString(0), "_mq", "content_s:\"elevator music\"");
    index(
        id,
        Integer.toString(1),
        "_mq",
        "{!xmlparser}<SpanOrTerms slop=\"3\" fieldName=\"content_s\">steep stairs</SpanOrTerms>");
    index(id, Integer.toString(2), "_mq", "content_s:\"elevator sounds\"");
    index(id, Integer.toString(3), "_mq", "content_s:\"elevator drop\"");
    index(id, Integer.toString(4), "_mq", "content_s:\"elevator stairs\"");
    index(id, Integer.toString(5), "_mq", "other_content_s:\"solr is cool\"");
    index(id, Integer.toString(6), "_mq", "other_content_s:\"solr is lame\"");
    index(id, Integer.toString(7), "_mq", "content_s:something", "_mq_payload", "some metadata");
    commit();
    handle.clear();
    handle.put("responseHeader", SKIP);
    handle.put("response", SKIP);

    Object[] params =
        new Object[] {
          CommonParams.SORT,
          "_query_id desc",
          CommonParams.JSON,
          read("/monitor/multi-doc-batch.json"),
          REVERSE_SEARCH_PARAM_NAME,
          true
        };
    QueryResponse response = query(params);
    System.out.println("Response = " + response);
    validate(response, 0, 0, "1");
    validate(response, 0, 1, "4");
    validate(response, 1, 0, "7");
    validate(response, 2, 0, "5");
    assertEquals(0, ((SolrDocumentList) response.getResponse().get("response")).size());

    index(id, Integer.toString(5), "_mq", "other_content_s:\"solr is lame\"");
    index(id, Integer.toString(6), "_mq", "other_content_s:\"solr is cool\"");
    index(
        id,
        Integer.toString(1),
        "_mq",
        "{!xmlparser}<SpanOrTerms slop=\"3\" fieldName=\"content_s\">steep hill</SpanOrTerms>");
    index(id, Integer.toString(2), "_mq", "content_s:elevator");
    commit();
    handle.clear();
    handle.put("responseHeader", SKIP);
    handle.put("response", SKIP);

    params =
        new Object[] {
          CommonParams.SORT,
          id + " desc",
          CommonParams.JSON,
          read("/monitor/multi-doc-batch.json"),
          REVERSE_SEARCH_PARAM_NAME,
          true,
          WRITE_TO_DOC_LIST_KEY,
          supportsWriteToDocList()
        };
    response = query(params);
    System.out.println("Response = " + response);
    validate(response, 0, 0, "2");
    validate(response, 0, 1, "4");
    validate(response, 1, 0, "7");
    validate(response, 2, 0, "6");
    if (supportsWriteToDocList()) {
      assertEquals(4, ((SolrDocumentList) response.getResponse().get("response")).size());
    }
  }

  @Test
  @ShardsFixed(num = 3)
  public void testNoDocListInResponse() throws Exception {
    del("*:*");
    index(id, Integer.toString(0), "_mq", "content_s:\"elevator music\"");
    index(
        id,
        Integer.toString(1),
        "_mq",
        "{!xmlparser}<SpanOrTerms slop=\"3\" fieldName=\"content_s\">steep stairs</SpanOrTerms>");
    index(id, Integer.toString(2), "_mq", "content_s:\"elevator sounds\"");
    index(id, Integer.toString(3), "_mq", "content_s:\"elevator drop\"");
    index(id, Integer.toString(4), "_mq", "content_s:\"elevator stairs\"");
    index(id, Integer.toString(5), "_mq", "other_content_s:\"solr is cool\"");
    index(id, Integer.toString(6), "_mq", "other_content_s:\"solr is lame\"");
    index(id, Integer.toString(7), "_mq", "content_s:something");
    commit();
    handle.clear();
    handle.put("responseHeader", SKIP);
    handle.put("response", SKIP);

    Object[] params =
        new Object[] {
          CommonParams.SORT,
          "_query_id desc",
          CommonParams.JSON,
          read("/monitor/multi-doc-batch.json"),
          REVERSE_SEARCH_PARAM_NAME,
          true,
          WRITE_TO_DOC_LIST_KEY,
          false
        };
    QueryResponse response = query(params);
    System.out.println("Response = " + response);
    validate(response, 0, 0, "1");
    validate(response, 0, 1, "4");
    validate(response, 1, 0, "7");
    validate(response, 2, 0, "5");
    assertEquals(0, ((SolrDocumentList) response.getResponse().get("response")).size());
  }

  @Test
  @ShardsFixed(num = 1)
  public void testDefaultParser() throws Exception {
    del("*:*");
    index(id, Integer.toString(0), "_mq", "content_s:\"elevator stairs\"");
    index(id, Integer.toString(1), "_mq", "content_s:\"something else\"");
    commit();
    handle.clear();
    handle.put("responseHeader", SKIP);
    handle.put("response", SKIP);

    Object[] params =
        new Object[] {
          CommonParams.SORT,
          id + " desc",
          CommonParams.JSON,
          read("/monitor/multi-doc-batch.json"),
          REVERSE_SEARCH_PARAM_NAME,
          true,
          WRITE_TO_DOC_LIST_KEY,
          supportsWriteToDocList()
        };

    QueryResponse response = query(params);
    System.out.println("Response = " + response);
    validate(response, 0, 0, "0");
    validate(response, 1, 0, "1");
  }

  @Test
  @ShardsFixed(num = 1)
  public void testDisjunctionQuery() throws Exception {
    del("*:*");
    index(
        id,
        Integer.toString(0),
        "_mq",
        "{!xmlparser}<DisjunctionMaxQuery><TermQuery fieldName=\"content0_s\">elevator</TermQuery><TermQuery fieldName=\"content1_s\">winda</TermQuery><TermQuery fieldName=\"content2_s\">stufen</TermQuery></DisjunctionMaxQuery>");
    index(
        id,
        Integer.toString(1),
        "_mq",
        "{!xmlparser}<DisjunctionMaxQuery><TermQuery fieldName=\"content0_s\">not</TermQuery><TermQuery fieldName=\"content1_s\">these</TermQuery><TermQuery fieldName=\"content2_s\">terms</TermQuery></DisjunctionMaxQuery>");
    commit();
    handle.clear();
    handle.put("responseHeader", SKIP);
    handle.put("response", SKIP);

    Object[] params =
        new Object[] {
          CommonParams.SORT,
          id + " desc",
          CommonParams.JSON,
          read("/monitor/single-doc-batch.json"),
          REVERSE_SEARCH_PARAM_NAME,
          true,
          WRITE_TO_DOC_LIST_KEY,
          supportsWriteToDocList()
        };

    QueryResponse response = query(params);
    System.out.println("Response = " + response);
    if (supportsWriteToDocList()) {
      assertEquals(3, ((SolrDocumentList) response.getResponse().get("response")).size());
    }
    validate(response, 0, 0, "0");
  }

  @Test
  @ShardsFixed(num = 1)
  public void testNoDanglingDecomposition() throws Exception {
    del("*:*");
    index(
        id,
        Integer.toString(0),
        "_mq",
        "{!xmlparser}<DisjunctionMaxQuery><TermQuery fieldName=\"content0_s\">elevator</TermQuery><TermQuery fieldName=\"content1_s\">lift</TermQuery></DisjunctionMaxQuery>");
    commit();
    handle.clear();
    handle.put("responseHeader", SKIP);
    handle.put("response", SKIP);

    Object[] params =
        new Object[] {
          CommonParams.SORT,
          id + " desc",
          CommonParams.JSON,
          read("/monitor/dangling-test-single-doc-batch.json"),
          REVERSE_SEARCH_PARAM_NAME,
          true,
          WRITE_TO_DOC_LIST_KEY,
          supportsWriteToDocList()
        };

    QueryResponse response = query(params);
    System.out.println("Response = " + response);
    if (supportsWriteToDocList()) {
      assertEquals(1, ((SolrDocumentList) response.getResponse().get("response")).size());
    }
    validate(response, 0, 0, "0");

    index(
        id,
        Integer.toString(0),
        "_mq",
        "{!xmlparser}<DisjunctionMaxQuery><TermQuery fieldName=\"content0_s\">elevator</TermQuery></DisjunctionMaxQuery>");
    commit();
    response = query(params);
    System.out.println("Response = " + response);
    if (supportsWriteToDocList()) {
      assertEquals(0, ((SolrDocumentList) response.getResponse().get("response")).size());
    }
    validateEmpty(response, 0);
  }

  @Test
  @ShardsFixed(num = 1)
  public void testNotQuery() throws Exception {
    del("*:*");
    index(id, Integer.toString(0), "_mq", "*:* -content_s:\"elevator stairs\"");
    index(id, Integer.toString(1), "_mq", "*:* -content_s:\"candy canes\"");
    commit();
    handle.clear();
    handle.put("responseHeader", SKIP);
    handle.put("response", SKIP);

    Object[] params =
        new Object[] {
          CommonParams.SORT,
          id + " desc",
          CommonParams.JSON,
          read("/monitor/single-doc-batch.json"),
          REVERSE_SEARCH_PARAM_NAME,
          true,
          WRITE_TO_DOC_LIST_KEY,
          supportsWriteToDocList()
        };

    QueryResponse response = query(params);
    System.out.println("Response = " + response);
    validate(response, 0, 1, "1");
  }

  @Test
  @ShardsFixed(num = 1)
  public void testWildCardQuery() throws Exception {
    del("*:*");
    index(id, Integer.toString(0), "_mq", "content_s:te*");
    index(id, Integer.toString(1), "_mq", "content_s:tes*");
    index(id, Integer.toString(2), "_mq", "content_s:test*");
    index(id, Integer.toString(3), "_mq", "content_s:tex*");
    index(id, Integer.toString(4), "_mq", "content_s:tests*");
    commit();
    handle.clear();
    handle.put("responseHeader", SKIP);
    handle.put("response", SKIP);

    Object[] params =
        new Object[] {
          CommonParams.SORT,
          id + " desc",
          CommonParams.JSON,
          read("/monitor/multi-doc-batch.json"),
          REVERSE_SEARCH_PARAM_NAME,
          true,
          WRITE_TO_DOC_LIST_KEY,
          supportsWriteToDocList()
        };

    QueryResponse response = query(params);
    if (supportsWriteToDocList()) {
      assertEquals(3, ((SolrDocumentList) response.getResponse().get("response")).size());
    }
    System.out.println("Response = " + response);
    validate(response, 3, 0, "0");
    validate(response, 3, 1, "1");
    validate(response, 3, 2, "2");
    validate(response, 4, 0, "0");
    validate(response, 4, 1, "1");
    validate(response, 4, 2, "2");
  }

  @Test
  @ShardsFixed(num = 1)
  public void manySegmentsQuery() throws Exception {
    del("*:*");
    int count = 10_000;
    IntStream.range(0, count)
        .forEach(
            i -> {
              try {
                index(id, Integer.toString(i), "_mq", "content_s:\"elevator stairs\"");
              } catch (Exception e) {
                throw new IllegalStateException(e);
              }
            });
    commit();
    handle.clear();
    handle.put("responseHeader", SKIP);
    handle.put("response", SKIP);

    Object[] params =
        new Object[] {
          CommonParams.SORT,
          id + " desc",
          CommonParams.JSON,
          read("/monitor/multi-doc-batch.json"),
          REVERSE_SEARCH_PARAM_NAME,
          true
        };

    QueryResponse response = query(params);
    validate(response, 0, 0, "0");
    assertEquals(count, ((Map) response.getResponse().get("monitor")).get("queriesRun"));
    assertEquals(
        count,
        ((List)
                ((Map)
                        ((Map)
                                ((Map) response.getResponse().get("monitor"))
                                    .get("monitorDocuments"))
                            .get(0))
                    .get("queries"))
            .size());

    IntStream.range(count / 2, count)
        .forEach(
            i -> {
              try {
                index(id, Integer.toString(i), "_mq", "content_s:\"x y\"");
              } catch (Exception e) {
                throw new IllegalStateException(e);
              }
            });
    commit();

    response = query(params);
    validate(response, 0, 0, "0");
    assertEquals(count / 2, ((Map) response.getResponse().get("monitor")).get("queriesRun"));
    assertEquals(
        count / 2,
        ((List)
                ((Map)
                        ((Map)
                                ((Map) response.getResponse().get("monitor"))
                                    .get("monitorDocuments"))
                            .get(0))
                    .get("queries"))
            .size());
  }

  void validate(QueryResponse response, int doc, int query, Object expectedValue) {
    var monitorQueries = monitorQueries(response, doc);
    assertTrue(monitorQueries.size() > query);
    assertEquals(expectedValue, monitorQueries.get(query));
  }

  void validateEmpty(QueryResponse response, int doc) {
    var monitorQueries = monitorQueries(response, doc);
    assertTrue(monitorQueries.isEmpty());
  }

  private List<Object> monitorQueries(QueryResponse response, int doc) {
    assertTrue(response.getResponse().get(MONITOR_OUTPUT_KEY) instanceof Map);
    assertTrue(
        ((Map<?, ?>) response.getResponse().get(MONITOR_OUTPUT_KEY)).get(MONITOR_DOCUMENTS_KEY)
            instanceof Map);
    Map<Integer, Object> monitorDocuments =
        (Map<Integer, Object>)
            ((Map<?, ?>) response.getResponse().get(MONITOR_OUTPUT_KEY)).get(MONITOR_DOCUMENTS_KEY);
    assertTrue(monitorDocuments.get(doc) instanceof Map);
    var nthDoc = (Map<String, Object>) monitorDocuments.get(doc);
    assertTrue(nthDoc.get(MONITOR_QUERIES_KEY) instanceof List);
    return (List<Object>) nthDoc.get(MONITOR_QUERIES_KEY);
  }

  protected String read(String resourceName) throws IOException, URISyntaxException {
    final URL url = getClass().getResource(resourceName);
    return Files.readString(Path.of(url.toURI()), StandardCharsets.UTF_8);
  }

  protected boolean supportsWriteToDocList() {
    return true;
  }
}
