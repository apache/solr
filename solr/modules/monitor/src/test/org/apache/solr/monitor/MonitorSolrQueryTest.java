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
import static org.apache.solr.monitor.MonitorConstants.WRITE_TO_DOC_LIST_KEY;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.lucene.monitor.MonitorFields;
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
    index(id, Integer.toString(0), MonitorFields.MONITOR_QUERY, "content_s:\"elevator music\"");
    index(
        id,
        Integer.toString(1),
        MonitorFields.MONITOR_QUERY,
        "{!xmlparser}<SpanOrTerms slop=\"3\" fieldName=\"content_s\">steep stairs</SpanOrTerms>");
    index(id, Integer.toString(2), MonitorFields.MONITOR_QUERY, "content_s:\"elevator sounds\"");
    index(id, Integer.toString(3), MonitorFields.MONITOR_QUERY, "content_s:\"elevator drop\"");
    index(id, Integer.toString(4), MonitorFields.MONITOR_QUERY, "content_s:\"elevator stairs\"");
    index(id, Integer.toString(5), MonitorFields.MONITOR_QUERY, "other_content_s:\"solr is cool\"");
    index(id, Integer.toString(6), MonitorFields.MONITOR_QUERY, "other_content_s:\"solr is lame\"");
    index(id, Integer.toString(7), MonitorFields.MONITOR_QUERY, "content_s:something");
    commit();
    handle.clear();
    handle.put("responseHeader", SKIP);
    handle.put("response", SKIP);

    Object[] params =
        new Object[] {
          CommonParams.SORT,
          MonitorFields.QUERY_ID + " desc",
          CommonParams.JSON,
          read("/monitor/multi-doc-batch.json"),
          CommonParams.QT,
          "/reverseSearch"
        };
    QueryResponse response = query(params);
    System.out.println("Response = " + response);
    validate(response, 0, List.of("1", "4"), false);
    validate(response, 1, List.of("7"), false);
    validate(response, 2, List.of("5"), false);
    assertEquals(0, ((SolrDocumentList) response.getResponse().get("response")).size());

    index(id, Integer.toString(5), MonitorFields.MONITOR_QUERY, "other_content_s:\"solr is lame\"");
    index(id, Integer.toString(6), MonitorFields.MONITOR_QUERY, "other_content_s:\"solr is cool\"");
    index(
        id,
        Integer.toString(1),
        MonitorFields.MONITOR_QUERY,
        "{!xmlparser}<SpanOrTerms slop=\"3\" fieldName=\"content_s\">steep hill</SpanOrTerms>");
    index(id, Integer.toString(2), MonitorFields.MONITOR_QUERY, "content_s:elevator");
    commit();
    handle.clear();
    handle.put("responseHeader", SKIP);
    handle.put("response", SKIP);

    final boolean writeToDocList = supportsWriteToDocList();
    params =
        new Object[] {
          CommonParams.SORT,
          id + " desc",
          CommonParams.JSON,
          read("/monitor/multi-doc-batch.json"),
          CommonParams.QT,
          "/reverseSearch",
          WRITE_TO_DOC_LIST_KEY,
          writeToDocList
        };
    response = query(params);
    System.out.println("Response = " + response);
    validate(response, 0, 0, "2", writeToDocList);
    validate(response, 0, 1, "4", writeToDocList);
    validate(response, 1, 0, "7", writeToDocList);
    validate(response, 2, 0, "6", writeToDocList);
    if (writeToDocList) {
      assertEquals(4, ((SolrDocumentList) response.getResponse().get("response")).size());
    }
  }

  @Test
  @ShardsFixed(num = 3)
  public void testNoDocListInResponse() throws Exception {
    index(id, Integer.toString(0), MonitorFields.MONITOR_QUERY, "content_s:\"elevator music\"");
    index(
        id,
        Integer.toString(1),
        MonitorFields.MONITOR_QUERY,
        "{!xmlparser}<SpanOrTerms slop=\"3\" fieldName=\"content_s\">steep stairs</SpanOrTerms>");
    index(id, Integer.toString(2), MonitorFields.MONITOR_QUERY, "content_s:\"elevator sounds\"");
    index(id, Integer.toString(3), MonitorFields.MONITOR_QUERY, "content_s:\"elevator drop\"");
    index(id, Integer.toString(4), MonitorFields.MONITOR_QUERY, "content_s:\"elevator stairs\"");
    index(id, Integer.toString(5), MonitorFields.MONITOR_QUERY, "other_content_s:\"solr is cool\"");
    index(id, Integer.toString(6), MonitorFields.MONITOR_QUERY, "other_content_s:\"solr is lame\"");
    index(id, Integer.toString(7), MonitorFields.MONITOR_QUERY, "content_s:something");
    commit();
    handle.clear();
    handle.put("responseHeader", SKIP);
    handle.put("response", SKIP);

    final boolean writeToDocList = false;
    Object[] params =
        new Object[] {
          CommonParams.SORT,
          MonitorFields.QUERY_ID + "  desc",
          CommonParams.JSON,
          read("/monitor/multi-doc-batch.json"),
          CommonParams.QT,
          "/reverseSearch",
          WRITE_TO_DOC_LIST_KEY,
          writeToDocList
        };
    QueryResponse response = query(params);
    System.out.println("Response = " + response);
    validate(response, 0, List.of("1", "4"), writeToDocList);
    validate(response, 1, List.of("7"), writeToDocList);
    validate(response, 2, List.of("5"), writeToDocList);
    assertEquals(0, ((SolrDocumentList) response.getResponse().get("response")).size());
  }

  @Test
  public void testDefaultParser() throws Exception {
    index(id, Integer.toString(0), MonitorFields.MONITOR_QUERY, "content_s:\"elevator stairs\"");
    index(id, Integer.toString(1), MonitorFields.MONITOR_QUERY, "content_s:\"something else\"");
    commit();
    handle.clear();
    handle.put("responseHeader", SKIP);
    handle.put("response", SKIP);

    final boolean writeToDocList = supportsWriteToDocList();
    Object[] params =
        new Object[] {
          CommonParams.SORT,
          id + " desc",
          CommonParams.JSON,
          read("/monitor/multi-doc-batch.json"),
          CommonParams.QT,
          "/reverseSearch",
          WRITE_TO_DOC_LIST_KEY,
          writeToDocList
        };

    QueryResponse response = query(params);
    System.out.println("Response = " + response);
    validate(response, 0, List.of("0"), writeToDocList);
    validate(response, 1, List.of("1"), writeToDocList);
  }

  @Test
  @ShardsFixed(num = 2)
  public void testDisjunctionQuery() throws Exception {
    index(
        id,
        Integer.toString(0),
        MonitorFields.MONITOR_QUERY,
        "{!xmlparser}<DisjunctionMaxQuery><TermQuery fieldName=\"content0_s\">elevator</TermQuery><TermQuery fieldName=\"content1_s\">winda</TermQuery><TermQuery fieldName=\"content2_s\">stufen</TermQuery></DisjunctionMaxQuery>");
    index(
        id,
        Integer.toString(1),
        MonitorFields.MONITOR_QUERY,
        "{!xmlparser}<DisjunctionMaxQuery><TermQuery fieldName=\"content0_s\">not</TermQuery><TermQuery fieldName=\"content1_s\">these</TermQuery><TermQuery fieldName=\"content2_s\">terms</TermQuery></DisjunctionMaxQuery>");
    commit();
    handle.clear();
    handle.put("responseHeader", SKIP);
    handle.put("response", SKIP);

    final boolean writeToDocList = supportsWriteToDocList();
    Object[] params =
        new Object[] {
          CommonParams.SORT,
          id + " desc",
          CommonParams.JSON,
          read("/monitor/single-doc-batch.json"),
          CommonParams.QT,
          "/reverseSearch",
          WRITE_TO_DOC_LIST_KEY,
          writeToDocList
        };

    QueryResponse response = query(params);
    System.out.println("Response = " + response);
    if (writeToDocList) {
      assertEquals(3, ((SolrDocumentList) response.getResponse().get("response")).size());
    }
    validate(response, 0, List.of("0"), writeToDocList);
  }

  @Test
  public void testNoDanglingDecomposition() throws Exception {
    index(
        id,
        Integer.toString(0),
        MonitorFields.MONITOR_QUERY,
        "{!xmlparser}<DisjunctionMaxQuery><TermQuery fieldName=\"content0_s\">elevator</TermQuery><TermQuery fieldName=\"content1_s\">lift</TermQuery></DisjunctionMaxQuery>");
    commit();
    handle.clear();
    handle.put("responseHeader", SKIP);
    handle.put("response", SKIP);

    final boolean writeToDocList = supportsWriteToDocList();
    Object[] params =
        new Object[] {
          CommonParams.SORT,
          id + " desc",
          CommonParams.JSON,
          read("/monitor/dangling-test-single-doc-batch.json"),
          CommonParams.QT,
          "/reverseSearch",
          WRITE_TO_DOC_LIST_KEY,
          writeToDocList
        };

    QueryResponse response = query(params);
    System.out.println("Response = " + response);
    if (writeToDocList) {
      assertEquals(1, ((SolrDocumentList) response.getResponse().get("response")).size());
    }
    validate(response, 0, List.of("0"), writeToDocList);

    index(
        id,
        Integer.toString(0),
        MonitorFields.MONITOR_QUERY,
        "{!xmlparser}<DisjunctionMaxQuery><TermQuery fieldName=\"content0_s\">elevator</TermQuery></DisjunctionMaxQuery>");
    commit();
    response = query(params);
    System.out.println("Response = " + response);
    if (writeToDocList) {
      assertEquals(0, ((SolrDocumentList) response.getResponse().get("response")).size());
    }
    validate(response, 0, List.of(), writeToDocList);
  }

  @Test
  public void testNotQuery() throws Exception {
    index(
        id,
        Integer.toString(0),
        MonitorFields.MONITOR_QUERY,
        "*:* -content0_s:\"elevator stairs\"");
    index(id, Integer.toString(1), MonitorFields.MONITOR_QUERY, "*:* -content_s:\"candy canes\"");
    commit();
    handle.clear();
    handle.put("responseHeader", SKIP);
    handle.put("response", SKIP);

    final boolean writeToDocList = supportsWriteToDocList();
    Object[] params =
        new Object[] {
          CommonParams.SORT,
          id + " desc",
          CommonParams.JSON,
          read("/monitor/single-doc-batch.json"),
          CommonParams.QT,
          "/reverseSearch",
          WRITE_TO_DOC_LIST_KEY,
          writeToDocList
        };

    QueryResponse response = query(params);
    System.out.println("Response = " + response);
    validate(response, 0, List.of("1"), writeToDocList);
  }

  @Test
  public void testWildCardQuery() throws Exception {
    index(id, Integer.toString(0), MonitorFields.MONITOR_QUERY, "content_s:te*");
    index(id, Integer.toString(1), MonitorFields.MONITOR_QUERY, "content_s:tes*");
    index(id, Integer.toString(2), MonitorFields.MONITOR_QUERY, "content_s:test*");
    index(id, Integer.toString(3), MonitorFields.MONITOR_QUERY, "content_s:tex*");
    index(id, Integer.toString(4), MonitorFields.MONITOR_QUERY, "content_s:tests*");
    commit();
    handle.clear();
    handle.put("responseHeader", SKIP);
    handle.put("response", SKIP);

    final boolean writeToDocList = supportsWriteToDocList();
    Object[] params =
        new Object[] {
          CommonParams.SORT,
          id + " desc",
          CommonParams.JSON,
          read("/monitor/multi-doc-batch.json"),
          CommonParams.QT,
          "/reverseSearch",
          WRITE_TO_DOC_LIST_KEY,
          writeToDocList
        };

    QueryResponse response = query(params);
    if (writeToDocList) {
      assertEquals(3, ((SolrDocumentList) response.getResponse().get("response")).size());
    }
    System.out.println("Response = " + response);
    validate(response, 3, List.of("0", "1", "2"), writeToDocList);
    validate(response, 4, List.of("0", "1", "2"), writeToDocList);
  }

  @Test
  public void testDeleteByQueryId() throws Exception {
    index(
        id,
        Integer.toString(0),
        MonitorFields.MONITOR_QUERY,
        "{!xmlparser}<DisjunctionMaxQuery><TermQuery fieldName=\"content0_s\">elevator</TermQuery><TermQuery fieldName=\"content1_s\">lift</TermQuery></DisjunctionMaxQuery>");
    commit();
    handle.clear();
    handle.put("responseHeader", SKIP);
    handle.put("response", SKIP);

    final boolean writeToDocList = supportsWriteToDocList();
    Object[] params =
        new Object[] {
          CommonParams.SORT,
          id + " desc",
          CommonParams.JSON,
          read("/monitor/dangling-test-single-doc-batch.json"),
          CommonParams.QT,
          "/reverseSearch",
          WRITE_TO_DOC_LIST_KEY,
          writeToDocList
        };

    QueryResponse response = query(params);
    if (writeToDocList) {
      assertEquals(1, ((SolrDocumentList) response.getResponse().get("response")).size());
    }
    validate(response, 0, List.of("0"), writeToDocList);

    del(MonitorFields.QUERY_ID + ":0");
    commit();
    response = query(CommonParams.Q, "*:*");
    assertEquals(0, ((SolrDocumentList) response.getResponse().get("response")).size());
  }

  void validate(QueryResponse response, int doc, Object expectedValue, boolean writeToDocList) {
    var monitorQueries = monitorQueries(response, doc);
    assertEquals(expectedValue, monitorQueries);
    if (writeToDocList) {
      validateDocList(response, doc, expectedValue);
    }
  }

  void validate(
      QueryResponse response, int doc, int query, Object expectedValue, boolean writeToDocList) {
    var monitorQueries = monitorQueries(response, doc);
    assertTrue(monitorQueries.size() > query);
    assertEquals(expectedValue, monitorQueries.get(query));
    if (writeToDocList) {
      validateDocList(response, doc, expectedValue);
    }
  }

  void validateDocList(QueryResponse response, int doc, Object expectedValue) {
    SolrDocumentList actualValues = (SolrDocumentList) response.getResponse().get("response");
    // minimal checks only so far; TODO: make this more comprehensive
    if (expectedValue instanceof List) {
      List<Object> expectedValues = (List) expectedValue;
      if (expectedValues.size() == 1 && actualValues.size() <= 2) {
        assertEquals(
            expectedValues.get(0),
            actualValues.get(actualValues.size() - 1 - doc).getFieldValue(MonitorFields.QUERY_ID));
      }
    }
  }

  List<Object> monitorQueries(QueryResponse response, int doc) {
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
