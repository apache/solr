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

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.lucene.monitor.MonitorFields;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class MonitorSolrQueryTest extends BaseDistributedSearchTestCase {

  @Test
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
          read("/monitor/doc1.json"),
          CommonParams.QT,
          "/reverseSearch"
        };
    QueryResponse response = query(params);
    System.out.println("Response = " + response);
    validateDocList(response, List.of("7"));
  }

  @Test
  @ShardsFixed(num = 3)
  public void testMonitorQueryWithUpdate() throws Exception {
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
          read("/monitor/doc0.json"),
          CommonParams.QT,
          "/reverseSearch"
        };
    QueryResponse response = query(params);
    System.out.println("Response = " + response);
    validateDocList(response, List.of("4", "1"));
    assertEquals(2, ((SolrDocumentList) response.getResponse().get("response")).size());

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

    params =
        new Object[] {
          CommonParams.SORT,
          id + " desc",
          CommonParams.JSON,
          read("/monitor/doc2.json"),
          CommonParams.QT,
          "/reverseSearch"
        };
    response = query(params);
    validateDocList(response, List.of("6"));
  }

  @Test
  public void testDefaultParser() throws Exception {
    index(id, Integer.toString(0), MonitorFields.MONITOR_QUERY, "content_s:\"elevator stairs\"");
    index(id, Integer.toString(1), MonitorFields.MONITOR_QUERY, "content_s:\"something else\"");
    commit();
    handle.clear();
    handle.put("responseHeader", SKIP);
    handle.put("response", SKIP);

    Object[] params =
        new Object[] {
          CommonParams.SORT,
          id + " desc",
          CommonParams.JSON,
          read("/monitor/multi-value-doc.json"),
          CommonParams.QT,
          "/reverseSearch"
        };

    QueryResponse response = query(params);
    System.out.println("Response = " + response);
    validateDocList(response, List.of("1", "0"));
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

    Object[] params =
        new Object[] {
          CommonParams.SORT,
          id + " desc",
          CommonParams.JSON,
          read("/monitor/elevator-doc.json"),
          CommonParams.QT,
          "/reverseSearch"
        };

    QueryResponse response = query(params);
    System.out.println("Response = " + response);
    validateDocList(response, List.of("0"));
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

    Object[] params =
        new Object[] {
          CommonParams.SORT,
          id + " desc",
          CommonParams.JSON,
          read("/monitor/dangling-test-single-doc-batch.json"),
          CommonParams.QT,
          "/reverseSearch"
        };

    QueryResponse response = query(params);
    System.out.println("Response = " + response);
    validateDocList(response, List.of("0"));

    index(
        id,
        Integer.toString(0),
        MonitorFields.MONITOR_QUERY,
        "{!xmlparser}<DisjunctionMaxQuery><TermQuery fieldName=\"content0_s\">elevator</TermQuery></DisjunctionMaxQuery>");
    commit();
    response = query(params);
    System.out.println("Response = " + response);
    validateDocList(response, List.of());
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

    Object[] params =
        new Object[] {
          CommonParams.SORT,
          id + " desc",
          CommonParams.JSON,
          read("/monitor/elevator-doc.json"),
          CommonParams.QT,
          "/reverseSearch"
        };

    QueryResponse response = query(params);
    System.out.println("Response = " + response);
    validateDocList(response, List.of("1"));
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

    Object[] params =
        new Object[] {
          CommonParams.SORT,
          id + " desc",
          CommonParams.JSON,
          read("/monitor/multi-value-doc.json"),
          CommonParams.QT,
          "/reverseSearch"
        };

    QueryResponse response = query(params);
    System.out.println("Response = " + response);
    validateDocList(response, List.of("2", "1", "0"));
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

    Object[] params =
        new Object[] {
          CommonParams.SORT,
          id + " desc",
          CommonParams.JSON,
          read("/monitor/dangling-test-single-doc-batch.json"),
          CommonParams.QT,
          "/reverseSearch"
        };

    QueryResponse response = query(params);
    validateDocList(response, List.of("0"));

    del(MonitorFields.QUERY_ID + ":0");
    commit();
    response = query(CommonParams.Q, "*:*");
    validateDocList(response, List.of());
  }

  void validateDocList(QueryResponse response, Object expectedValue) {
    SolrDocumentList actualValues = (SolrDocumentList) response.getResponse().get("response");
    // minimal checks only so far; TODO: make this more comprehensive
    if (expectedValue instanceof List) {
      List<Object> expectedValues = (List) expectedValue;
      int i = 0;
      assertEquals(expectedValues.size(), actualValues.size());
      for (var ev : expectedValues) {
        assertEquals(ev, actualValues.get(i).getFieldValue(MonitorFields.QUERY_ID));
        i++;
      }
    }
  }

  protected String read(String resourceName) throws IOException, URISyntaxException {
    final URL url = getClass().getResource(resourceName);
    return Files.readString(Path.of(url.toURI()), StandardCharsets.UTF_8);
  }
}
