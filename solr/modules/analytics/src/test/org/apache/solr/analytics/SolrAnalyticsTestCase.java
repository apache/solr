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
package org.apache.solr.analytics;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.solr.JSONTestUtil;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Utils;
import org.junit.BeforeClass;

public class SolrAnalyticsTestCase extends SolrCloudTestCase {
  private static final double DEFAULT_DELTA = .0000001;

  protected static final String COLLECTIONORALIAS = "collection1";
  protected static final int TIMEOUT = DEFAULT_TIMEOUT;
  protected static final String id = "id";

  private static UpdateRequest cloudReq;

  @BeforeClass
  public static void setupCollection() throws Exception {
    // Single-sharded core
    initCore("solrconfig-analytics.xml", "schema-analytics.xml");
    h.update("<delete><query>*:*</query></delete>");

    // Solr Cloud
    configureCluster(4).addConfig("conf", configset("cloud-analytics")).configure();

    CollectionAdminRequest.createCollection(COLLECTIONORALIAS, "conf", 2, 1)
        .process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(
        COLLECTIONORALIAS, cluster.getZkStateReader(), false, true, TIMEOUT);

    new UpdateRequest().deleteByQuery("*:*").commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    cloudReq = new UpdateRequest();
  }

  protected static void cleanIndex() throws Exception {
    h.update("<delete><query>*:*</query></delete>");

    new UpdateRequest().deleteByQuery("*:*").commit(cluster.getSolrClient(), COLLECTIONORALIAS);
  }

  protected static void addDoc(List<String> fieldsAndValues) {
    assertU(adoc(fieldsAndValues.toArray(new String[0])));
    cloudReq.add(fieldsAndValues.toArray(new String[0]));
  }

  protected static void commitDocs() {
    assertU(commit());
    try {
      cloudReq.commit(cluster.getSolrClient(), COLLECTIONORALIAS);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    cloudReq = new UpdateRequest();
  }

  public static final int INT = 7;
  public static final int LONG = 2;
  public static final int FLOAT = 6;
  public static final int DOUBLE = 5;
  public static final int DATE = 3;
  public static final int STRING = 4;
  public static final int NUM_LOOPS = 20;

  protected static void populateDocsForAnalyticsTests() {
    for (int j = 0; j < NUM_LOOPS; ++j) {
      int i = j % INT;
      long l = j % LONG;
      float f = j % FLOAT;
      double d = j % DOUBLE;
      String dt = (1800 + j % DATE) + "-12-31T23:59:59Z";
      String dtm = (1800 + j % DATE + 10) + "-12-31T23:59:59Z";
      String s = "str" + (j % STRING);
      List<String> fields = new ArrayList<>();
      fields.add("id");
      fields.add("1000" + j);

      if (i != 0) {
        fields.add("int_i");
        fields.add("" + i);
        fields.add("int_im");
        fields.add("" + i);
        fields.add("int_im");
        fields.add("" + (i + 10));
      }

      if (l != 0l) {
        fields.add("long_l");
        fields.add("" + l);
        fields.add("long_lm");
        fields.add("" + l);
        fields.add("long_lm");
        fields.add("" + (l + 10));
      }

      if (f != 0.0f) {
        fields.add("float_f");
        fields.add("" + f);
        fields.add("float_fm");
        fields.add("" + f);
        fields.add("float_fm");
        fields.add("" + (f + 10));
      }

      if (d != 0.0d) {
        fields.add("double_d");
        fields.add("" + d);
        fields.add("double_dm");
        fields.add("" + d);
        fields.add("double_dm");
        fields.add("" + (d + 10));
      }

      if ((j % DATE) != 0) {
        fields.add("date_dt");
        fields.add(dt);
        fields.add("date_dtm");
        fields.add(dt);
        fields.add("date_dtm");
        fields.add(dtm);
      }

      if ((j % STRING) != 0) {
        fields.add("string_s");
        fields.add(s);
        fields.add("string_sm");
        fields.add(s);
        fields.add("string_sm");
        fields.add(s + "_second");
      }

      addDoc(fields);
    }
    commitDocs();
  }

  private void testResults(SolrParams params, String analyticsRequest, String... tests) {
    String coreJson = queryCoreJson(params);
    Object cloudObj = queryCloudObject(params);

    for (String test : tests) {
      if (test == null || test.length() == 0) continue;
      // Single-Sharded
      String err = null;
      try {
        err = JSONTestUtil.match(coreJson, test, DEFAULT_DELTA);
      } catch (Exception e) {
        err = e.getMessage();
      } finally {
        assertNull(
            "query failed JSON validation. test= Single-Sharded Collection"
                + "\n error="
                + err
                + "\n expected ="
                + test
                + "\n response = "
                + coreJson
                + "\n analyticsRequest = "
                + analyticsRequest,
            err);
      }

      // Cloud
      err = null;
      try {
        err = JSONTestUtil.matchObj(cloudObj, test, DEFAULT_DELTA);
      } catch (Exception e) {
        err = e.getMessage();
      } finally {
        assertNull(
            "query failed JSON validation. test= Solr Cloud Collection"
                + "\n error="
                + err
                + "\n expected ="
                + test
                + "\n response = "
                + Utils.toJSONString(cloudObj)
                + "\n analyticsRequest = "
                + analyticsRequest,
            err);
      }
    }
  }

  private String queryCoreJson(SolrParams params) {
    try {
      return JQ(req(params));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Object queryCloudObject(SolrParams params) {
    QueryResponse resp;
    try {
      cluster.waitForAllNodes(10);
      QueryRequest qreq = new QueryRequest(params);
      resp = qreq.process(cluster.getSolrClient(), COLLECTIONORALIAS);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return convertDatesToStrings(resp.getResponse().asShallowMap());
  }

  protected void testAnalytics(String analyticsRequest, String... tests)
      throws IOException, InterruptedException, SolrServerException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "*:*");
    params.set("indent", "true");
    params.set("rows", "0");
    params.set("wt", "json");

    params.set("analytics", analyticsRequest);

    String[] revisedTests =
        Arrays.stream(tests)
            .map(test -> "analytics_response/" + test)
            .toArray(size -> new String[size]);
    testResults(params, analyticsRequest, revisedTests);
  }

  protected void testExpressions(Map<String, ETP> expressions) throws Exception {
    StringBuilder analyticsRequest = new StringBuilder("{ \"expressions\": {");
    String expressionsStr =
        expressions.entrySet().stream()
            .map(entry -> '"' + entry.getKey() + "\":\"" + entry.getValue().expression + '"')
            .reduce((a, b) -> a + ',' + b)
            .orElseGet(() -> "");
    analyticsRequest.append(expressionsStr);
    analyticsRequest.append("}}");

    String results =
        expressions.entrySet().stream()
            .map(entry -> '"' + entry.getKey() + "\":" + entry.getValue().expectedResultStr())
            .reduce((a, b) -> a + ',' + b)
            .orElseGet(() -> "");

    testAnalytics(analyticsRequest.toString(), "results=={" + results + ", \"_UNORDERED_\":true}");
  }

  protected void testGrouping(
      String grouping,
      Map<String, String> expressions,
      Map<String, String> facets,
      Map<String, List<FVP>> results,
      boolean sortAscending)
      throws Exception {
    testGroupingSorted(
        grouping,
        expressions,
        facets,
        results,
        ", 'sort': { 'criteria' : [{'type': 'facetvalue', 'direction': '"
            + (sortAscending ? "ascending" : "descending")
            + "'}]}",
        (fvp1, fvp2) -> fvp1.facetValue.compareTo(fvp2.facetValue),
        sortAscending);
  }

  @SuppressWarnings("unchecked")
  protected void testGrouping(
      String grouping,
      Map<String, String> expressions,
      Map<String, String> facets,
      Map<String, List<FVP>> results,
      String sortExpression,
      boolean sortAscending)
      throws Exception {
    testGroupingSorted(
        grouping,
        expressions,
        facets,
        results,
        ", 'sort': { 'criteria' : [{'type': 'expression', 'expression': '"
            + sortExpression
            + "', 'direction': '"
            + (sortAscending ? "ascending" : "descending")
            + "'}]}",
        (fvp1, fvp2) ->
            fvp1.expectedResults
                .get(sortExpression)
                .compareTo(fvp2.expectedResults.get(sortExpression)),
        sortAscending);
  }

  protected void testGrouping(
      String grouping,
      Map<String, String> expressions,
      Map<String, String> facets,
      Map<String, List<FVP>> results)
      throws Exception {
    testGroupingSorted(
        grouping, expressions, facets, results, "", (fvp1, fvp2) -> fvp1.compareTo(fvp2), true);
  }

  private void testGroupingSorted(
      String grouping,
      Map<String, String> expressions,
      Map<String, String> facets,
      Map<String, List<FVP>> results,
      String sort,
      Comparator<FVP> comparator,
      boolean sortAscending)
      throws Exception {
    StringBuilder analyticsRequest =
        new StringBuilder("{ \"groupings\": { \"" + grouping + "\" : { \"expressions\" : {");
    String expressionsStr =
        expressions.entrySet().stream()
            .map(entry -> '"' + entry.getKey() + "\":\"" + entry.getValue() + '"')
            .collect(Collectors.joining(" , "));
    analyticsRequest.append(expressionsStr);
    analyticsRequest.append("}, \"facets\": {");
    String facetsStr =
        facets.entrySet().stream()
            .map(
                entry ->
                    '"'
                        + entry.getKey()
                        + "\":"
                        + entry.getValue().replaceFirst("}\\s*$", sort)
                        + "}")
            .collect(Collectors.joining(" , "));
    analyticsRequest.append(facetsStr);
    analyticsRequest.append("}}}}");

    String groupingResults =
        results.entrySet().stream()
            .map(
                facet -> {
                  String resultList =
                      facet.getValue().stream()
                          .sorted(sortAscending ? comparator : comparator.reversed())
                          .map(fvp -> fvp.toJsonResults())
                          .collect(Collectors.joining(" , "));
                  return '"' + facet.getKey() + "\" : [ " + resultList + " ]";
                })
            .collect(Collectors.joining(" , "));

    testAnalytics(
        analyticsRequest.toString(),
        "groupings/" + grouping + "=={" + groupingResults + ", \"_UNORDERED_\":true}");
  }

  private static String resultToJson(Object result) {
    if (result instanceof String) {
      return '"' + result.toString() + '"';
    }
    return result.toString();
  }

  /*
   * Expression Test Pair, contains the expression and the expected result
   */
  protected static class ETP {
    final String expression;
    final Object expectedResult;

    public ETP(String expression, Object expectedResult) {
      this.expression = expression;
      this.expectedResult = expectedResult;
    }

    public String expectedResultStr() {
      if (expectedResult instanceof String) {
        return '"' + expectedResult.toString() + '"';
      }
      return expectedResult.toString();
    }
  }

  /*
   * FacetValuePair, contains the expression and the expected result
   */
  @SuppressWarnings("rawtypes")
  protected static class FVP implements Comparable<FVP> {
    private final int order;
    public final String facetValue;
    public final Map<String, Comparable> expectedResults;

    public FVP(int order, String facetValue, Map<String, Comparable> expectedResults) {
      this.order = order;
      this.facetValue = facetValue;
      this.expectedResults = expectedResults;
    }

    public String toJsonResults() {
      String valueResults =
          expectedResults.entrySet().stream()
              .map(result -> '"' + result.getKey() + "\":" + resultToJson(result.getValue()))
              .collect(Collectors.joining(" , "));
      return "{ \"value\" : \""
          + facetValue
          + "\", \"results\": { "
          + valueResults
          + ", \"_UNORDERED_\":true } }";
    }

    @Override
    public int compareTo(FVP other) {
      return Integer.compare(order, other.order);
    }
  }

  @SuppressWarnings("unchecked")
  protected static Object convertDatesToStrings(Object value) {
    if (value instanceof Date) {
      return Instant.ofEpochMilli(((Date) value).getTime()).toString();
    } else if (value instanceof Map) {
      ((Map<String, Object>) value).replaceAll((key, obj) -> convertDatesToStrings(obj));
    } else if (value instanceof List) {
      ((List<Object>) value).replaceAll(obj -> convertDatesToStrings(obj));
    }
    return value;
  }
}
