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
package org.apache.solr.analytics.legacy.facet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Node;

public class LegacyFieldFacetTest extends LegacyAbstractAnalyticsFacetTest {
  static String fileName = "fieldFacets.txt";

  public static final int INT = 71;
  public static final int LONG = 36;
  public static final int LONGM = 50;
  public static final int FLOAT = 73;
  public static final int FLOATM = 84;
  public static final int DOUBLE = 49;
  public static final int DATE = 12;
  public static final int DATEM = 30;
  public static final int STRING = 28;
  public static final int STRINGM = 40;
  public static final int NUM_LOOPS = 100;

  // INT
  private static ArrayList<ArrayList<Integer>> intDateTestStart;
  private static ArrayList<Long> intDateTestMissing;
  private static ArrayList<ArrayList<Integer>> intStringTestStart;
  private static ArrayList<Long> intStringTestMissing;

  // LONG
  private static ArrayList<ArrayList<Long>> longDateTestStart;
  private static ArrayList<Long> longDateTestMissing;
  private static ArrayList<ArrayList<Long>> longStringTestStart;
  private static ArrayList<Long> longStringTestMissing;

  // FLOAT
  private static ArrayList<ArrayList<Float>> floatDateTestStart;
  private static ArrayList<Long> floatDateTestMissing;
  private static ArrayList<ArrayList<Float>> floatStringTestStart;
  private static ArrayList<Long> floatStringTestMissing;

  // DOUBLE
  private static ArrayList<ArrayList<Double>> doubleDateTestStart;
  private static ArrayList<Long> doubleDateTestMissing;
  private static ArrayList<ArrayList<Double>> doubleStringTestStart;
  private static ArrayList<Long> doubleStringTestMissing;

  // DATE
  private static ArrayList<ArrayList<String>> dateIntTestStart;
  private static ArrayList<Long> dateIntTestMissing;
  private static ArrayList<ArrayList<String>> dateLongTestStart;
  private static ArrayList<Long> dateLongTestMissing;

  // String
  private static ArrayList<ArrayList<String>> stringIntTestStart;
  private static ArrayList<Long> stringIntTestMissing;
  private static ArrayList<ArrayList<String>> stringLongTestStart;
  private static ArrayList<Long> stringLongTestMissing;

  // Multi-Valued
  private static ArrayList<ArrayList<Integer>> multiLongTestStart;
  private static ArrayList<Long> multiLongTestMissing;
  private static ArrayList<ArrayList<Integer>> multiStringTestStart;
  private static ArrayList<Long> multiStringTestMissing;
  private static ArrayList<ArrayList<Integer>> multiDateTestStart;
  private static ArrayList<Long> multiDateTestMissing;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-analytics.xml", "schema-analytics.xml");
    h.update("<delete><query>*:*</query></delete>");

    defaults.put("int", 0);
    defaults.put("long", 0L);
    defaults.put("float", (float) 0);
    defaults.put("double", (double) 0);
    defaults.put("date", "1800-12-31T23:59:59Z");
    defaults.put("string", "str0");

    // INT
    intDateTestStart = new ArrayList<>();
    intDateTestMissing = new ArrayList<>();
    intStringTestStart = new ArrayList<>();
    intStringTestMissing = new ArrayList<>();

    // LONG
    longDateTestStart = new ArrayList<>();
    longDateTestMissing = new ArrayList<>();
    longStringTestStart = new ArrayList<>();
    longStringTestMissing = new ArrayList<>();

    // FLOAT
    floatDateTestStart = new ArrayList<>();
    floatDateTestMissing = new ArrayList<>();
    floatStringTestStart = new ArrayList<>();
    floatStringTestMissing = new ArrayList<>();

    // DOUBLE
    doubleDateTestStart = new ArrayList<>();
    doubleDateTestMissing = new ArrayList<>();
    doubleStringTestStart = new ArrayList<>();
    doubleStringTestMissing = new ArrayList<>();

    // DATE
    dateIntTestStart = new ArrayList<>();
    dateIntTestMissing = new ArrayList<>();
    dateLongTestStart = new ArrayList<>();
    dateLongTestMissing = new ArrayList<>();

    // String
    stringIntTestStart = new ArrayList<>();
    stringIntTestMissing = new ArrayList<>();
    stringLongTestStart = new ArrayList<>();
    stringLongTestMissing = new ArrayList<>();

    // Multi-Valued
    multiLongTestStart = new ArrayList<>();
    multiLongTestMissing = new ArrayList<>();
    multiStringTestStart = new ArrayList<>();
    multiStringTestMissing = new ArrayList<>();
    multiDateTestStart = new ArrayList<>();
    multiDateTestMissing = new ArrayList<>();

    boolean multiCanHaveDuplicates = Boolean.getBoolean(NUMERIC_POINTS_SYSPROP);

    for (int j = 0; j < NUM_LOOPS; ++j) {
      int i = j % INT;
      long l = j % LONG;
      long lm = j % LONGM;
      float f = j % FLOAT;
      double d = j % DOUBLE;
      int dt = j % DATE;
      int dtm = j % DATEM;
      int s = j % STRING;
      int sm = j % STRINGM;

      List<String> fields = new ArrayList<>();
      fields.add("id");
      fields.add("1000" + j);

      if (i != 0) {
        fields.add("int_id");
        fields.add("" + i);
      }
      if (l != 0l) {
        fields.add("long_ld");
        fields.add("" + l);
        fields.add("long_ldm");
        fields.add("" + l);
      }
      if (lm != 0l) {
        fields.add("long_ldm");
        fields.add("" + lm);
      }
      if (f != 0.0f) {
        fields.add("float_fd");
        fields.add("" + f);
      }
      if (d != 0.0d) {
        fields.add("double_dd");
        fields.add("" + d);
      }
      if (dt != 0) {
        fields.add("date_dtd");
        fields.add((1800 + dt) + "-12-31T23:59:59Z");
        fields.add("date_dtdm");
        fields.add((1800 + dt) + "-12-31T23:59:59Z");
      }
      if (dtm != 0) {
        fields.add("date_dtdm");
        fields.add((1800 + dtm) + "-12-31T23:59:59Z");
      }
      if (s != 0) {
        fields.add("string_sd");
        fields.add("str" + s);
        fields.add("string_sdm");
        fields.add("str" + s);
      }
      if (sm != 0) {
        fields.add("string_sdm");
        fields.add("str" + sm);
      }
      assertU(adoc(fields.toArray(new String[0])));

      if (dt != 0) {
        // Dates
        if (j - DATE < 0) {
          ArrayList<Integer> list1 = new ArrayList<>();
          if (i != 0) {
            list1.add(i);
            intDateTestMissing.add(0l);
          } else {
            intDateTestMissing.add(1l);
          }
          intDateTestStart.add(list1);
          ArrayList<Long> list2 = new ArrayList<>();
          if (l != 0l) {
            list2.add(l);
            longDateTestMissing.add(0l);
          } else {
            longDateTestMissing.add(1l);
          }
          longDateTestStart.add(list2);
          ArrayList<Float> list3 = new ArrayList<>();
          if (f != 0.0f) {
            list3.add(f);
            floatDateTestMissing.add(0l);
          } else {
            floatDateTestMissing.add(1l);
          }
          floatDateTestStart.add(list3);
          ArrayList<Double> list4 = new ArrayList<>();
          if (d != 0.0d) {
            list4.add(d);
            doubleDateTestMissing.add(0l);
          } else {
            doubleDateTestMissing.add(1l);
          }
          doubleDateTestStart.add(list4);
          ArrayList<Integer> list5 = new ArrayList<>();
          if (i != 0) {
            list5.add(i);
            multiDateTestMissing.add(0l);
          } else {
            multiDateTestMissing.add(1l);
          }
          multiDateTestStart.add(list5);
        } else {
          if (i != 0) intDateTestStart.get(dt - 1).add(i);
          else increment(intDateTestMissing, dt - 1);
          if (l != 0l) longDateTestStart.get(dt - 1).add(l);
          else increment(longDateTestMissing, dt - 1);
          if (f != 0.0f) floatDateTestStart.get(dt - 1).add(f);
          else increment(floatDateTestMissing, dt - 1);
          if (d != 0.0d) doubleDateTestStart.get(dt - 1).add(d);
          else increment(doubleDateTestMissing, dt - 1);
          if (i != 0) multiDateTestStart.get(dt - 1).add(i);
          else increment(multiDateTestMissing, dt - 1);
        }
      }

      if (dtm != 0) {
        if (j - DATEM < 0 && dtm != dt) {
          ArrayList<Integer> list1 = new ArrayList<>();
          if (i != 0) {
            list1.add(i);
            multiDateTestMissing.add(0l);
          } else {
            multiDateTestMissing.add(1l);
          }
          multiDateTestStart.add(list1);
        } else if (dtm != dt || multiCanHaveDuplicates) {
          if (i != 0) multiDateTestStart.get(dtm - 1).add(i);
          else increment(multiDateTestMissing, dtm - 1);
        }
      }

      if (s != 0) {
        // Strings
        if (j - STRING < 0) {
          ArrayList<Integer> list1 = new ArrayList<>();
          if (i != 0) {
            list1.add(i);
            intStringTestMissing.add(0l);
          } else {
            intStringTestMissing.add(1l);
          }
          intStringTestStart.add(list1);
          ArrayList<Long> list2 = new ArrayList<>();
          if (l != 0l) {
            list2.add(l);
            longStringTestMissing.add(0l);
          } else {
            longStringTestMissing.add(1l);
          }
          longStringTestStart.add(list2);
          ArrayList<Float> list3 = new ArrayList<>();
          if (f != 0.0f) {
            list3.add(f);
            floatStringTestMissing.add(0l);
          } else {
            floatStringTestMissing.add(1l);
          }
          floatStringTestStart.add(list3);
          ArrayList<Double> list4 = new ArrayList<>();
          if (d != 0.0d) {
            list4.add(d);
            doubleStringTestMissing.add(0l);
          } else {
            doubleStringTestMissing.add(1l);
          }
          doubleStringTestStart.add(list4);
          ArrayList<Integer> list5 = new ArrayList<>();
          if (i != 0) {
            list5.add(i);
            multiStringTestMissing.add(0l);
          } else {
            multiStringTestMissing.add(1l);
          }
          multiStringTestStart.add(list5);
        } else {
          if (i != 0) intStringTestStart.get(s - 1).add(i);
          else increment(intStringTestMissing, s - 1);
          if (l != 0l) longStringTestStart.get(s - 1).add(l);
          else increment(longStringTestMissing, s - 1);
          if (f != 0.0f) floatStringTestStart.get(s - 1).add(f);
          else increment(floatStringTestMissing, s - 1);
          if (d != 0.0d) doubleStringTestStart.get(s - 1).add(d);
          else increment(doubleStringTestMissing, s - 1);
          if (i != 0) multiStringTestStart.get(s - 1).add(i);
          else increment(multiStringTestMissing, s - 1);
        }
      }

      // Strings
      if (sm != 0) {
        if (j - STRINGM < 0 && sm != s) {
          ArrayList<Integer> list1 = new ArrayList<>();
          if (i != 0) {
            list1.add(i);
            multiStringTestMissing.add(0l);
          } else {
            multiStringTestMissing.add(1l);
          }
          multiStringTestStart.add(list1);
        } else if (sm != s) {
          if (i != 0) multiStringTestStart.get(sm - 1).add(i);
          else increment(multiStringTestMissing, sm - 1);
        }
      }

      // Int
      if (i != 0) {
        if (j - INT < 0) {
          ArrayList<String> list1 = new ArrayList<>();
          if (dt != 0) {
            list1.add((1800 + dt) + "-12-31T23:59:59Z");
            dateIntTestMissing.add(0l);
          } else {
            dateIntTestMissing.add(1l);
          }
          dateIntTestStart.add(list1);
          ArrayList<String> list2 = new ArrayList<>();
          if (s != 0) {
            list2.add("str" + s);
            stringIntTestMissing.add(0l);
          } else {
            stringIntTestMissing.add(1l);
          }
          stringIntTestStart.add(list2);
        } else {
          if (dt != 0) dateIntTestStart.get(i - 1).add((1800 + dt) + "-12-31T23:59:59Z");
          else increment(dateIntTestMissing, i - 1);
          if (s != 0) stringIntTestStart.get(i - 1).add("str" + s);
          else increment(stringIntTestMissing, i - 1);
        }
      }

      // Long
      if (l != 0) {
        if (j - LONG < 0) {
          ArrayList<String> list1 = new ArrayList<>();
          if (dt != 0) {
            list1.add((1800 + dt) + "-12-31T23:59:59Z");
            dateLongTestMissing.add(0l);
          } else {
            dateLongTestMissing.add(1l);
          }
          dateLongTestStart.add(list1);
          ArrayList<String> list2 = new ArrayList<>();
          if (s != 0) {
            list2.add("str" + s);
            stringLongTestMissing.add(0l);
          } else {
            stringLongTestMissing.add(1l);
          }
          stringLongTestStart.add(list2);
          ArrayList<Integer> list3 = new ArrayList<>();
          if (i != 0) {
            list3.add(i);
            multiLongTestMissing.add(0l);
          } else {
            multiLongTestMissing.add(1l);
          }
          multiLongTestStart.add(list3);
        } else {
          if (dt != 0) dateLongTestStart.get((int) l - 1).add((1800 + dt) + "-12-31T23:59:59Z");
          else increment(dateLongTestMissing, (int) l - 1);
          if (s != 0) stringLongTestStart.get((int) l - 1).add("str" + s);
          else increment(stringLongTestMissing, (int) l - 1);
          if (i != 0) multiLongTestStart.get((int) l - 1).add(i);
          else increment(multiLongTestMissing, (int) l - 1);
        }
      }

      // Long
      if (lm != 0) {
        if (j - LONGM < 0 && lm != l) {
          ArrayList<Integer> list1 = new ArrayList<>();
          if (i != 0) {
            list1.add(i);
            multiLongTestMissing.add(0l);
          } else {
            multiLongTestMissing.add(1l);
          }
          multiLongTestStart.add(list1);
        } else if (lm != l || multiCanHaveDuplicates) {
          if (i != 0) multiLongTestStart.get((int) lm - 1).add(i);
          else increment(multiLongTestMissing, (int) lm - 1);
        }
      }

      if (usually()) {
        assertU(commit()); // to have several segments
      }
    }

    assertU(commit());
    String[] reqFacetParamas = fileToStringArr(LegacyFieldFacetTest.class, fileName);
    String[] reqParamas = new String[reqFacetParamas.length + 2];
    System.arraycopy(reqFacetParamas, 0, reqParamas, 0, reqFacetParamas.length);
    reqParamas[reqFacetParamas.length] = "solr";
    reqParamas[reqFacetParamas.length + 1] = "asc";
    setResponse(h.query(request(reqFacetParamas)));
  }

  @Test
  public void timeAllowedTest() throws Exception {
    String query =
        "int_id: [0 TO "
            + random().nextInt(INT)
            + "] AND long_ld: [0 TO "
            + random().nextInt(LONG)
            + "]";
    try (SolrQueryRequest req =
        req(
            fileToStringArr(LegacyFieldFacetTest.class, fileName),
            "q",
            query,
            "timeAllowed",
            "0",
            "cache",
            "false")) {
      SolrQueryResponse resp = h.queryAndResponse(req.getParams().get(CommonParams.QT), req);

      assertEquals(resp.getResponseHeader().toString(), 0, resp.getResponseHeader().get("status"));
      Boolean partialResults =
          resp.getResponseHeader()
              .getBooleanArg(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY);
      assertNotNull(
          "No partial results header returned: " + resp.getResponseHeader().toString(),
          partialResults);
      assertTrue(
          "The request was not stopped halfway through, the partial results header was false",
          partialResults);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void sumTest() throws Exception {
    // Int Date
    ArrayList<Double> intDate = getDoubleList("sum", "fieldFacets", "date_dtd", "double", "int");
    ArrayList<Double> intDateTest = calculateNumberStat(intDateTestStart, "sum");
    assertEqualsList(getRawResponse(), intDate, intDateTest);
    // Int String
    ArrayList<Double> intString = getDoubleList("sum", "fieldFacets", "string_sd", "double", "int");
    ArrayList<Double> intStringTest = calculateNumberStat(intStringTestStart, "sum");
    assertEqualsList(getRawResponse(), intString, intStringTest);

    // Long Date
    ArrayList<Double> longDate = getDoubleList("sum", "fieldFacets", "date_dtd", "double", "long");
    ArrayList<Double> longDateTest = calculateNumberStat(longDateTestStart, "sum");
    assertEqualsList(getRawResponse(), longDate, longDateTest);
    // Long String
    ArrayList<Double> longString =
        getDoubleList("sum", "fieldFacets", "string_sd", "double", "long");
    ArrayList<Double> longStringTest = calculateNumberStat(longStringTestStart, "sum");
    assertEqualsList(getRawResponse(), longString, longStringTest);

    // Float Date
    ArrayList<Double> floatDate =
        getDoubleList("sum", "fieldFacets", "date_dtd", "double", "float");
    ArrayList<Double> floatDateTest = calculateNumberStat(floatDateTestStart, "sum");
    assertEqualsList(getRawResponse(), floatDate, floatDateTest);
    // Float String
    ArrayList<Double> floatString =
        getDoubleList("sum", "fieldFacets", "string_sd", "double", "float");
    ArrayList<Double> floatStringTest = calculateNumberStat(floatStringTestStart, "sum");
    assertEqualsList(getRawResponse(), floatString, floatStringTest);

    // Double Date
    ArrayList<Double> doubleDate =
        getDoubleList("sum", "fieldFacets", "date_dtd", "double", "double");
    ArrayList<Double> doubleDateTest = calculateNumberStat(doubleDateTestStart, "sum");
    assertEqualsList(getRawResponse(), doubleDate, doubleDateTest);
    // Double String
    ArrayList<Double> doubleString =
        getDoubleList("sum", "fieldFacets", "string_sd", "double", "double");
    ArrayList<Double> doubleStringTest = calculateNumberStat(doubleStringTestStart, "sum");
    assertEqualsList(getRawResponse(), doubleString, doubleStringTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void meanTest() throws Exception {
    // Int Date
    ArrayList<Double> intDate = getDoubleList("mean", "fieldFacets", "date_dtd", "double", "int");
    ArrayList<Double> intDateTest = calculateNumberStat(intDateTestStart, "mean");
    assertEqualsList(getRawResponse(), intDate, intDateTest);
    // Int String
    ArrayList<Double> intString =
        getDoubleList("mean", "fieldFacets", "string_sd", "double", "int");
    ArrayList<Double> intStringTest = calculateNumberStat(intStringTestStart, "mean");
    assertEqualsList(getRawResponse(), intString, intStringTest);

    // Long Date
    ArrayList<Double> longDate = getDoubleList("mean", "fieldFacets", "date_dtd", "double", "long");
    ArrayList<Double> longDateTest = calculateNumberStat(longDateTestStart, "mean");
    assertEqualsList(getRawResponse(), longDate, longDateTest);
    // Long String
    ArrayList<Double> longString =
        getDoubleList("mean", "fieldFacets", "string_sd", "double", "long");
    ArrayList<Double> longStringTest = calculateNumberStat(longStringTestStart, "mean");
    assertEqualsList(getRawResponse(), longString, longStringTest);

    // Float Date
    ArrayList<Double> floatDate =
        getDoubleList("mean", "fieldFacets", "date_dtd", "double", "float");
    ArrayList<Double> floatDateTest = calculateNumberStat(floatDateTestStart, "mean");
    assertEqualsList(getRawResponse(), floatDate, floatDateTest);
    // Float String
    ArrayList<Double> floatString =
        getDoubleList("mean", "fieldFacets", "string_sd", "double", "float");
    ArrayList<Double> floatStringTest = calculateNumberStat(floatStringTestStart, "mean");
    assertEqualsList(getRawResponse(), floatString, floatStringTest);

    // Double Date
    ArrayList<Double> doubleDate =
        getDoubleList("mean", "fieldFacets", "date_dtd", "double", "double");
    ArrayList<Double> doubleDateTest = calculateNumberStat(doubleDateTestStart, "mean");
    assertEqualsList(getRawResponse(), doubleDate, doubleDateTest);
    // Double String
    ArrayList<Double> doubleString =
        getDoubleList("mean", "fieldFacets", "string_sd", "double", "double");
    ArrayList<Double> doubleStringTest = calculateNumberStat(doubleStringTestStart, "mean");
    assertEqualsList(getRawResponse(), doubleString, doubleStringTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void stddevFacetAscTest() throws Exception {
    // Int Date
    ArrayList<Double> intDate = getDoubleList("stddev", "fieldFacets", "date_dtd", "double", "int");
    ArrayList<Double> intDateTest = calculateNumberStat(intDateTestStart, "stddev");
    checkStddevs(intDate, intDateTest);
    // Int String
    ArrayList<Double> intString =
        getDoubleList("stddev", "fieldFacets", "string_sd", "double", "int");
    ArrayList<Double> intStringTest = calculateNumberStat(intStringTestStart, "stddev");
    checkStddevs(intString, intStringTest);

    // Long Date
    ArrayList<Double> longDate =
        getDoubleList("stddev", "fieldFacets", "date_dtd", "double", "long");
    ArrayList<Double> longDateTest = calculateNumberStat(longDateTestStart, "stddev");
    checkStddevs(longDate, longDateTest);
    // Long String
    ArrayList<Double> longString =
        getDoubleList("stddev", "fieldFacets", "string_sd", "double", "long");
    ArrayList<Double> longStringTest = calculateNumberStat(longStringTestStart, "stddev");
    checkStddevs(longString, longStringTest);

    // Float Date
    ArrayList<Double> floatDate =
        getDoubleList("stddev", "fieldFacets", "date_dtd", "double", "float");
    ArrayList<Double> floatDateTest = calculateNumberStat(floatDateTestStart, "stddev");
    checkStddevs(floatDate, floatDateTest);
    // Float String
    ArrayList<Double> floatString =
        getDoubleList("stddev", "fieldFacets", "string_sd", "double", "float");
    ArrayList<Double> floatStringTest = calculateNumberStat(floatStringTestStart, "stddev");
    checkStddevs(floatString, floatStringTest);

    // Double Date
    ArrayList<Double> doubleDate =
        getDoubleList("stddev", "fieldFacets", "date_dtd", "double", "double");
    ArrayList<Double> doubleDateTest = calculateNumberStat(doubleDateTestStart, "stddev");
    checkStddevs(doubleDate, doubleDateTest);
    // Double String
    ArrayList<Double> doubleString =
        getDoubleList("stddev", "fieldFacets", "string_sd", "double", "double");
    ArrayList<Double> doubleStringTest = calculateNumberStat(doubleStringTestStart, "stddev");
    checkStddevs(doubleString, doubleStringTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void medianFacetAscTest() throws Exception {
    // Int Date
    ArrayList<Double> intDate = getDoubleList("median", "fieldFacets", "date_dtd", "double", "int");
    ArrayList<Double> intDateTest = calculateNumberStat(intDateTestStart, "median");
    assertEqualsList(getRawResponse(), intDate, intDateTest);
    // Int String
    ArrayList<Double> intString =
        getDoubleList("median", "fieldFacets", "string_sd", "double", "int");
    ArrayList<Double> intStringTest = calculateNumberStat(intStringTestStart, "median");
    assertEqualsList(getRawResponse(), intString, intStringTest);

    // Long Date
    ArrayList<Double> longDate =
        getDoubleList("median", "fieldFacets", "date_dtd", "double", "long");
    ArrayList<Double> longDateTest = calculateNumberStat(longDateTestStart, "median");
    assertEqualsList(getRawResponse(), longDate, longDateTest);
    // Long String
    ArrayList<Double> longString =
        getDoubleList("median", "fieldFacets", "string_sd", "double", "long");
    ArrayList<Double> longStringTest = calculateNumberStat(longStringTestStart, "median");
    assertEqualsList(getRawResponse(), longString, longStringTest);

    // Float Date
    ArrayList<Double> floatDate =
        getDoubleList("median", "fieldFacets", "date_dtd", "double", "float");
    ArrayList<Double> floatDateTest = calculateNumberStat(floatDateTestStart, "median");
    assertEqualsList(getRawResponse(), floatDate, floatDateTest);
    // Float String
    ArrayList<Double> floatString =
        getDoubleList("median", "fieldFacets", "string_sd", "double", "float");
    ArrayList<Double> floatStringTest = calculateNumberStat(floatStringTestStart, "median");
    assertEqualsList(getRawResponse(), floatString, floatStringTest);

    // Double Date
    ArrayList<Double> doubleDate =
        getDoubleList("median", "fieldFacets", "date_dtd", "double", "double");
    ArrayList<Double> doubleDateTest = calculateNumberStat(doubleDateTestStart, "median");
    assertEqualsList(getRawResponse(), doubleDate, doubleDateTest);
    // Double String
    ArrayList<Double> doubleString =
        getDoubleList("median", "fieldFacets", "string_sd", "double", "double");
    ArrayList<Double> doubleStringTest = calculateNumberStat(doubleStringTestStart, "median");
    assertEqualsList(getRawResponse(), doubleString, doubleStringTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void perc20Test() throws Exception {
    // Int Date
    ArrayList<Integer> intDate =
        getIntegerList("percentile_20n", "fieldFacets", "date_dtd", "int", "int");
    ArrayList<Integer> intDateTest =
        (ArrayList<Integer>) calculateStat(intDateTestStart, "perc_20");
    assertEqualsList(getRawResponse(), intDate, intDateTest);
    // Int String
    ArrayList<Integer> intString =
        getIntegerList("percentile_20n", "fieldFacets", "string_sd", "int", "int");
    ArrayList<Integer> intStringTest =
        (ArrayList<Integer>) calculateStat(intStringTestStart, "perc_20");
    assertEqualsList(getRawResponse(), intString, intStringTest);

    // Long Date
    ArrayList<Long> longDate =
        getLongList("percentile_20n", "fieldFacets", "date_dtd", "long", "long");
    ArrayList<Long> longDateTest = (ArrayList<Long>) calculateStat(longDateTestStart, "perc_20");
    assertEqualsList(getRawResponse(), longDate, longDateTest);
    // Long String
    ArrayList<Long> longString =
        getLongList("percentile_20n", "fieldFacets", "string_sd", "long", "long");
    ArrayList<Long> longStringTest =
        (ArrayList<Long>) calculateStat(longStringTestStart, "perc_20");
    assertEqualsList(getRawResponse(), longString, longStringTest);

    // Float Date
    ArrayList<Float> floatDate =
        getFloatList("percentile_20n", "fieldFacets", "date_dtd", "float", "float");
    ArrayList<Float> floatDateTest =
        (ArrayList<Float>) calculateStat(floatDateTestStart, "perc_20");
    assertEqualsList(getRawResponse(), floatDate, floatDateTest);
    // Float String
    ArrayList<Float> floatString =
        getFloatList("percentile_20n", "fieldFacets", "string_sd", "float", "float");
    ArrayList<Float> floatStringTest =
        (ArrayList<Float>) calculateStat(floatStringTestStart, "perc_20");
    assertEqualsList(getRawResponse(), floatString, floatStringTest);

    // Double Date
    ArrayList<Double> doubleDate =
        getDoubleList("percentile_20n", "fieldFacets", "date_dtd", "double", "double");
    ArrayList<Double> doubleDateTest =
        (ArrayList<Double>) calculateStat(doubleDateTestStart, "perc_20");
    assertEqualsList(getRawResponse(), doubleDate, doubleDateTest);
    // Double String
    ArrayList<Double> doubleString =
        getDoubleList("percentile_20n", "fieldFacets", "string_sd", "double", "double");
    ArrayList<Double> doubleStringTest =
        (ArrayList<Double>) calculateStat(doubleStringTestStart, "perc_20");
    assertEqualsList(getRawResponse(), doubleString, doubleStringTest);

    // Date Int
    ArrayList<String> dateInt =
        getStringList("percentile_20", "fieldFacets", "int_id", "date", "date");
    ArrayList<String> dateIntTest = (ArrayList<String>) calculateStat(dateIntTestStart, "perc_20");
    assertEqualsList(getRawResponse(), dateInt, dateIntTest);
    // Date Long
    ArrayList<String> dateString =
        getStringList("percentile_20", "fieldFacets", "long_ld", "date", "date");
    ArrayList<String> dateLongTest =
        (ArrayList<String>) calculateStat(dateLongTestStart, "perc_20");
    assertEqualsList(getRawResponse(), dateString, dateLongTest);

    // String Int
    ArrayList<String> stringInt =
        getStringList("percentile_20", "fieldFacets", "int_id", "str", "str");
    ArrayList<String> stringIntTest =
        (ArrayList<String>) calculateStat(stringIntTestStart, "perc_20");
    assertEqualsList(getRawResponse(), stringInt, stringIntTest);
    // String Long
    ArrayList<String> stringLong =
        getStringList("percentile_20", "fieldFacets", "long_ld", "str", "str");
    ArrayList<String> stringLongTest =
        (ArrayList<String>) calculateStat(stringLongTestStart, "perc_20");
    assertEqualsList(getRawResponse(), stringLong, stringLongTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void perc60Test() throws Exception {
    // Int Date
    ArrayList<Integer> intDate =
        getIntegerList("percentile_60n", "fieldFacets", "date_dtd", "int", "int");
    ArrayList<Integer> intDateTest =
        (ArrayList<Integer>) calculateStat(intDateTestStart, "perc_60");
    assertEqualsList(getRawResponse(), intDate, intDateTest);
    // Int String
    ArrayList<Integer> intString =
        getIntegerList("percentile_60n", "fieldFacets", "string_sd", "int", "int");
    ArrayList<Integer> intStringTest =
        (ArrayList<Integer>) calculateStat(intStringTestStart, "perc_60");
    assertEqualsList(getRawResponse(), intString, intStringTest);

    // Long Date
    ArrayList<Long> longDate =
        getLongList("percentile_60n", "fieldFacets", "date_dtd", "long", "long");
    ArrayList<Long> longDateTest = (ArrayList<Long>) calculateStat(longDateTestStart, "perc_60");
    assertEqualsList(getRawResponse(), longDate, longDateTest);
    // Long String
    ArrayList<Long> longString =
        getLongList("percentile_60n", "fieldFacets", "string_sd", "long", "long");
    ArrayList<Long> longStringTest =
        (ArrayList<Long>) calculateStat(longStringTestStart, "perc_60");
    assertEqualsList(getRawResponse(), longString, longStringTest);

    // Float Date
    ArrayList<Float> floatDate =
        getFloatList("percentile_60n", "fieldFacets", "date_dtd", "float", "float");
    ArrayList<Float> floatDateTest =
        (ArrayList<Float>) calculateStat(floatDateTestStart, "perc_60");
    assertEqualsList(getRawResponse(), floatDate, floatDateTest);
    // Float String
    ArrayList<Float> floatString =
        getFloatList("percentile_60n", "fieldFacets", "string_sd", "float", "float");
    ArrayList<Float> floatStringTest =
        (ArrayList<Float>) calculateStat(floatStringTestStart, "perc_60");
    assertEqualsList(getRawResponse(), floatString, floatStringTest);

    // Double Date
    ArrayList<Double> doubleDate =
        getDoubleList("percentile_60n", "fieldFacets", "date_dtd", "double", "double");
    ArrayList<Double> doubleDateTest =
        (ArrayList<Double>) calculateStat(doubleDateTestStart, "perc_60");
    assertEqualsList(getRawResponse(), doubleDate, doubleDateTest);
    // Double String
    ArrayList<Double> doubleString =
        getDoubleList("percentile_60n", "fieldFacets", "string_sd", "double", "double");
    ArrayList<Double> doubleStringTest =
        (ArrayList<Double>) calculateStat(doubleStringTestStart, "perc_60");
    assertEqualsList(getRawResponse(), doubleString, doubleStringTest);

    // Date Int
    ArrayList<String> dateInt =
        getStringList("percentile_60", "fieldFacets", "int_id", "date", "date");
    ArrayList<String> dateIntTest = (ArrayList<String>) calculateStat(dateIntTestStart, "perc_60");
    assertEqualsList(getRawResponse(), dateInt, dateIntTest);
    // Date Long
    ArrayList<String> dateString =
        getStringList("percentile_60", "fieldFacets", "long_ld", "date", "date");
    ArrayList<String> dateLongTest =
        (ArrayList<String>) calculateStat(dateLongTestStart, "perc_60");
    assertEqualsList(getRawResponse(), dateString, dateLongTest);

    // String Int
    ArrayList<String> stringInt =
        getStringList("percentile_60", "fieldFacets", "int_id", "str", "str");
    ArrayList<String> stringIntTest =
        (ArrayList<String>) calculateStat(stringIntTestStart, "perc_60");
    assertEqualsList(getRawResponse(), stringInt, stringIntTest);
    // String Long
    ArrayList<String> stringLong =
        getStringList("percentile_60", "fieldFacets", "long_ld", "str", "str");
    ArrayList<String> stringLongTest =
        (ArrayList<String>) calculateStat(stringLongTestStart, "perc_60");
    assertEqualsList(getRawResponse(), stringLong, stringLongTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void minTest() throws Exception {
    // Int Date
    ArrayList<Integer> intDate = getIntegerList("minn", "fieldFacets", "date_dtd", "int", "int");
    ArrayList<Integer> intDateTest = (ArrayList<Integer>) calculateStat(intDateTestStart, "min");
    assertEqualsList(getRawResponse(), intDate, intDateTest);
    // Int String
    ArrayList<Integer> intString = getIntegerList("minn", "fieldFacets", "string_sd", "int", "int");
    ArrayList<Integer> intStringTest =
        (ArrayList<Integer>) calculateStat(intStringTestStart, "min");
    assertEqualsList(getRawResponse(), intString, intStringTest);

    // Long Date
    ArrayList<Long> longDate = getLongList("minn", "fieldFacets", "date_dtd", "long", "long");
    ArrayList<Long> longDateTest = (ArrayList<Long>) calculateStat(longDateTestStart, "min");
    assertEqualsList(getRawResponse(), longDate, longDateTest);
    // Long String
    ArrayList<Long> longString = getLongList("minn", "fieldFacets", "string_sd", "long", "long");
    ArrayList<Long> longStringTest = (ArrayList<Long>) calculateStat(longStringTestStart, "min");
    assertEqualsList(getRawResponse(), longString, longStringTest);

    // Float Date
    ArrayList<Float> floatDate = getFloatList("minn", "fieldFacets", "date_dtd", "float", "float");
    ArrayList<Float> floatDateTest = (ArrayList<Float>) calculateStat(floatDateTestStart, "min");
    assertEqualsList(getRawResponse(), floatDate, floatDateTest);
    // Float String
    ArrayList<Float> floatString =
        getFloatList("minn", "fieldFacets", "string_sd", "float", "float");
    ArrayList<Float> floatStringTest =
        (ArrayList<Float>) calculateStat(floatStringTestStart, "min");
    assertEqualsList(getRawResponse(), floatString, floatStringTest);

    // Double Date
    ArrayList<Double> doubleDate =
        getDoubleList("minn", "fieldFacets", "date_dtd", "double", "double");
    ArrayList<Double> doubleDateTest =
        (ArrayList<Double>) calculateStat(doubleDateTestStart, "min");
    assertEqualsList(getRawResponse(), doubleDate, doubleDateTest);
    // Double String
    ArrayList<Double> doubleString =
        getDoubleList("minn", "fieldFacets", "string_sd", "double", "double");
    ArrayList<Double> doubleStringTest =
        (ArrayList<Double>) calculateStat(doubleStringTestStart, "min");
    assertEqualsList(getRawResponse(), doubleString, doubleStringTest);

    // Date Int
    ArrayList<String> dateInt = getStringList("min", "fieldFacets", "int_id", "date", "date");
    ArrayList<String> dateIntTest = (ArrayList<String>) calculateStat(dateIntTestStart, "min");
    assertEqualsList(getRawResponse(), dateInt, dateIntTest);
    // Date Long
    ArrayList<String> dateString = getStringList("min", "fieldFacets", "long_ld", "date", "date");
    ArrayList<String> dateLongTest = (ArrayList<String>) calculateStat(dateLongTestStart, "min");
    assertEqualsList(getRawResponse(), dateString, dateLongTest);

    // String Int
    ArrayList<String> stringInt = getStringList("min", "fieldFacets", "int_id", "str", "str");
    ArrayList<String> stringIntTest = (ArrayList<String>) calculateStat(stringIntTestStart, "min");
    assertEqualsList(getRawResponse(), stringInt, stringIntTest);
    // String Long
    ArrayList<String> stringLong = getStringList("min", "fieldFacets", "long_ld", "str", "str");
    ArrayList<String> stringLongTest =
        (ArrayList<String>) calculateStat(stringLongTestStart, "min");
    assertEqualsList(getRawResponse(), stringLong, stringLongTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void maxTest() throws Exception {
    // Int Date
    ArrayList<Integer> intDate = getIntegerList("maxn", "fieldFacets", "date_dtd", "int", "int");
    ArrayList<Integer> intDateTest = (ArrayList<Integer>) calculateStat(intDateTestStart, "max");
    assertEqualsList(getRawResponse(), intDate, intDateTest);

    // Int String
    ArrayList<Integer> intString = getIntegerList("maxn", "fieldFacets", "string_sd", "int", "int");
    ArrayList<Integer> intStringTest =
        (ArrayList<Integer>) calculateStat(intStringTestStart, "max");
    assertEqualsList(getRawResponse(), intString, intStringTest);

    // Long Date
    ArrayList<Long> longDate = getLongList("maxn", "fieldFacets", "date_dtd", "long", "long");
    ArrayList<Long> longDateTest = (ArrayList<Long>) calculateStat(longDateTestStart, "max");
    assertEqualsList(getRawResponse(), longDate, longDateTest);

    // Long String
    ArrayList<Long> longString = getLongList("maxn", "fieldFacets", "string_sd", "long", "long");
    ArrayList<Long> longStringTest = (ArrayList<Long>) calculateStat(longStringTestStart, "max");
    assertEqualsList(getRawResponse(), longString, longStringTest);

    // Float Date
    ArrayList<Float> floatDate = getFloatList("maxn", "fieldFacets", "date_dtd", "float", "float");
    ArrayList<Float> floatDateTest = (ArrayList<Float>) calculateStat(floatDateTestStart, "max");
    assertEqualsList(getRawResponse(), floatDate, floatDateTest);

    // Float String
    ArrayList<Float> floatString =
        getFloatList("maxn", "fieldFacets", "string_sd", "float", "float");
    ArrayList<Float> floatStringTest =
        (ArrayList<Float>) calculateStat(floatStringTestStart, "max");
    assertEqualsList(getRawResponse(), floatString, floatStringTest);

    // Double Date
    ArrayList<Double> doubleDate =
        getDoubleList("maxn", "fieldFacets", "date_dtd", "double", "double");
    ArrayList<Double> doubleDateTest =
        (ArrayList<Double>) calculateStat(doubleDateTestStart, "max");
    assertEqualsList(getRawResponse(), doubleDate, doubleDateTest);

    // Double String
    ArrayList<Double> doubleString =
        getDoubleList("maxn", "fieldFacets", "string_sd", "double", "double");
    ArrayList<Double> doubleStringTest =
        (ArrayList<Double>) calculateStat(doubleStringTestStart, "max");
    assertEqualsList(getRawResponse(), doubleString, doubleStringTest);

    // String Int
    ArrayList<String> stringInt = getStringList("max", "fieldFacets", "int_id", "str", "str");
    ArrayList<String> stringIntTest = (ArrayList<String>) calculateStat(stringIntTestStart, "max");
    assertEqualsList(getRawResponse(), stringInt, stringIntTest);

    // String Long
    ArrayList<String> stringLong = getStringList("max", "fieldFacets", "long_ld", "str", "str");
    ArrayList<String> stringLongTest =
        (ArrayList<String>) calculateStat(stringLongTestStart, "max");
    assertEqualsList(getRawResponse(), stringLong, stringLongTest);

    // Date Int
    ArrayList<String> dateInt = getStringList("max", "fieldFacets", "int_id", "date", "date");
    ArrayList<String> dateIntTest = (ArrayList<String>) calculateStat(dateIntTestStart, "max");
    assertEqualsList(getRawResponse(), dateInt, dateIntTest);

    // Date Long
    ArrayList<String> dateString = getStringList("max", "fieldFacets", "long_ld", "date", "date");
    ArrayList<String> dateLongTest = (ArrayList<String>) calculateStat(dateLongTestStart, "max");
    assertEqualsList(getRawResponse(), dateString, dateLongTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void uniqueTest() throws Exception {
    // Int Date
    ArrayList<Long> intDate = getLongList("uniquen", "fieldFacets", "date_dtd", "long", "int");
    ArrayList<Long> intDateTest = (ArrayList<Long>) calculateStat(intDateTestStart, "unique");
    assertEqualsList(getRawResponse(), intDate, intDateTest);
    // Int String
    ArrayList<Long> intString = getLongList("uniquen", "fieldFacets", "string_sd", "long", "int");
    ArrayList<Long> intStringTest = (ArrayList<Long>) calculateStat(intStringTestStart, "unique");
    assertEqualsList(getRawResponse(), intString, intStringTest);

    // Long Date
    ArrayList<Long> longDate = getLongList("uniquen", "fieldFacets", "date_dtd", "long", "long");
    ArrayList<Long> longDateTest = (ArrayList<Long>) calculateStat(longDateTestStart, "unique");
    assertEqualsList(getRawResponse(), longDate, longDateTest);
    // Long String
    ArrayList<Long> longString = getLongList("uniquen", "fieldFacets", "string_sd", "long", "long");
    ArrayList<Long> longStringTest = (ArrayList<Long>) calculateStat(longStringTestStart, "unique");
    assertEqualsList(getRawResponse(), longString, longStringTest);

    // Float Date
    ArrayList<Long> floatDate = getLongList("uniquen", "fieldFacets", "date_dtd", "long", "float");
    ArrayList<Long> floatDateTest = (ArrayList<Long>) calculateStat(floatDateTestStart, "unique");
    assertEqualsList(getRawResponse(), floatDate, floatDateTest);
    // Float String
    ArrayList<Long> floatString =
        getLongList("uniquen", "fieldFacets", "string_sd", "long", "float");
    ArrayList<Long> floatStringTest =
        (ArrayList<Long>) calculateStat(floatStringTestStart, "unique");
    assertEqualsList(getRawResponse(), floatString, floatStringTest);

    // Double Date
    ArrayList<Long> doubleDate =
        getLongList("uniquen", "fieldFacets", "date_dtd", "long", "double");
    ArrayList<Long> doubleDateTest = (ArrayList<Long>) calculateStat(doubleDateTestStart, "unique");
    assertEqualsList(getRawResponse(), doubleDate, doubleDateTest);
    // Double String
    ArrayList<Long> doubleString =
        getLongList("uniquen", "fieldFacets", "string_sd", "long", "double");
    ArrayList<Long> doubleStringTest =
        (ArrayList<Long>) calculateStat(doubleStringTestStart, "unique");
    assertEqualsList(getRawResponse(), doubleString, doubleStringTest);

    // Date Int
    ArrayList<Long> dateInt = getLongList("unique", "fieldFacets", "int_id", "long", "date");
    ArrayList<Long> dateIntTest = (ArrayList<Long>) calculateStat(dateIntTestStart, "unique");
    assertEqualsList(getRawResponse(), dateInt, dateIntTest);
    // Date Long
    ArrayList<Long> dateString = getLongList("unique", "fieldFacets", "long_ld", "long", "date");
    ArrayList<Long> dateLongTest = (ArrayList<Long>) calculateStat(dateLongTestStart, "unique");
    assertEqualsList(getRawResponse(), dateString, dateLongTest);

    // String Int
    ArrayList<Long> stringInt = getLongList("unique", "fieldFacets", "int_id", "long", "str");
    ArrayList<Long> stringIntTest = (ArrayList<Long>) calculateStat(stringIntTestStart, "unique");
    assertEqualsList(getRawResponse(), stringInt, stringIntTest);
    // String Long
    ArrayList<Long> stringLong = getLongList("unique", "fieldFacets", "long_ld", "long", "str");
    ArrayList<Long> stringLongTest = (ArrayList<Long>) calculateStat(stringLongTestStart, "unique");
    assertEqualsList(getRawResponse(), stringLong, stringLongTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void countTest() throws Exception {
    // Int Date
    ArrayList<Long> intDate = getLongList("countn", "fieldFacets", "date_dtd", "long", "int");
    ArrayList<Long> intDateTest = (ArrayList<Long>) calculateStat(intDateTestStart, "count");
    assertEqualsList(getRawResponse(), intDate, intDateTest);

    // Int String
    ArrayList<Long> intString = getLongList("countn", "fieldFacets", "string_sd", "long", "int");
    ArrayList<Long> intStringTest = (ArrayList<Long>) calculateStat(intStringTestStart, "count");
    assertEqualsList(getRawResponse(), intString, intStringTest);

    // Long Date
    ArrayList<Long> longDate = getLongList("countn", "fieldFacets", "date_dtd", "long", "long");
    ArrayList<Long> longDateTest = (ArrayList<Long>) calculateStat(longDateTestStart, "count");
    assertEqualsList(getRawResponse(), longDate, longDateTest);

    // Long String
    ArrayList<Long> longString = getLongList("countn", "fieldFacets", "string_sd", "long", "long");
    ArrayList<Long> longStringTest = (ArrayList<Long>) calculateStat(longStringTestStart, "count");
    assertEqualsList(getRawResponse(), longString, longStringTest);

    // Float Date
    ArrayList<Long> floatDate = getLongList("countn", "fieldFacets", "date_dtd", "long", "float");
    ArrayList<Long> floatDateTest = (ArrayList<Long>) calculateStat(floatDateTestStart, "count");
    assertEqualsList(getRawResponse(), floatDate, floatDateTest);

    // Float String
    ArrayList<Long> floatString =
        getLongList("countn", "fieldFacets", "string_sd", "long", "float");
    ArrayList<Long> floatStringTest =
        (ArrayList<Long>) calculateStat(floatStringTestStart, "count");
    assertEqualsList(getRawResponse(), floatString, floatStringTest);

    // Double Date
    ArrayList<Long> doubleDate = getLongList("countn", "fieldFacets", "date_dtd", "long", "double");
    ArrayList<Long> doubleDateTest = (ArrayList<Long>) calculateStat(doubleDateTestStart, "count");
    assertEqualsList(getRawResponse(), doubleDate, doubleDateTest);

    // Double String
    ArrayList<Long> doubleString =
        getLongList("countn", "fieldFacets", "string_sd", "long", "double");
    ArrayList<Long> doubleStringTest =
        (ArrayList<Long>) calculateStat(doubleStringTestStart, "count");
    assertEqualsList(getRawResponse(), doubleString, doubleStringTest);

    // Date Int
    ArrayList<Long> dateInt = getLongList("count", "fieldFacets", "int_id", "long", "date");
    ArrayList<Long> dateIntTest = (ArrayList<Long>) calculateStat(dateIntTestStart, "count");
    assertEqualsList(getRawResponse(), dateIntTest, dateInt);

    // Date Long
    ArrayList<Long> dateLong = getLongList("count", "fieldFacets", "long_ld", "long", "date");
    ArrayList<Long> dateLongTest = (ArrayList<Long>) calculateStat(dateLongTestStart, "count");
    assertEqualsList(getRawResponse(), dateLong, dateLongTest);

    // String Int
    ArrayList<Long> stringInt = getLongList("count", "fieldFacets", "int_id", "long", "str");
    ArrayList<Long> stringIntTest = (ArrayList<Long>) calculateStat(stringIntTestStart, "count");
    assertEqualsList(getRawResponse(), stringInt, stringIntTest);

    // String Long
    ArrayList<Long> stringLong = getLongList("count", "fieldFacets", "long_ld", "long", "str");
    ArrayList<Long> stringLongTest = (ArrayList<Long>) calculateStat(stringLongTestStart, "count");
    assertEqualsList(getRawResponse(), stringLong, stringLongTest);
  }

  @Test
  public void missingTest() throws Exception {
    // Int Date
    ArrayList<Long> intDate = getLongList("missingn", "fieldFacets", "date_dtd", "long", "int");
    setLatestType("int");
    assertEqualsList(getRawResponse(), intDateTestMissing, intDate);

    // Int String
    ArrayList<Long> intString = getLongList("missingn", "fieldFacets", "string_sd", "long", "int");
    assertEqualsList(getRawResponse(), intStringTestMissing, intString);

    // Long Date
    ArrayList<Long> longDate = getLongList("missingn", "fieldFacets", "date_dtd", "long", "long");
    setLatestType("long");
    assertEqualsList(getRawResponse(), longDateTestMissing, longDate);

    // Long String
    ArrayList<Long> longString =
        getLongList("missingn", "fieldFacets", "string_sd", "long", "long");
    assertEqualsList(getRawResponse(), longStringTestMissing, longString);

    // Float Date
    ArrayList<Long> floatDate = getLongList("missingn", "fieldFacets", "date_dtd", "long", "float");
    setLatestType("float");
    assertEqualsList(getRawResponse(), floatDateTestMissing, floatDate);

    // Float String
    ArrayList<Long> floatString =
        getLongList("missingn", "fieldFacets", "string_sd", "long", "float");
    assertEqualsList(getRawResponse(), floatStringTestMissing, floatString);

    // Double Date
    ArrayList<Long> doubleDate =
        getLongList("missingn", "fieldFacets", "date_dtd", "long", "double");
    setLatestType("double");
    assertEqualsList(getRawResponse(), doubleDateTestMissing, doubleDate);

    // Double String
    ArrayList<Long> doubleString =
        getLongList("missingn", "fieldFacets", "string_sd", "long", "double");
    assertEqualsList(getRawResponse(), doubleStringTestMissing, doubleString);

    // Date Int
    ArrayList<Long> dateInt = getLongList("missing", "fieldFacets", "int_id", "long", "date");
    setLatestType("date");
    assertEqualsList(getRawResponse(), dateIntTestMissing, dateInt);

    // Date Long
    ArrayList<Long> dateLong = getLongList("missing", "fieldFacets", "long_ld", "long", "date");
    assertEqualsList(getRawResponse(), dateLongTestMissing, dateLong);

    // String Int
    ArrayList<Long> stringInt = getLongList("missing", "fieldFacets", "int_id", "long", "str");
    setLatestType("string");
    assertEqualsList(getRawResponse(), stringIntTestMissing, stringInt);

    // String Long
    ArrayList<Long> stringLong = getLongList("missing", "fieldFacets", "long_ld", "long", "str");
    assertEqualsList(getRawResponse(), stringLongTestMissing, stringLong);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void multiValueTest() throws Exception {
    // Long
    ArrayList<Double> lon =
        getDoubleList("multivalued", "fieldFacets", "long_ldm", "double", "mean");
    ArrayList<Double> longTest = calculateNumberStat(multiLongTestStart, "mean");
    assertEqualsList(getRawResponse(), lon, longTest);
    // Date
    ArrayList<Double> date =
        getDoubleList("multivalued", "fieldFacets", "date_dtdm", "double", "mean");
    ArrayList<Double> dateTest = calculateNumberStat(multiDateTestStart, "mean");
    assertEqualsList(getRawResponse(), date, dateTest);
    // String
    ArrayList<Double> string =
        getDoubleList("multivalued", "fieldFacets", "string_sdm", "double", "mean");
    ArrayList<Double> stringTest = calculateNumberStat(multiStringTestStart, "mean");
    assertEqualsList(getRawResponse(), string, stringTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void missingFacetTest() throws Exception {
    // int MultiDate
    String xPath =
        "/response/lst[@name='stats']/lst[@name='missingf']/lst[@name='fieldFacets']/lst[@name='date_dtdm']/lst[@name='(MISSING)']";
    Node missingNodeXPath = getNode(xPath);
    assertNotNull(getRawResponse(), missingNodeXPath);

    ArrayList<Double> string =
        getDoubleList("missingf", "fieldFacets", "date_dtdm", "double", "mean");
    // super.removeNodes(xPath, string);
    ArrayList<Double> stringTest = calculateNumberStat(multiDateTestStart, "mean");
    assertEqualsList(getRawResponse(), string, stringTest);

    // Int String
    xPath =
        "/response/lst[@name='stats']/lst[@name='missingf']/lst[@name='fieldFacets']/lst[@name='string_sd']/lst[@name='(MISSING)']";
    missingNodeXPath = getNode(xPath);
    String missingNodeXPathStr = xPath;
    assertNotNull(getRawResponse(), missingNodeXPath);

    xPath =
        "/response/lst[@name='stats']/lst[@name='missingf']/lst[@name='fieldFacets']/lst[@name='string_sd']/lst[@name='str0']";
    assertNull(getRawResponse(), getNode(xPath));

    ArrayList<Double> intString =
        getDoubleList("missingf", "fieldFacets", "string_sd", "double", "mean");
    // removeNodes(missingNodeXPathStr, intString);
    ArrayList<Double> intStringTest = calculateNumberStat(intStringTestStart, "mean");
    assertEqualsList(getRawResponse(), intString, intStringTest);

    // Int Date
    ArrayList<Double> intDate =
        getDoubleList("missingf", "fieldFacets", "date_dtd", "double", "mean");
    ArrayList<ArrayList<Double>> intDateMissingTestStart =
        (ArrayList<ArrayList<Double>>) intDateTestStart.clone();
    ArrayList<Double> intDateTest = calculateNumberStat(intDateMissingTestStart, "mean");
    assertEqualsList(getRawResponse(), intDate, intDateTest);
  }

  private void checkStddevs(ArrayList<Double> list1, ArrayList<Double> list2) {
    Collections.sort(list1);
    Collections.sort(list2);
    for (int i = 0; i < list1.size(); i++) {
      if ((Math.abs(list1.get(i) - list2.get(i)) < .00000000001) == false) {
        assertEquals(getRawResponse(), list1.get(i), list2.get(i), 0.00000000001);
      }
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static void assertEqualsList(
      String mes,
      ArrayList<? extends Comparable> actual,
      ArrayList<? extends Comparable> expected) {
    Collections.sort(actual);
    Collections.sort(expected);
    assertEquals(mes, actual, expected);
  }
}
