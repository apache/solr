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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.Test;

@ThreadLeakLingering(linger = 0)
public class LegacyFieldFacetCloudTest extends LegacyAbstractAnalyticsFacetCloudTest {
  public static final int INT = 71;
  public static final int LONG = 36;
  public static final int LONGM = 50;
  public static final int FLOAT = 73;
  public static final int FLOATM = 84;
  public static final int DOUBLE = 49;
  public static final int DATE = 12;
  public static final int DATEM = 30;
  public static final int STRING = 29;
  public static final int STRINGM = 41;
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
  public static void beforeTest() throws Exception {

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

    UpdateRequest req = new UpdateRequest();
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
      req.add(fields.toArray(new String[0]));

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
    }

    req.commit(cluster.getSolrClient(), COLLECTIONORALIAS);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void sumTest() throws Exception {
    String[] params =
        new String[] {
          "o.sum.s.int", "sum(int_id)",
          "o.sum.s.long", "sum(long_ld)",
          "o.sum.s.float", "sum(float_fd)",
          "o.sum.s.double", "sum(double_dd)",
          "o.sum.ff", "string_sd",
          "o.sum.ff", "date_dtd"
        };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    // Int Date
    ArrayList<Double> intDate =
        getValueList(response, "sum", "fieldFacets", "date_dtd", "int", false);
    ArrayList<Double> intDateTest = calculateFacetedNumberStat(intDateTestStart, "sum");
    assertEqualsList(responseStr, intDate, intDateTest);
    // Int String
    ArrayList<Double> intString =
        getValueList(response, "sum", "fieldFacets", "string_sd", "int", false);
    ArrayList<Double> intStringTest = calculateFacetedNumberStat(intStringTestStart, "sum");
    assertEqualsList(responseStr, intString, intStringTest);

    // Long Date
    ArrayList<Double> longDate =
        getValueList(response, "sum", "fieldFacets", "date_dtd", "long", false);
    ArrayList<Double> longDateTest = calculateFacetedNumberStat(longDateTestStart, "sum");
    assertEqualsList(responseStr, longDate, longDateTest);
    // Long String
    ArrayList<Double> longString =
        getValueList(response, "sum", "fieldFacets", "string_sd", "long", false);
    ArrayList<Double> longStringTest = calculateFacetedNumberStat(longStringTestStart, "sum");
    assertEqualsList(responseStr, longString, longStringTest);

    // Float Date
    ArrayList<Double> floatDate =
        getValueList(response, "sum", "fieldFacets", "date_dtd", "float", false);
    ArrayList<Double> floatDateTest = calculateFacetedNumberStat(floatDateTestStart, "sum");
    assertEqualsList(responseStr, floatDate, floatDateTest);
    // Float String
    ArrayList<Double> floatString =
        getValueList(response, "sum", "fieldFacets", "string_sd", "float", false);
    ArrayList<Double> floatStringTest = calculateFacetedNumberStat(floatStringTestStart, "sum");
    assertEqualsList(responseStr, floatString, floatStringTest);

    // Double Date
    ArrayList<Double> doubleDate =
        getValueList(response, "sum", "fieldFacets", "date_dtd", "double", false);
    ArrayList<Double> doubleDateTest = calculateFacetedNumberStat(doubleDateTestStart, "sum");
    assertEqualsList(responseStr, doubleDate, doubleDateTest);
    // Double String
    ArrayList<Double> doubleString =
        getValueList(response, "sum", "fieldFacets", "string_sd", "double", false);
    ArrayList<Double> doubleStringTest = calculateFacetedNumberStat(doubleStringTestStart, "sum");
    assertEqualsList(responseStr, doubleString, doubleStringTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void meanTest() throws Exception {
    String[] params =
        new String[] {
          "o.mean.s.int", "mean(int_id)",
          "o.mean.s.long", "mean(long_ld)",
          "o.mean.s.float", "mean(float_fd)",
          "o.mean.s.double", "mean(double_dd)",
          "o.mean.ff", "string_sd",
          "o.mean.ff", "date_dtd"
        };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    // Int Date
    ArrayList<Double> intDate =
        getValueList(response, "mean", "fieldFacets", "date_dtd", "int", false);
    ArrayList<Double> intDateTest = calculateFacetedNumberStat(intDateTestStart, "mean");
    assertEqualsList(responseStr, intDate, intDateTest);
    // Int String
    ArrayList<Double> intString =
        getValueList(response, "mean", "fieldFacets", "string_sd", "int", false);
    ArrayList<Double> intStringTest = calculateFacetedNumberStat(intStringTestStart, "mean");
    assertEqualsList(responseStr, intString, intStringTest);

    // Long Date
    ArrayList<Double> longDate =
        getValueList(response, "mean", "fieldFacets", "date_dtd", "long", false);
    ArrayList<Double> longDateTest = calculateFacetedNumberStat(longDateTestStart, "mean");
    assertEqualsList(responseStr, longDate, longDateTest);
    // Long String
    ArrayList<Double> longString =
        getValueList(response, "mean", "fieldFacets", "string_sd", "long", false);
    ArrayList<Double> longStringTest = calculateFacetedNumberStat(longStringTestStart, "mean");
    assertEqualsList(responseStr, longString, longStringTest);

    // Float Date
    ArrayList<Double> floatDate =
        getValueList(response, "mean", "fieldFacets", "date_dtd", "float", false);
    ArrayList<Double> floatDateTest = calculateFacetedNumberStat(floatDateTestStart, "mean");
    assertEqualsList(responseStr, floatDate, floatDateTest);
    // Float String
    ArrayList<Double> floatString =
        getValueList(response, "mean", "fieldFacets", "string_sd", "float", false);
    ArrayList<Double> floatStringTest = calculateFacetedNumberStat(floatStringTestStart, "mean");
    assertEqualsList(responseStr, floatString, floatStringTest);

    // Double Date
    ArrayList<Double> doubleDate =
        getValueList(response, "mean", "fieldFacets", "date_dtd", "double", false);
    ArrayList<Double> doubleDateTest = calculateFacetedNumberStat(doubleDateTestStart, "mean");
    assertEqualsList(responseStr, doubleDate, doubleDateTest);
    // Double String
    ArrayList<Double> doubleString =
        getValueList(response, "mean", "fieldFacets", "string_sd", "double", false);
    ArrayList<Double> doubleStringTest = calculateFacetedNumberStat(doubleStringTestStart, "mean");
    assertEqualsList(responseStr, doubleString, doubleStringTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void stddevFacetAscTest() throws Exception {
    String[] params =
        new String[] {
          "o.stddev.s.int", "stddev(int_id)",
          "o.stddev.s.long", "stddev(long_ld)",
          "o.stddev.s.float", "stddev(float_fd)",
          "o.stddev.s.double", "stddev(double_dd)",
          "o.stddev.ff", "string_sd",
          "o.stddev.ff", "date_dtd"
        };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);

    // Int Date
    ArrayList<Double> intDate =
        getValueList(response, "stddev", "fieldFacets", "date_dtd", "int", false);
    ArrayList<Double> intDateTest = calculateFacetedNumberStat(intDateTestStart, "stddev");
    checkStddevs(response, intDate, intDateTest);
    // Int String
    ArrayList<Double> intString =
        getValueList(response, "stddev", "fieldFacets", "string_sd", "int", false);
    ArrayList<Double> intStringTest = calculateFacetedNumberStat(intStringTestStart, "stddev");
    checkStddevs(response, intString, intStringTest);

    // Long Date
    ArrayList<Double> longDate =
        getValueList(response, "stddev", "fieldFacets", "date_dtd", "long", false);
    ArrayList<Double> longDateTest = calculateFacetedNumberStat(longDateTestStart, "stddev");
    checkStddevs(response, longDate, longDateTest);
    // Long String
    ArrayList<Double> longString =
        getValueList(response, "stddev", "fieldFacets", "string_sd", "long", false);
    ArrayList<Double> longStringTest = calculateFacetedNumberStat(longStringTestStart, "stddev");
    checkStddevs(response, longString, longStringTest);

    // Float Date
    ArrayList<Double> floatDate =
        getValueList(response, "stddev", "fieldFacets", "date_dtd", "float", false);
    ArrayList<Double> floatDateTest = calculateFacetedNumberStat(floatDateTestStart, "stddev");
    checkStddevs(response, floatDate, floatDateTest);
    // Float String
    ArrayList<Double> floatString =
        getValueList(response, "stddev", "fieldFacets", "string_sd", "float", false);
    ArrayList<Double> floatStringTest = calculateFacetedNumberStat(floatStringTestStart, "stddev");
    checkStddevs(response, floatString, floatStringTest);

    // Double Date
    ArrayList<Double> doubleDate =
        getValueList(response, "stddev", "fieldFacets", "date_dtd", "double", false);
    ArrayList<Double> doubleDateTest = calculateFacetedNumberStat(doubleDateTestStart, "stddev");
    checkStddevs(response, doubleDate, doubleDateTest);
    // Double String
    ArrayList<Double> doubleString =
        getValueList(response, "stddev", "fieldFacets", "string_sd", "double", false);
    ArrayList<Double> doubleStringTest =
        calculateFacetedNumberStat(doubleStringTestStart, "stddev");
    checkStddevs(response, doubleString, doubleStringTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void medianFacetAscTest() throws Exception {
    String[] params =
        new String[] {
          "o.median.s.int", "median(int_id)",
          "o.median.s.long", "median(long_ld)",
          "o.median.s.float", "median(float_fd)",
          "o.median.s.double", "median(double_dd)",
          "o.median.ff", "string_sd",
          "o.median.ff", "date_dtd"
        };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    // Int Date
    ArrayList<Double> intDate =
        getValueList(response, "median", "fieldFacets", "date_dtd", "int", false);
    ArrayList<Double> intDateTest = calculateFacetedNumberStat(intDateTestStart, "median");
    assertEqualsList(responseStr, intDate, intDateTest);
    // Int String
    ArrayList<Double> intString =
        getValueList(response, "median", "fieldFacets", "string_sd", "int", false);
    ArrayList<Double> intStringTest = calculateFacetedNumberStat(intStringTestStart, "median");
    assertEqualsList(responseStr, intString, intStringTest);

    // Long Date
    ArrayList<Double> longDate =
        getValueList(response, "median", "fieldFacets", "date_dtd", "long", false);
    ArrayList<Double> longDateTest = calculateFacetedNumberStat(longDateTestStart, "median");
    assertEqualsList(responseStr, longDate, longDateTest);
    // Long String
    ArrayList<Double> longString =
        getValueList(response, "median", "fieldFacets", "string_sd", "long", false);
    ArrayList<Double> longStringTest = calculateFacetedNumberStat(longStringTestStart, "median");
    assertEqualsList(responseStr, longString, longStringTest);

    // Float Date
    ArrayList<Double> floatDate =
        getValueList(response, "median", "fieldFacets", "date_dtd", "float", false);
    ArrayList<Double> floatDateTest = calculateFacetedNumberStat(floatDateTestStart, "median");
    assertEqualsList(responseStr, floatDate, floatDateTest);
    // Float String
    ArrayList<Double> floatString =
        getValueList(response, "median", "fieldFacets", "string_sd", "float", false);
    ArrayList<Double> floatStringTest = calculateFacetedNumberStat(floatStringTestStart, "median");
    assertEqualsList(responseStr, floatString, floatStringTest);

    // Double Date
    ArrayList<Double> doubleDate =
        getValueList(response, "median", "fieldFacets", "date_dtd", "double", false);
    ArrayList<Double> doubleDateTest = calculateFacetedNumberStat(doubleDateTestStart, "median");
    assertEqualsList(responseStr, doubleDate, doubleDateTest);
    // Double String
    ArrayList<Double> doubleString =
        getValueList(response, "median", "fieldFacets", "string_sd", "double", false);
    ArrayList<Double> doubleStringTest =
        calculateFacetedNumberStat(doubleStringTestStart, "median");
    assertEqualsList(responseStr, doubleString, doubleStringTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void perc20Test() throws Exception {
    String[] params =
        new String[] {
          "o.percentile_20n.s.int", "percentile(20,int_id)",
          "o.percentile_20n.s.long", "percentile(20,long_ld)",
          "o.percentile_20n.s.float", "percentile(20,float_fd)",
          "o.percentile_20n.s.double", "percentile(20,double_dd)",
          "o.percentile_20n.ff", "string_sd",
          "o.percentile_20n.ff", "date_dtd",
          "o.percentile_20.s.str", "percentile(20,string_sd)",
          "o.percentile_20.s.date", "string(percentile(20,date_dtd))",
          "o.percentile_20.ff", "int_id",
          "o.percentile_20.ff", "long_ld"
        };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    // Int Date
    ArrayList<Integer> intDate =
        getValueList(response, "percentile_20n", "fieldFacets", "date_dtd", "int", false);
    ArrayList<Integer> intDateTest =
        (ArrayList<Integer>) calculateFacetedStat(intDateTestStart, "perc_20");
    assertEqualsList(responseStr, intDate, intDateTest);
    // Int String
    ArrayList<Integer> intString =
        getValueList(response, "percentile_20n", "fieldFacets", "string_sd", "int", false);
    ArrayList<Integer> intStringTest =
        (ArrayList<Integer>) calculateFacetedStat(intStringTestStart, "perc_20");
    assertEqualsList(responseStr, intString, intStringTest);

    // Long Date
    ArrayList<Long> longDate =
        getValueList(response, "percentile_20n", "fieldFacets", "date_dtd", "long", false);
    ArrayList<Long> longDateTest =
        (ArrayList<Long>) calculateFacetedStat(longDateTestStart, "perc_20");
    assertEqualsList(responseStr, longDate, longDateTest);
    // Long String
    ArrayList<Long> longString =
        getValueList(response, "percentile_20n", "fieldFacets", "string_sd", "long", false);
    ArrayList<Long> longStringTest =
        (ArrayList<Long>) calculateFacetedStat(longStringTestStart, "perc_20");
    assertEqualsList(responseStr, longString, longStringTest);

    // Float Date
    ArrayList<Float> floatDate =
        getValueList(response, "percentile_20n", "fieldFacets", "date_dtd", "float", false);
    ArrayList<Float> floatDateTest =
        (ArrayList<Float>) calculateFacetedStat(floatDateTestStart, "perc_20");
    assertEqualsList(responseStr, floatDate, floatDateTest);
    // Float String
    ArrayList<Float> floatString =
        getValueList(response, "percentile_20n", "fieldFacets", "string_sd", "float", false);
    ArrayList<Float> floatStringTest =
        (ArrayList<Float>) calculateFacetedStat(floatStringTestStart, "perc_20");
    assertEqualsList(responseStr, floatString, floatStringTest);

    // Double Date
    ArrayList<Double> doubleDate =
        getValueList(response, "percentile_20n", "fieldFacets", "date_dtd", "double", false);
    ArrayList<Double> doubleDateTest =
        (ArrayList<Double>) calculateFacetedStat(doubleDateTestStart, "perc_20");
    assertEqualsList(responseStr, doubleDate, doubleDateTest);
    // Double String
    ArrayList<Double> doubleString =
        getValueList(response, "percentile_20n", "fieldFacets", "string_sd", "double", false);
    ArrayList<Double> doubleStringTest =
        (ArrayList<Double>) calculateFacetedStat(doubleStringTestStart, "perc_20");
    assertEqualsList(responseStr, doubleString, doubleStringTest);

    // Date Int
    ArrayList<String> dateInt =
        getValueList(response, "percentile_20", "fieldFacets", "int_id", "date", false);
    ArrayList<String> dateIntTest =
        (ArrayList<String>) calculateFacetedStat(dateIntTestStart, "perc_20");
    assertEqualsList(responseStr, dateInt, dateIntTest);
    // Date Long
    ArrayList<String> dateString =
        getValueList(response, "percentile_20", "fieldFacets", "long_ld", "date", false);
    ArrayList<String> dateLongTest =
        (ArrayList<String>) calculateFacetedStat(dateLongTestStart, "perc_20");
    assertEqualsList(responseStr, dateString, dateLongTest);

    // String Int
    ArrayList<String> stringInt =
        getValueList(response, "percentile_20", "fieldFacets", "int_id", "str", false);
    ArrayList<String> stringIntTest =
        (ArrayList<String>) calculateFacetedStat(stringIntTestStart, "perc_20");
    assertEqualsList(responseStr, stringInt, stringIntTest);
    // String Long
    ArrayList<String> stringLong =
        getValueList(response, "percentile_20", "fieldFacets", "long_ld", "str", false);
    ArrayList<String> stringLongTest =
        (ArrayList<String>) calculateFacetedStat(stringLongTestStart, "perc_20");
    assertEqualsList(responseStr, stringLong, stringLongTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void perc60Test() throws Exception {
    String[] params =
        new String[] {
          "o.percentile_60n.s.int", "percentile(60,int_id)",
          "o.percentile_60n.s.long", "percentile(60,long_ld)",
          "o.percentile_60n.s.float", "percentile(60,float_fd)",
          "o.percentile_60n.s.double", "percentile(60,double_dd)",
          "o.percentile_60n.ff", "string_sd",
          "o.percentile_60n.ff", "date_dtd",
          "o.percentile_60.s.str", "percentile(60,string_sd)",
          "o.percentile_60.s.date", "string(percentile(60,date_dtd))",
          "o.percentile_60.ff", "int_id",
          "o.percentile_60.ff", "long_ld"
        };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    // Int Date
    ArrayList<Integer> intDate =
        getValueList(response, "percentile_60n", "fieldFacets", "date_dtd", "int", false);
    ArrayList<Integer> intDateTest =
        (ArrayList<Integer>) calculateFacetedStat(intDateTestStart, "perc_60");
    assertEqualsList(responseStr, intDate, intDateTest);
    // Int String
    ArrayList<Integer> intString =
        getValueList(response, "percentile_60n", "fieldFacets", "string_sd", "int", false);
    ArrayList<Integer> intStringTest =
        (ArrayList<Integer>) calculateFacetedStat(intStringTestStart, "perc_60");
    assertEqualsList(responseStr, intString, intStringTest);

    // Long Date
    ArrayList<Long> longDate =
        getValueList(response, "percentile_60n", "fieldFacets", "date_dtd", "long", false);
    ArrayList<Long> longDateTest =
        (ArrayList<Long>) calculateFacetedStat(longDateTestStart, "perc_60");
    assertEqualsList(responseStr, longDate, longDateTest);
    // Long String
    ArrayList<Long> longString =
        getValueList(response, "percentile_60n", "fieldFacets", "string_sd", "long", false);
    ArrayList<Long> longStringTest =
        (ArrayList<Long>) calculateFacetedStat(longStringTestStart, "perc_60");
    assertEqualsList(responseStr, longString, longStringTest);

    // Float Date
    ArrayList<Float> floatDate =
        getValueList(response, "percentile_60n", "fieldFacets", "date_dtd", "float", false);
    ArrayList<Float> floatDateTest =
        (ArrayList<Float>) calculateFacetedStat(floatDateTestStart, "perc_60");
    assertEqualsList(responseStr, floatDate, floatDateTest);
    // Float String
    ArrayList<Float> floatString =
        getValueList(response, "percentile_60n", "fieldFacets", "string_sd", "float", false);
    ArrayList<Float> floatStringTest =
        (ArrayList<Float>) calculateFacetedStat(floatStringTestStart, "perc_60");
    assertEqualsList(responseStr, floatString, floatStringTest);

    // Double Date
    ArrayList<Double> doubleDate =
        getValueList(response, "percentile_60n", "fieldFacets", "date_dtd", "double", false);
    ArrayList<Double> doubleDateTest =
        (ArrayList<Double>) calculateFacetedStat(doubleDateTestStart, "perc_60");
    assertEqualsList(responseStr, doubleDate, doubleDateTest);
    // Double String
    ArrayList<Double> doubleString =
        getValueList(response, "percentile_60n", "fieldFacets", "string_sd", "double", false);
    ArrayList<Double> doubleStringTest =
        (ArrayList<Double>) calculateFacetedStat(doubleStringTestStart, "perc_60");
    assertEqualsList(responseStr, doubleString, doubleStringTest);

    // Date Int
    ArrayList<String> dateInt =
        getValueList(response, "percentile_60", "fieldFacets", "int_id", "date", false);
    ArrayList<String> dateIntTest =
        (ArrayList<String>) calculateFacetedStat(dateIntTestStart, "perc_60");
    assertEqualsList(responseStr, dateInt, dateIntTest);
    // Date Long
    ArrayList<String> dateString =
        getValueList(response, "percentile_60", "fieldFacets", "long_ld", "date", false);
    ArrayList<String> dateLongTest =
        (ArrayList<String>) calculateFacetedStat(dateLongTestStart, "perc_60");
    assertEqualsList(responseStr, dateString, dateLongTest);

    // String Int
    ArrayList<String> stringInt =
        getValueList(response, "percentile_60", "fieldFacets", "int_id", "str", false);
    ArrayList<String> stringIntTest =
        (ArrayList<String>) calculateFacetedStat(stringIntTestStart, "perc_60");
    assertEqualsList(responseStr, stringInt, stringIntTest);
    // String Long
    ArrayList<String> stringLong =
        getValueList(response, "percentile_60", "fieldFacets", "long_ld", "str", false);
    ArrayList<String> stringLongTest =
        (ArrayList<String>) calculateFacetedStat(stringLongTestStart, "perc_60");
    assertEqualsList(responseStr, stringLong, stringLongTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void minTest() throws Exception {
    String[] params =
        new String[] {
          "o.minn.s.int", "min(int_id)",
          "o.minn.s.long", "min(long_ld)",
          "o.minn.s.float", "min(float_fd)",
          "o.minn.s.double", "min(double_dd)",
          "o.minn.ff", "string_sd",
          "o.minn.ff", "date_dtd",
          "o.min.s.str", "min(string_sd)",
          "o.min.s.date", "string(min(date_dtd))",
          "o.min.ff", "int_id",
          "o.min.ff", "long_ld"
        };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    // Int Date
    ArrayList<Integer> intDate =
        getValueList(response, "minn", "fieldFacets", "date_dtd", "int", false);
    ArrayList<Integer> intDateTest =
        (ArrayList<Integer>) calculateFacetedStat(intDateTestStart, "min");
    assertEqualsList(responseStr, intDate, intDateTest);
    // Int String
    ArrayList<Integer> intString =
        getValueList(response, "minn", "fieldFacets", "string_sd", "int", false);
    ArrayList<Integer> intStringTest =
        (ArrayList<Integer>) calculateFacetedStat(intStringTestStart, "min");
    assertEqualsList(responseStr, intString, intStringTest);

    // Long Date
    ArrayList<Long> longDate =
        getValueList(response, "minn", "fieldFacets", "date_dtd", "long", false);
    ArrayList<Long> longDateTest = (ArrayList<Long>) calculateFacetedStat(longDateTestStart, "min");
    assertEqualsList(responseStr, longDate, longDateTest);
    // Long String
    ArrayList<Long> longString =
        getValueList(response, "minn", "fieldFacets", "string_sd", "long", false);
    ArrayList<Long> longStringTest =
        (ArrayList<Long>) calculateFacetedStat(longStringTestStart, "min");
    assertEqualsList(responseStr, longString, longStringTest);

    // Float Date
    ArrayList<Float> floatDate =
        getValueList(response, "minn", "fieldFacets", "date_dtd", "float", false);
    ArrayList<Float> floatDateTest =
        (ArrayList<Float>) calculateFacetedStat(floatDateTestStart, "min");
    assertEqualsList(responseStr, floatDate, floatDateTest);
    // Float String
    ArrayList<Float> floatString =
        getValueList(response, "minn", "fieldFacets", "string_sd", "float", false);
    ArrayList<Float> floatStringTest =
        (ArrayList<Float>) calculateFacetedStat(floatStringTestStart, "min");
    assertEqualsList(responseStr, floatString, floatStringTest);

    // Double Date
    ArrayList<Double> doubleDate =
        getValueList(response, "minn", "fieldFacets", "date_dtd", "double", false);
    ArrayList<Double> doubleDateTest =
        (ArrayList<Double>) calculateFacetedStat(doubleDateTestStart, "min");
    assertEqualsList(responseStr, doubleDate, doubleDateTest);
    // Double String
    ArrayList<Double> doubleString =
        getValueList(response, "minn", "fieldFacets", "string_sd", "double", false);
    ArrayList<Double> doubleStringTest =
        (ArrayList<Double>) calculateFacetedStat(doubleStringTestStart, "min");
    assertEqualsList(responseStr, doubleString, doubleStringTest);

    // Date Int
    ArrayList<String> dateInt =
        getValueList(response, "min", "fieldFacets", "int_id", "date", false);
    ArrayList<String> dateIntTest =
        (ArrayList<String>) calculateFacetedStat(dateIntTestStart, "min");
    assertEqualsList(responseStr, dateInt, dateIntTest);
    // Date Long
    ArrayList<String> dateString =
        getValueList(response, "min", "fieldFacets", "long_ld", "date", false);
    ArrayList<String> dateLongTest =
        (ArrayList<String>) calculateFacetedStat(dateLongTestStart, "min");
    assertEqualsList(responseStr, dateString, dateLongTest);

    // String Int
    ArrayList<String> stringInt =
        getValueList(response, "min", "fieldFacets", "int_id", "str", false);
    ArrayList<String> stringIntTest =
        (ArrayList<String>) calculateFacetedStat(stringIntTestStart, "min");
    assertEqualsList(responseStr, stringInt, stringIntTest);
    // String Long
    ArrayList<String> stringLong =
        getValueList(response, "min", "fieldFacets", "long_ld", "str", false);
    ArrayList<String> stringLongTest =
        (ArrayList<String>) calculateFacetedStat(stringLongTestStart, "min");
    assertEqualsList(responseStr, stringLong, stringLongTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void maxTest() throws Exception {
    String[] params =
        new String[] {
          "o.maxn.s.int", "max(int_id)",
          "o.maxn.s.long", "max(long_ld)",
          "o.maxn.s.float", "max(float_fd)",
          "o.maxn.s.double", "max(double_dd)",
          "o.maxn.ff", "string_sd",
          "o.maxn.ff", "date_dtd",
          "o.max.s.str", "max(string_sd)",
          "o.max.s.date", "string(max(date_dtd))",
          "o.max.ff", "int_id",
          "o.max.ff", "long_ld"
        };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    // Int Date
    ArrayList<Integer> intDate =
        getValueList(response, "maxn", "fieldFacets", "date_dtd", "int", false);
    ArrayList<Integer> intDateTest =
        (ArrayList<Integer>) calculateFacetedStat(intDateTestStart, "max");
    assertEqualsList(responseStr, intDate, intDateTest);

    // Int String
    ArrayList<Integer> intString =
        getValueList(response, "maxn", "fieldFacets", "string_sd", "int", false);
    ArrayList<Integer> intStringTest =
        (ArrayList<Integer>) calculateFacetedStat(intStringTestStart, "max");
    assertEqualsList(responseStr, intString, intStringTest);

    // Long Date
    ArrayList<Long> longDate =
        getValueList(response, "maxn", "fieldFacets", "date_dtd", "long", false);
    ArrayList<Long> longDateTest = (ArrayList<Long>) calculateFacetedStat(longDateTestStart, "max");
    assertEqualsList(responseStr, longDate, longDateTest);

    // Long String
    ArrayList<Long> longString =
        getValueList(response, "maxn", "fieldFacets", "string_sd", "long", false);
    ArrayList<Long> longStringTest =
        (ArrayList<Long>) calculateFacetedStat(longStringTestStart, "max");
    assertEqualsList(responseStr, longString, longStringTest);

    // Float Date
    ArrayList<Float> floatDate =
        getValueList(response, "maxn", "fieldFacets", "date_dtd", "float", false);
    ArrayList<Float> floatDateTest =
        (ArrayList<Float>) calculateFacetedStat(floatDateTestStart, "max");
    assertEqualsList(responseStr, floatDate, floatDateTest);

    // Float String
    ArrayList<Float> floatString =
        getValueList(response, "maxn", "fieldFacets", "string_sd", "float", false);
    ArrayList<Float> floatStringTest =
        (ArrayList<Float>) calculateFacetedStat(floatStringTestStart, "max");
    assertEqualsList(responseStr, floatString, floatStringTest);

    // Double Date
    ArrayList<Double> doubleDate =
        getValueList(response, "maxn", "fieldFacets", "date_dtd", "double", false);
    ArrayList<Double> doubleDateTest =
        (ArrayList<Double>) calculateFacetedStat(doubleDateTestStart, "max");
    assertEqualsList(responseStr, doubleDate, doubleDateTest);

    // Double String
    ArrayList<Double> doubleString =
        getValueList(response, "maxn", "fieldFacets", "string_sd", "double", false);
    ArrayList<Double> doubleStringTest =
        (ArrayList<Double>) calculateFacetedStat(doubleStringTestStart, "max");
    assertEqualsList(responseStr, doubleString, doubleStringTest);

    // String Int
    ArrayList<String> stringInt =
        getValueList(response, "max", "fieldFacets", "int_id", "str", false);
    ArrayList<String> stringIntTest =
        (ArrayList<String>) calculateFacetedStat(stringIntTestStart, "max");
    assertEqualsList(responseStr, stringInt, stringIntTest);

    // String Long
    ArrayList<String> stringLong =
        getValueList(response, "max", "fieldFacets", "long_ld", "str", false);
    ArrayList<String> stringLongTest =
        (ArrayList<String>) calculateFacetedStat(stringLongTestStart, "max");
    assertEqualsList(responseStr, stringLong, stringLongTest);

    // Date Int
    ArrayList<String> dateInt =
        getValueList(response, "max", "fieldFacets", "int_id", "date", false);
    ArrayList<String> dateIntTest =
        (ArrayList<String>) calculateFacetedStat(dateIntTestStart, "max");
    assertEqualsList(responseStr, dateInt, dateIntTest);

    // Date Long
    ArrayList<String> dateString =
        getValueList(response, "max", "fieldFacets", "long_ld", "date", false);
    ArrayList<String> dateLongTest =
        (ArrayList<String>) calculateFacetedStat(dateLongTestStart, "max");
    assertEqualsList(responseStr, dateString, dateLongTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void uniqueTest() throws Exception {
    String[] params =
        new String[] {
          "o.uniquen.s.int", "unique(int_id)",
          "o.uniquen.s.long", "unique(long_ld)",
          "o.uniquen.s.float", "unique(float_fd)",
          "o.uniquen.s.double", "unique(double_dd)",
          "o.uniquen.ff", "string_sd",
          "o.uniquen.ff", "date_dtd",
          "o.unique.s.str", "unique(string_sd)",
          "o.unique.s.date", "unique(date_dtd)",
          "o.unique.ff", "int_id",
          "o.unique.ff", "long_ld"
        };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    // Int Date
    ArrayList<Long> intDate =
        getValueList(response, "uniquen", "fieldFacets", "date_dtd", "int", false);
    ArrayList<Long> intDateTest =
        (ArrayList<Long>) calculateFacetedStat(intDateTestStart, "unique");
    assertEqualsList(responseStr, intDate, intDateTest);
    // Int String
    ArrayList<Long> intString =
        getValueList(response, "uniquen", "fieldFacets", "string_sd", "int", false);
    ArrayList<Long> intStringTest =
        (ArrayList<Long>) calculateFacetedStat(intStringTestStart, "unique");
    assertEqualsList(responseStr, intString, intStringTest);

    // Long Date
    ArrayList<Long> longDate =
        getValueList(response, "uniquen", "fieldFacets", "date_dtd", "long", false);
    ArrayList<Long> longDateTest =
        (ArrayList<Long>) calculateFacetedStat(longDateTestStart, "unique");
    assertEqualsList(responseStr, longDate, longDateTest);
    // Long String
    ArrayList<Long> longString =
        getValueList(response, "uniquen", "fieldFacets", "string_sd", "long", false);
    ArrayList<Long> longStringTest =
        (ArrayList<Long>) calculateFacetedStat(longStringTestStart, "unique");
    assertEqualsList(responseStr, longString, longStringTest);

    // Float Date
    ArrayList<Long> floatDate =
        getValueList(response, "uniquen", "fieldFacets", "date_dtd", "float", false);
    ArrayList<Long> floatDateTest =
        (ArrayList<Long>) calculateFacetedStat(floatDateTestStart, "unique");
    assertEqualsList(responseStr, floatDate, floatDateTest);
    // Float String
    ArrayList<Long> floatString =
        getValueList(response, "uniquen", "fieldFacets", "string_sd", "float", false);
    ArrayList<Long> floatStringTest =
        (ArrayList<Long>) calculateFacetedStat(floatStringTestStart, "unique");
    assertEqualsList(responseStr, floatString, floatStringTest);

    // Double Date
    ArrayList<Long> doubleDate =
        getValueList(response, "uniquen", "fieldFacets", "date_dtd", "double", false);
    ArrayList<Long> doubleDateTest =
        (ArrayList<Long>) calculateFacetedStat(doubleDateTestStart, "unique");
    assertEqualsList(responseStr, doubleDate, doubleDateTest);
    // Double String
    ArrayList<Long> doubleString =
        getValueList(response, "uniquen", "fieldFacets", "string_sd", "double", false);
    ArrayList<Long> doubleStringTest =
        (ArrayList<Long>) calculateFacetedStat(doubleStringTestStart, "unique");
    assertEqualsList(responseStr, doubleString, doubleStringTest);

    // Date Int
    ArrayList<Long> dateInt =
        getValueList(response, "unique", "fieldFacets", "int_id", "date", false);
    ArrayList<Long> dateIntTest =
        (ArrayList<Long>) calculateFacetedStat(dateIntTestStart, "unique");
    assertEqualsList(responseStr, dateInt, dateIntTest);
    // Date Long
    ArrayList<Long> dateString =
        getValueList(response, "unique", "fieldFacets", "long_ld", "date", false);
    ArrayList<Long> dateLongTest =
        (ArrayList<Long>) calculateFacetedStat(dateLongTestStart, "unique");
    assertEqualsList(responseStr, dateString, dateLongTest);

    // String Int
    ArrayList<Long> stringInt =
        getValueList(response, "unique", "fieldFacets", "int_id", "str", false);
    ArrayList<Long> stringIntTest =
        (ArrayList<Long>) calculateFacetedStat(stringIntTestStart, "unique");
    assertEqualsList(responseStr, stringInt, stringIntTest);
    // String Long
    ArrayList<Long> stringLong =
        getValueList(response, "unique", "fieldFacets", "long_ld", "str", false);
    ArrayList<Long> stringLongTest =
        (ArrayList<Long>) calculateFacetedStat(stringLongTestStart, "unique");
    assertEqualsList(responseStr, stringLong, stringLongTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void countTest() throws Exception {
    String[] params =
        new String[] {
          "o.countn.s.int", "count(int_id)",
          "o.countn.s.long", "count(long_ld)",
          "o.countn.s.float", "count(float_fd)",
          "o.countn.s.double", "count(double_dd)",
          "o.countn.ff", "string_sd",
          "o.countn.ff", "date_dtd",
          "o.count.s.str", "count(string_sd)",
          "o.count.s.date", "count(date_dtd)",
          "o.count.ff", "int_id",
          "o.count.ff", "long_ld"
        };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    // Int Date
    ArrayList<Long> intDate =
        getValueList(response, "countn", "fieldFacets", "date_dtd", "int", false);
    ArrayList<Long> intDateTest = (ArrayList<Long>) calculateFacetedStat(intDateTestStart, "count");
    assertEqualsList(responseStr, intDate, intDateTest);

    // Int String
    ArrayList<Long> intString =
        getValueList(response, "countn", "fieldFacets", "string_sd", "int", false);
    ArrayList<Long> intStringTest =
        (ArrayList<Long>) calculateFacetedStat(intStringTestStart, "count");
    assertEqualsList(responseStr, intString, intStringTest);

    // Long Date
    ArrayList<Long> longDate =
        getValueList(response, "countn", "fieldFacets", "date_dtd", "long", false);
    ArrayList<Long> longDateTest =
        (ArrayList<Long>) calculateFacetedStat(longDateTestStart, "count");
    assertEqualsList(responseStr, longDate, longDateTest);

    // Long String
    ArrayList<Long> longString =
        getValueList(response, "countn", "fieldFacets", "string_sd", "long", false);
    ArrayList<Long> longStringTest =
        (ArrayList<Long>) calculateFacetedStat(longStringTestStart, "count");
    assertEqualsList(responseStr, longString, longStringTest);

    // Float Date
    ArrayList<Long> floatDate =
        getValueList(response, "countn", "fieldFacets", "date_dtd", "float", false);
    ArrayList<Long> floatDateTest =
        (ArrayList<Long>) calculateFacetedStat(floatDateTestStart, "count");
    assertEqualsList(responseStr, floatDate, floatDateTest);

    // Float String
    ArrayList<Long> floatString =
        getValueList(response, "countn", "fieldFacets", "string_sd", "float", false);
    ArrayList<Long> floatStringTest =
        (ArrayList<Long>) calculateFacetedStat(floatStringTestStart, "count");
    assertEqualsList(responseStr, floatString, floatStringTest);

    // Double Date
    ArrayList<Long> doubleDate =
        getValueList(response, "countn", "fieldFacets", "date_dtd", "double", false);
    ArrayList<Long> doubleDateTest =
        (ArrayList<Long>) calculateFacetedStat(doubleDateTestStart, "count");
    assertEqualsList(responseStr, doubleDate, doubleDateTest);

    // Double String
    ArrayList<Long> doubleString =
        getValueList(response, "countn", "fieldFacets", "string_sd", "double", false);
    ArrayList<Long> doubleStringTest =
        (ArrayList<Long>) calculateFacetedStat(doubleStringTestStart, "count");
    assertEqualsList(responseStr, doubleString, doubleStringTest);

    // Date Int
    ArrayList<Long> dateInt =
        getValueList(response, "count", "fieldFacets", "int_id", "date", false);
    ArrayList<Long> dateIntTest = (ArrayList<Long>) calculateFacetedStat(dateIntTestStart, "count");
    assertEqualsList(responseStr, dateIntTest, dateInt);

    // Date Long
    ArrayList<Long> dateLong =
        getValueList(response, "count", "fieldFacets", "long_ld", "date", false);
    ArrayList<Long> dateLongTest =
        (ArrayList<Long>) calculateFacetedStat(dateLongTestStart, "count");
    assertEqualsList(responseStr, dateLong, dateLongTest);

    // String Int
    ArrayList<Long> stringInt =
        getValueList(response, "count", "fieldFacets", "int_id", "str", false);
    ArrayList<Long> stringIntTest =
        (ArrayList<Long>) calculateFacetedStat(stringIntTestStart, "count");
    assertEqualsList(responseStr, stringInt, stringIntTest);

    // String Long
    ArrayList<Long> stringLong =
        getValueList(response, "count", "fieldFacets", "long_ld", "str", false);
    ArrayList<Long> stringLongTest =
        (ArrayList<Long>) calculateFacetedStat(stringLongTestStart, "count");
    assertEqualsList(responseStr, stringLong, stringLongTest);
  }

  @Test
  public void missingTest() throws Exception {
    String[] params =
        new String[] {
          "o.missingn.s.int", "missing(int_id)",
          "o.missingn.s.long", "missing(long_ld)",
          "o.missingn.s.float", "missing(float_fd)",
          "o.missingn.s.double", "missing(double_dd)",
          "o.missingn.ff", "string_sd",
          "o.missingn.ff", "date_dtd",
          "o.missing.s.str", "missing(string_sd)",
          "o.missing.s.date", "missing(date_dtd)",
          "o.missing.ff", "int_id",
          "o.missing.ff", "long_ld"
        };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    // Int Date
    ArrayList<Long> intDate =
        getValueList(response, "missingn", "fieldFacets", "date_dtd", "int", false);
    setLatestType("int");
    assertEqualsList(responseStr, intDateTestMissing, intDate);

    // Int String
    ArrayList<Long> intString =
        getValueList(response, "missingn", "fieldFacets", "string_sd", "int", false);
    assertEqualsList(responseStr, intStringTestMissing, intString);

    // Long Date
    ArrayList<Long> longDate =
        getValueList(response, "missingn", "fieldFacets", "date_dtd", "long", false);
    setLatestType("long");
    assertEqualsList(responseStr, longDateTestMissing, longDate);

    // Long String
    ArrayList<Long> longString =
        getValueList(response, "missingn", "fieldFacets", "string_sd", "long", false);
    assertEqualsList(responseStr, longStringTestMissing, longString);

    // Float Date
    ArrayList<Long> floatDate =
        getValueList(response, "missingn", "fieldFacets", "date_dtd", "float", false);
    setLatestType("float");
    assertEqualsList(responseStr, floatDateTestMissing, floatDate);

    // Float String
    ArrayList<Long> floatString =
        getValueList(response, "missingn", "fieldFacets", "string_sd", "float", false);
    assertEqualsList(responseStr, floatStringTestMissing, floatString);

    // Double Date
    ArrayList<Long> doubleDate =
        getValueList(response, "missingn", "fieldFacets", "date_dtd", "double", false);
    setLatestType("double");
    assertEqualsList(responseStr, doubleDateTestMissing, doubleDate);

    // Double String
    ArrayList<Long> doubleString =
        getValueList(response, "missingn", "fieldFacets", "string_sd", "double", false);
    assertEqualsList(responseStr, doubleStringTestMissing, doubleString);

    // Date Int
    ArrayList<Long> dateInt =
        getValueList(response, "missing", "fieldFacets", "int_id", "date", false);
    setLatestType("date");
    assertEqualsList(responseStr, dateIntTestMissing, dateInt);

    // Date Long
    ArrayList<Long> dateLong =
        getValueList(response, "missing", "fieldFacets", "long_ld", "date", false);
    assertEqualsList(responseStr, dateLongTestMissing, dateLong);

    // String Int
    ArrayList<Long> stringInt =
        getValueList(response, "missing", "fieldFacets", "int_id", "str", false);
    setLatestType("string");
    assertEqualsList(responseStr, stringIntTestMissing, stringInt);

    // String Long
    ArrayList<Long> stringLong =
        getValueList(response, "missing", "fieldFacets", "long_ld", "str", false);
    assertEqualsList(responseStr, stringLongTestMissing, stringLong);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void multiValueTest() throws Exception {
    String[] params =
        new String[] {
          "o.multivalued.s.mean", "mean(int_id)",
          "o.multivalued.ff", "long_ldm",
          "o.multivalued.ff", "string_sdm",
          "o.multivalued.ff", "date_dtdm"
        };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    // Long
    ArrayList<Double> lon =
        getValueList(response, "multivalued", "fieldFacets", "long_ldm", "mean", false);
    ArrayList<Double> longTest = calculateFacetedNumberStat(multiLongTestStart, "mean");
    assertEqualsList(responseStr, lon, longTest);
    // Date
    ArrayList<Double> date =
        getValueList(response, "multivalued", "fieldFacets", "date_dtdm", "mean", false);
    ArrayList<Double> dateTest = calculateFacetedNumberStat(multiDateTestStart, "mean");
    assertEqualsList(responseStr, date, dateTest);
    // String
    ArrayList<Double> string =
        getValueList(response, "multivalued", "fieldFacets", "string_sdm", "mean", false);
    ArrayList<Double> stringTest = calculateFacetedNumberStat(multiStringTestStart, "mean");
    assertEqualsList(responseStr, string, stringTest);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void missingFacetTest() throws Exception {
    String[] params =
        new String[] {
          "o.missingf.s.mean", "mean(int_id)",
          "o.missingf.ff", "date_dtd",
          "o.missingf.ff", "string_sd",
          "o.missingf.ff.string_sd.sm", "true",
          "o.missingf.ff", "date_dtdm",
          "o.missingf.ff.date_dtdm.sm", "true"
        };
    NamedList<Object> response = queryLegacyCloudAnalytics(params);
    String responseStr = response.toString();

    // int MultiDate
    assertTrue(
        responseStr,
        responseContainsFacetValue(response, "missingf", "fieldFacets", "date_dtdm", "(MISSING)"));
    ArrayList<Double> string =
        getValueList(response, "missingf", "fieldFacets", "date_dtdm", "mean", false);
    ArrayList<Double> stringTest = calculateFacetedNumberStat(multiDateTestStart, "mean");
    assertEqualsList(responseStr, string, stringTest);

    // Int String
    assertTrue(
        responseStr,
        responseContainsFacetValue(response, "missingf", "fieldFacets", "string_sd", "(MISSING)"));
    assertFalse(
        responseStr,
        responseContainsFacetValue(response, "missingf", "fieldFacets", "string_sd", "str0"));
    ArrayList<Double> intString =
        getValueList(response, "missingf", "fieldFacets", "string_sd", "mean", false);
    ArrayList<Double> intStringTest = calculateFacetedNumberStat(intStringTestStart, "mean");
    assertEqualsList(responseStr, intString, intStringTest);

    // Int Date
    ArrayList<Double> intDate =
        getValueList(response, "missingf", "fieldFacets", "date_dtd", "mean", false);
    ArrayList<ArrayList<Double>> intDateMissingTestStart =
        (ArrayList<ArrayList<Double>>) intDateTestStart.clone();
    ArrayList<Double> intDateTest = calculateFacetedNumberStat(intDateMissingTestStart, "mean");
    assertEqualsList(responseStr, intDate, intDateTest);
  }

  private void checkStddevs(
      NamedList<Object> response, ArrayList<Double> list1, ArrayList<Double> list2) {
    Collections.sort(list1);
    Collections.sort(list2);
    for (int i = 0; i < list2.size(); i++) {
      if ((Math.abs(list1.get(i) - list2.get(i)) < .00000000001) == false) {
        assertEquals(response.toString(), list1.get(i), list2.get(i), 0.00000000001);
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
