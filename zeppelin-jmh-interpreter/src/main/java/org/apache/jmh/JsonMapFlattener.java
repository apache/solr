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
package org.apache.jmh;

import org.apache.jmh.noggit.JSONParser;
import org.apache.jmh.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.UnaryOperator;

public abstract class JsonMapFlattener {

  private static final Logger log = LoggerFactory.getLogger(JsonMapFlattener.class);
  public static final String PRIMARY_METRIC_SCORE_UNIT = "primaryMetric.scoreUnit";
  public static final String PRIMARY_METRIC_SCORE = "primaryMetric.score";
  public static final String SCORE_UNIT = "scoreUnit";

  public static final String BENCHMARK = "benchmark";
  private static List<String> PREVENT_CONSTANT_PULL = Arrays.asList("score", "metric", BENCHMARK);

  private JsonMapFlattener() {
  }

  public static String mapToHtmlTable(Map<String,Object> map) {

    StringBuilder stringMapTable = new StringBuilder();
    stringMapTable.append("<table>");

    for (Map.Entry<String,Object> entry : map.entrySet()) {
      stringMapTable.append("<tr><td>" + entry.getKey() + "</td><td>" +entry.getValue() + "</td></tr>");
    //  System.out.println(entry.getKey() + " = " + entry.getValue());
    }

    return stringMapTable.toString();
  }

  public static Map<String,Object> flatten(Map<String,? extends Object> map) {
    Map<String,Object> result = new LinkedHashMap<>();
    flatten("", map.entrySet().iterator(), result, UnaryOperator.identity());
    return result;
  }

  public static Map<String,String> flattenToStringMap(Map<String,? extends Object> map) {
    Map<String,String> result = new LinkedHashMap<>();
    flatten("", map.entrySet().iterator(), result, it -> it == null ? null : it.toString());
    return result;
  }

  private static void flatten(String prefix, Iterator<? extends Entry<String,?>> inputMap, Map<String,? extends Object> resultMap,
      Function<Object,Object> valueTransformer) {
    prefix = prefix.trim();
    if (prefix.equals("secondaryMetrics")) {
      prefix = "";
    }
    if (prefix.trim().length() > 0) {
      prefix = prefix + ".";
    }
    while (inputMap.hasNext()) {
      Entry<String,? extends Object> entry = inputMap.next();
      flatten(prefix.concat(entry.getKey()), entry.getValue(), resultMap, valueTransformer);
    }
  }

  @SuppressWarnings("unchecked") private static void flatten(String prefix, Object source, Map<String,?> result, Function<Object,Object> valueTransformer) {
    if (source instanceof Iterable) {
      flattenCollection(prefix, (Iterable<Object>) source, result, valueTransformer);
      return;
    }
    if (source instanceof Map) {
      flatten(prefix, ((Map<String,?>) source).entrySet().iterator(), result, valueTransformer);
      return;
    }
    ((Map) result).put(prefix, valueTransformer.apply(source));
  }

  private static void flattenCollection(String prefix, Iterable<Object> iterable, Map<String,?> resultMap, Function<Object,Object> valueTransformer) {
    int cnt = 0;
    for (Object element : iterable) {
      flatten(prefix + "[" + cnt + "]", element, resultMap, valueTransformer);
      cnt++;
    }
  }

  public static StringBuilder zeppelinTableFromFlatMapHeader(Map<?,?> rows) {
    final StringBuilder table = new StringBuilder(256);

    int[] cnt = new int[] {0};
    rows.keySet().forEach(key -> {

      table.append(key);
      if (cnt[0]++ < rows.size() - 1) {
        table.append("\t");
      }
    });

    table.append("\n");

    return table;
  }

  public static StringBuilder zeppelinTableFromFlatMapAddTo(StringBuilder table, Map<?,?> rows) {
    int[] cnt = new int[] {0};
    rows.values().forEach(value -> {
      table.append(value);
      if (cnt[0]++ < rows.size() - 1) {
        table.append("\t");
      }
    });
    table.append("\n");

    return table;
  }

  public static String[] resultsToZeppelinTable(String json) throws IOException {
      log.info("json=\n{}", json);
      List<Map<String,Object>> flatMaps = flatMapsFromResult(json);

      //System.out.println("flatMaps: " + flatMaps);

     return flatMapsToZeppelinTable(flatMaps);
  }


  public static String[] flatMapsToZeppelinTable(List<Map<String,Object>> flatMaps) {
    String[] returnString = new String[2];


    try {

      //System.out.println("flatMaps: " + flatMaps);

      Map<String,Object> constantData = removeConstantData(flatMaps);

      Iterator<Map<String,Object>> flatMapIt = flatMaps.iterator();

      //    System.out.println("handle header");
      StringBuilder[] sb = new StringBuilder[1];
      Map<String,Object> firstMap = flatMapIt.next();
      sb[0] = zeppelinTableFromFlatMapHeader(firstMap);

      // System.out.println("handle rows");
      Set<String> headerKeys = firstMap.keySet();
      flatMaps.forEach(fm -> {
        Iterator<Entry<String,Object>> it = fm.entrySet().iterator();
        it.forEachRemaining(entry -> {
          if (!headerKeys.contains(entry.getKey()) && !entry.getKey().startsWith(
              PRIMARY_METRIC_SCORE)) {
            it.remove();
          }
        } );
      });


      flatMaps.forEach(fm -> zeppelinTableFromFlatMapAddTo(sb[0], fm));

      returnString[0] = mapToHtmlTable(constantData);

      returnString[1] = sb[0].toString();


      //  } else {
      //    throw new UnsupportedOperationException();
      //				@SuppressWarnings("unchecked")
      //				Map flatMap = flatten((Map<String,? extends Object>) value);
      //				//System.out.println("flat map:\n" + flatMap);
      //				System.out.println(zeppelinTableFromFlatMapHeader(flatMap));
      //   }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    //    String zeppelinOutput = returnString[0].toString();
    //   System.out.println("zeppelinOutput: " + zeppelinOutput);

    return returnString;
  }

  public static List<Map<String,Object>> flatMapsFromResult(String json) throws IOException {
    //System.out.println("json string: " + json);
    JSONParser jsonParser = new JSONParser(json);
    List<Map<String,Object>>  value = (List<Map<String,Object>>) new ObjectBuilder(jsonParser).getValStrict();

//    if (!(value instanceof List)) {
//    } else {
//      throw new UnsupportedOperationException();
//    }

    List<Map<String,Object>> flatMaps = new ArrayList<>();

    //Iterator<Map<String,Object>> it = ((List<Map<String,Object>>) value).iterator();
    //Map<String,? extends Object> first = it.next();
    //Map flatMap = flatten(first);


   // flatMaps.add(flatMap);
    AtomicInteger sz = new AtomicInteger(-1);
    value.forEach(map -> {
      log.info("json size {}",value.size());

      @SuppressWarnings("rawtypes") Map fm = flatten(map);

      if (sz.get() > -1) {
   //     assert fm.size() == sz.get() : fm.keySet();
      }
      sz.set(fm.size());
      log.info("map keys: {}", fm.keySet());
      log.info("flat map: {}", fm);

      Set<String> headerKeys = map.keySet();
      flatMaps.forEach(m -> {
        Iterator<Map.Entry<String,Object>> it = m.entrySet().iterator();
        it.forEachRemaining(entry -> {
          if (entry.getKey().equals(PRIMARY_METRIC_SCORE) || entry.getKey().equals("ops/s") || entry.getKey().equals(
              PRIMARY_METRIC_SCORE_UNIT)) {
            return;
          }
          if (!headerKeys.contains(entry.getKey())) {
            it.remove();
          }
        } );
      });

      if (!flatMaps.isEmpty()) {
        Set<String> headerKeys2 = flatMaps.iterator().next().keySet();
        Iterator<Entry<String,Object>> it = map.entrySet().iterator();
        it.forEachRemaining(e -> {
          if (e.getKey().equals(PRIMARY_METRIC_SCORE) || e.getKey().equals("ops/s") || e.getKey().equals(
              SCORE_UNIT)|| e.getKey().equals(
              PRIMARY_METRIC_SCORE_UNIT)) {
            return;
          }
          if (!headerKeys2.contains(e.getKey())) {
            it.remove();
          }

        });
      }

      flatMaps.add(process(fm));
    });
    return flatMaps;
  }
//
  private static Map<String,? extends Object> process(Map<String,Object> map) {
    Iterator<Entry<String,Object>> it = map.entrySet().iterator();
    it.forEachRemaining(e -> {
      if (e.getKey().toLowerCase(Locale.ROOT).contains("raw") || e.getKey().toLowerCase(Locale.ROOT).contains("error")
          || e.getKey().toLowerCase(Locale.ROOT).contains("confidence") || e.getKey().toLowerCase(Locale.ROOT).contains("percentile")
          || e.getKey().toLowerCase(Locale.ROOT).contains("jvmargs")) { //
        it.remove();
      }
    });
    return map;
  }

  private static Map<String,Object> removeConstantData(List<Map<String,Object>> mapList) {
    Map<String,Object> constantData = new LinkedHashMap<>();
   // List<String> doesNotVary = new ArrayList<>();
    Iterator<Map<String,Object>> it = mapList.iterator();
    if (!it.hasNext()) return constantData;
    constantData.putAll(it.next());

    for (Map<String,Object> map : mapList) {
      Set<? extends Entry<String,Object>> entries = map.entrySet();
      for (Entry<String,Object> entry : entries) {
        Object prev = constantData.get(entry.getKey());

        if (!entry.getValue().equals(prev) || preventConstantPull(entry.getKey())) {
          constantData.remove(entry.getKey());
        }
      }

       String shortBenchmark = null;
      if (map.containsKey(BENCHMARK)) {
        String val = (String) map.get(BENCHMARK);
        shortBenchmark = val.substring(val.lastIndexOf('.') + 1);
        map.replace(BENCHMARK, shortBenchmark);
      }

      if (map.containsKey(PRIMARY_METRIC_SCORE)) {

        Map<String,Object> newMap = new LinkedHashMap<>(map.size());

        Object val = map.remove(PRIMARY_METRIC_SCORE);
        Object unit = map.remove(PRIMARY_METRIC_SCORE_UNIT);

        if (shortBenchmark != null) {
          map.remove(BENCHMARK);
          newMap.put(BENCHMARK, shortBenchmark);
        }
        newMap.put(unit.toString(), val);
        newMap.putAll(map);

        map.clear();
        map.putAll(newMap);
      }


    }

    for (String key : constantData.keySet()) {
      for (Map<String,Object> map : mapList) {
        map.remove(key);
      }
    }
    for (Map<String,Object> map : mapList) {
      log.info("map after constant data remove={}", map);
    }


    return constantData;
  }

  private static boolean preventConstantPull(String key) {
    boolean prevent = false;
    for (String val : PREVENT_CONSTANT_PULL) {
      if (key.toLowerCase(Locale.ROOT).contains(val.toLowerCase(Locale.ROOT))) {
        prevent = true;
        break;
       }
    }
    return prevent;
  }

  public static String flatMapsToJSArray( List<Map<String,Object>> flatMaps) {
    String returnString;
//    data = Array(26) [
//        0: Object {
//      name: "E"
//      value: 0.12702
//    }
//    1: Object {
//      name: "T"
//      value: 0.09056
//    }
//    2: Object {name: "A", value: 0.08167}
//    3: Object {name: "O", value: 0.07507}
//    4: Object {name: "I", value: 0.06966}
//    5: Object {name: "N", value: 0.06749}
//    6: Object {name: "S", value: 0.06327}
//    7: Object {name: "H", value: 0.06094}
//    8: Object {name: "R", value: 0.05987}
//    9: Object {name: "D", value: 0.04253}
//    10: Object {name: "L", value: 0.04025}
//    11: Object {name: "C", value: 0.02782}
//    12: Object {name: "U", value: 0.02758}
//    13: Object {name: "M", value: 0.02406}
//    14: Object {name: "W", value: 0.0236}
//    15: Object {name: "F", value: 0.02288}
//    16: Object {name: "G", value: 0.02015}
//    17: Object {name: "Y", value: 0.01974}
//    18: Object {name: "P", value: 0.01929}
//    19: Object {name: "B", value: 0.01492}
//    20: Object {name: "V", value: 0.00978}
//    21: Object {name: "K", value: 0.00772}
//    22: Object {name: "J", value: 0.00153}
//    23: Object {name: "X", value: 0.0015}
//    24: Object {name: "Q", value: 0.00095}
//    25: Object {name: "Z", value: 0.00074}
//    columns: Array(2) [
//        0: "letter"
//    1: "frequency"
//]
//    format: "%"
//    y: "↑ Frequency"
//]

    try {

      //System.out.println("flatMaps: " + flatMaps);

      Map<String,Object> constantData = removeConstantData(flatMaps);

      Iterator<Map<String,Object>> flatMapIt = flatMaps.iterator();

      //    System.out.println("handle header");
      StringBuilder[] sb = new StringBuilder[1];
      sb[0] = zeppelinTableFromFlatMapHeader(flatMapIt.next());

      // System.out.println("handle rows");
      flatMapIt.forEachRemaining(fm -> zeppelinTableFromFlatMapAddTo(sb[0], fm));



      returnString = sb[0].toString();


      //  } else {
      //    throw new UnsupportedOperationException();
      //				@SuppressWarnings("unchecked")
      //				Map flatMap = flatten((Map<String,? extends Object>) value);
      //				//System.out.println("flat map:\n" + flatMap);
      //				System.out.println(zeppelinTableFromFlatMapHeader(flatMap));
      //   }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    //    String zeppelinOutput = returnString[0].toString();
    //   System.out.println("zeppelinOutput: " + zeppelinOutput);

    return returnString;
  }

  public static void main(String[] args) throws IOException {
    System.out.println(resultsToZeppelinTable("/data3-ext4/apache-solr/solr/benchmark"));
  }

}