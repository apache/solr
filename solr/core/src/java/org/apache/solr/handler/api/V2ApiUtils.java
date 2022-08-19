/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.api;

import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.response.SolrQueryResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utilities helpful for common V2 API declaration tasks. */
public class V2ApiUtils {
  private V2ApiUtils() {
    /* Private ctor prevents instantiation */
  }

  public static void flattenMapWithPrefix(
      Map<String, Object> toFlatten, Map<String, Object> destination, String additionalPrefix) {
    if (toFlatten == null || toFlatten.isEmpty() || destination == null) {
      return;
    }

    toFlatten.forEach((k, v) -> destination.put(additionalPrefix + k, v));
  }

  public static void flattenToCommaDelimitedString(
      Map<String, Object> destination, List<String> toFlatten, String newKey) {
    final String flattenedStr = String.join(",", toFlatten);
    destination.put(newKey, flattenedStr);
  }


  // TODO Come up with a better approach (maybe change Responses to be based on some class that can natively do this
  //  without the intermediate map(s)?)
  public static void squashIntoSolrResponse(SolrQueryResponse rsp, ReflectMapWriter mw) {
    Map<String, Object> myMap = new HashMap<>();
    myMap = mw.toMap(myMap);
    for (Map.Entry<String, Object> entry : myMap.entrySet()) {
      rsp.add(entry.getKey(), entry.getValue());
    }
  }
}
