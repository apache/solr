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

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.MapWriter.EntryWriter;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.response.SolrQueryResponse;

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

  /**
   * Convert a JacksonReflectMapWriter (typically a {@link
   * org.apache.solr.jersey.SolrJerseyResponse}) into the NamedList on a SolrQueryResponse
   *
   * @param rsp the response to attach the resulting NamedList to
   * @param mw the input object to be converted into a NamedList
   * @param trimHeader should the 'responseHeader' portion of the response be added to the
   *     NamedList, or should populating that header be left to code elsewhere. This value should
   *     usually be 'false' when called from v2 code, and 'true' when called from v1 code.
   */
  public static void squashIntoSolrResponse(
      SolrQueryResponse rsp, JacksonReflectMapWriter mw, boolean trimHeader) {
    squashIntoNamedList(rsp.getValues(), mw, trimHeader);
  }

  public static void squashIntoSolrResponse(SolrQueryResponse rsp, JacksonReflectMapWriter mw) {
    squashIntoSolrResponse(rsp, mw, false);
  }

  public static void squashIntoNamedList(
      NamedList<Object> destination, JacksonReflectMapWriter mw) {
    squashIntoNamedList(destination, mw, false);
  }

  private static void squashIntoNamedList(
      NamedList<Object> destination, JacksonReflectMapWriter mw, boolean trimHeader) {
    try {
      mw.writeMap(new EntryWriter() {
        @Override
        public EntryWriter put(CharSequence key, Object value) {
          var kStr = key.toString();
          if (trimHeader && kStr.equals("responseHeader")) {
            return null;
          }
          destination.add(kStr, value);
          return this; // returning "this" means we can't use a lambda :-(
        }
      });
    } catch (IOException e) {
      throw new RuntimeException(e); // impossible
    }

  }
}
