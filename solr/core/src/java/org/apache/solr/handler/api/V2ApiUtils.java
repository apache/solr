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

import static org.apache.solr.client.solrj.impl.BinaryResponseParser.BINARY_CONTENT_TYPE_V2;
import static org.apache.solr.common.params.CommonParams.WT;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.MapWriter.EntryWriter;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/** Utilities helpful for common V2 API declaration tasks. */
public class V2ApiUtils {
  private V2ApiUtils() {
    /* Private ctor prevents instantiation */
  }

  public static boolean isEnabled() {
    return !"true".equals(System.getProperty("disable.v2.api", "false"));
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
   * Convert a JacksonReflectMapWriter (typically a {@link SolrJerseyResponse}) into the NamedList
   * on a SolrQueryResponse, omitting the response header
   *
   * @param rsp the response to attach the resulting NamedList to
   * @param mw the input object to be converted into a NamedList
   */
  public static void squashIntoSolrResponseWithoutHeader(SolrQueryResponse rsp, Object mw) {
    squashObjectIntoNamedList(rsp.getValues(), mw, true);
  }

  /**
   * Convert a JacksonReflectMapWriter (typically a {@link SolrJerseyResponse}) into the NamedList
   * on a SolrQueryResponse, including the response header
   *
   * @param rsp the response to attach the resulting NamedList to
   * @param mw the input object to be converted into a NamedList
   */
  public static void squashIntoSolrResponseWithHeader(SolrQueryResponse rsp, Object mw) {
    squashObjectIntoNamedList(rsp.getValues(), mw, false);
  }

  public static void squashIntoNamedList(NamedList<Object> destination, Object mw) {
    squashObjectIntoNamedList(destination, mw, false);
  }

  public static void squashIntoNamedListWithoutHeader(
      NamedList<Object> destination, Object toSquash) {
    squashObjectIntoNamedList(destination, toSquash, true);
  }

  public static String getMediaTypeFromWtParam(
      SolrQueryRequest solrQueryRequest, String defaultMediaType) {
    final String wtParam = solrQueryRequest.getParams().get(WT);
    if (wtParam == null) return "application/json";

    // The only currently-supported response-formats for JAX-RS v2 endpoints.
    switch (wtParam) {
      case "xml":
        return "application/xml";
      case "javabin":
        return BINARY_CONTENT_TYPE_V2;
      default:
        return defaultMediaType;
    }
  }

  public static void squashObjectIntoNamedList(
      NamedList<Object> destination, Object o, boolean trimHeader) {
    final var ew =
        new EntryWriter() {
          @Override
          public EntryWriter put(CharSequence key, Object value) {
            var kStr = key.toString();
            if (trimHeader && kStr.equals("responseHeader")) {
              return null;
            }
            destination.add(kStr, value);
            return this; // returning "this" means we can't use a lambda :-(
          }
        };
    Utils.reflectWrite(
        ew,
        o,
        // TODO Should we be lenient here and accept both the Jackson and our homegrown annotation?
        field -> field.getAnnotation(JsonProperty.class) != null,
        JsonAnyGetter.class,
        field -> {
          final JsonProperty prop = field.getAnnotation(JsonProperty.class);
          return prop.value().isEmpty() ? field.getName() : prop.value();
        });
  }
}
