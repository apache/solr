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
package org.apache.solr.client.solrj.util;

import java.io.IOException;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.XML;

/**
 * @since solr 1.3
 */
public class ClientUtils {
  // Standard Content types
  public static final String TEXT_XML = "application/xml; charset=UTF-8";
  public static final String TEXT_JSON = "application/json; charset=UTF-8";

  public static final String DEFAULT_PATH = "/select";

  /**
   * Create the full URL for a SolrRequest (excepting query parameters) as a String
   *
   * @param solrRequest the {@link SolrRequest} to build the URL for
   * @param serverRootUrl the root URL of the Solr server being targeted.
   * @param collection the collection to send the request to. May be null if no collection is
   *     needed.
   * @throws MalformedURLException if {@code serverRootUrl} contains a malformed URL string
   */
  public static String buildRequestUrl(
      SolrRequest<?> solrRequest, String serverRootUrl, String collection)
      throws MalformedURLException {

    String basePath = Objects.requireNonNull(serverRootUrl);
    if (solrRequest.getApiVersion() == SolrRequest.ApiVersion.V2) {
      basePath = addNormalV2ApiRoot(basePath);
    }

    if (solrRequest.requiresCollection() && collection != null) basePath += "/" + collection;

    String path = solrRequest.getPath();
    if (path == null || !path.startsWith("/")) {
      path = DEFAULT_PATH;
    }

    return basePath + path;
  }

  private static String addNormalV2ApiRoot(String basePath) throws MalformedURLException {
    final var oldURI = URI.create(basePath);
    final var revisedPath = buildReplacementV2Path(oldURI.getPath());
    return oldURI.resolve(revisedPath).toString();
  }

  private static String buildReplacementV2Path(String existingPath) {
    if (existingPath.contains("/solr")) {
      return existingPath.replaceFirst("/solr", "/api");
    } else if (existingPath.endsWith("/api")) {
      return existingPath;
    } else {
      return existingPath + "/api";
    }
  }

  // ------------------------------------------------------------------------
  // ------------------------------------------------------------------------

  @SuppressWarnings({"unchecked"})
  public static void writeXML(SolrInputDocument doc, Writer writer) throws IOException {
    writer.write("<doc>");

    for (SolrInputField field : doc) {
      String name = field.getName();

      for (Object v : field) {
        String update = null;

        if (v instanceof SolrInputDocument) {
          writeVal(writer, name, v, null);
        } else if (v instanceof Map) {
          // currently, only supports a single value
          for (Entry<Object, Object> entry : ((Map<Object, Object>) v).entrySet()) {
            update = entry.getKey().toString();
            v = entry.getValue();
            if (v instanceof Collection<?> values) {
              for (Object value : values) {
                writeVal(writer, name, value, update);
              }
            } else {
              writeVal(writer, name, v, update);
            }
          }
        } else {
          writeVal(writer, name, v, update);
        }
      }
    }

    if (doc.hasChildDocuments()) {
      for (SolrInputDocument childDocument : doc.getChildDocuments()) {
        writeXML(childDocument, writer);
      }
    }

    writer.write("</doc>");
  }

  private static void writeVal(Writer writer, String name, Object v, String update)
      throws IOException {
    if (v instanceof Date) {
      v = ((Date) v).toInstant().toString();
    } else if (v instanceof byte[] bytes) {
      v = Base64.getEncoder().encodeToString(bytes);
    } else if (v instanceof ByteBuffer bytes) {
      v =
          new String(
              Base64.getEncoder()
                  .encode(
                      ByteBuffer.wrap(
                          bytes.array(),
                          bytes.arrayOffset() + bytes.position(),
                          bytes.limit() - bytes.position()))
                  .array(),
              StandardCharsets.ISO_8859_1);
    }

    XML.Writable valWriter = null;
    if (v instanceof SolrInputDocument solrDoc) {
      valWriter = (writer1) -> writeXML(solrDoc, writer1);
    } else if (v != null) {
      final Object val = v;
      valWriter = (writer1) -> XML.escapeCharData(val.toString(), writer1);
    }

    if (update == null) {
      if (v != null) {
        XML.writeXML(writer, "field", valWriter, "name", name);
      }
    } else {
      if (v == null) {
        XML.writeXML(
            writer, "field", (XML.Writable) null, "name", name, "update", update, "null", true);
      } else {
        XML.writeXML(writer, "field", valWriter, "name", name, "update", update);
      }
    }
  }

  // ---------------------------------------------------------------------------------------

  /**
   * See: <a href="https://www.google.com/?gws_rd=ssl#q=lucene+query+parser+syntax">Lucene query
   * parser syntax</a> for more information on Escaping Special Characters
   */
  // NOTE: It is a broken link to any lucene-queryparser.jar docs, not in classpath!!!!!
  public static String escapeQueryChars(String s) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      // These characters are part of the query syntax and must be escaped
      if (c == '\\'
          || c == '+'
          || c == '-'
          || c == '!'
          || c == '('
          || c == ')'
          || c == ':'
          || c == '^'
          || c == '['
          || c == ']'
          || c == '\"'
          || c == '{'
          || c == '}'
          || c == '~'
          || c == '*'
          || c == '?'
          || c == '|'
          || c == '&'
          || c == ';'
          || c == '/'
          || Character.isWhitespace(c)) {
        sb.append('\\');
      }
      sb.append(c);
    }
    return sb.toString();
  }

  /**
   * Returns the (literal) value encoded properly, so it can be appended after a <code>name=</code>
   * local-param key.
   *
   * <p>NOTE: This method assumes <code>$</code> is a literal character that must be quoted. (It
   * does not assume strings starting <code>$</code> should be treated as param references)
   */
  public static String encodeLocalParamVal(String val) {
    int len = val.length();
    if (0 == len) return "''"; // quoted empty string

    // Note: QueryParsing#parseLocalParams's peek() (used to check for a '=' or the closing quote
    // char) skips leading whitespace as a side effect, so an unquoted empty value would silently
    // absorb the whitespace meant to separate it from the next local param, corrupting parsing of
    // everything that follows. Quoting sidesteps this entirely.

    int i = 0;
    char first = val.charAt(0);
    // A leading '$' would be read back as a param dereference, and a leading quote char would be
    // read back as the start of a quoted string (StrParser#getQuotedString accepts both ' and "
    // as delimiters); both must be quoted regardless of the rest of the value.
    if (first == '$' || first == '\'' || first == '"') {
      // leave i == 0 so the quoting branch below is taken
    } else {
      for (; i < len; i++) {
        char ch = val.charAt(i);
        if (Character.isWhitespace(ch) || ch == '}') break;
      }
    }

    if (i >= len) return val;

    // We need to enclose in quotes... but now we need to escape.  Both the quote delimiter itself
    // and a literal backslash must be escaped: StrParser#getQuotedString treats any '\' as the
    // start of an escape sequence when reading a quoted value, so an un-escaped '\' here would be
    // silently consumed (or worse, combined with the following char into an unintended escape like
    // \n) when the value is parsed back.
    StringBuilder sb = new StringBuilder(val.length() + 4);
    sb.append('\'');
    for (i = 0; i < len; i++) {
      char ch = val.charAt(i);
      if (ch == '\'' || ch == '\\') {
        sb.append('\\');
      }
      sb.append(ch);
    }
    sb.append('\'');
    return sb.toString();
  }

  /**
   * Constructs a slices map from a collection of slices and handles disambiguation if multiple
   * collections are being queried simultaneously
   */
  public static void addSlices(
      Map<String, Slice> target,
      String collectionName,
      Collection<Slice> slices,
      boolean multiCollection) {
    for (Slice slice : slices) {
      String key = slice.getName();
      if (multiCollection) key = collectionName + "_" + key;
      target.put(key, slice);
    }
  }

  /**
   * Determines whether any SolrClient "default" collection should apply to the specified request
   *
   * @param providedCollection a collection/core explicitly provided to the SolrClient (typically
   *     through {@link org.apache.solr.client.solrj.SolrClient#request(SolrRequest, String)}
   * @param request the {@link SolrRequest} being executed
   */
  public static boolean shouldApplyDefaultCollection(
      String providedCollection, SolrRequest<?> request) {
    return providedCollection == null && request.requiresCollection();
  }
}
