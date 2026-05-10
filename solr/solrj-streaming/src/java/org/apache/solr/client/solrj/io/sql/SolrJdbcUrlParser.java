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
package org.apache.solr.client.solrj.io.sql;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;
import org.apache.solr.client.solrj.impl.CloudSolrClient;

public class SolrJdbcUrlParser {
  public static JdbcConnectionMetadata parse(String jdbcUrl, Properties properties)
      throws SQLException {
    URI uri = toURI(jdbcUrl);

    Properties effectiveProps = new Properties();
    effectiveProps.putAll(properties);
    loadParams(uri, effectiveProps);

    String collection = effectiveProps.getProperty("collection");
    if (collection == null) {
      throw new SQLException("Missing required property 'collection' in JDBC URL or properties.");
    }
    effectiveProps.remove("collection");
    effectiveProps.putIfAbsent("aggregationMode", "facet");
    effectiveProps.setProperty("includeMetadata", "true");

    String connectionString =
        isHttpScheme(uri.getScheme())
            ? uri.getScheme() + "://" + uri.getAuthority() + uri.getPath()
            : uri.getAuthority() + uri.getPath();

    var solrConnection = CloudSolrClient.CloudSolrClientConnection.parse(connectionString);

    return new JdbcConnectionMetadata(jdbcUrl, solrConnection, collection, effectiveProps);
  }

  private static URI toURI(String url) throws SQLException {
    String uriString = removePrefix(url);
    URI uri;
    try {
      uri = new URI(uriString);
    } catch (URISyntaxException e) {
      throw new SQLException(String.format(Locale.ROOT, "Invalid JDBC URL '%s'.", url), e);
    }
    if (uri.getAuthority() == null) {
      throw new SQLException(
          String.format(Locale.ROOT, "Invalid JDBC URL '%s': missing host.", url));
    }

    return uri;
  }

  /**
   * Removes the {@code jdbc:solr:} prefix from a Solr JDBC URL.
   *
   * <p>Examples: {@code jdbc:solr://zoo1:2181/solr?collection=coll1} → {@code
   * zoo1:2181/solr?collection=coll1} {@code jdbc:solr:http://solr1:8083/solr?collection=coll1} →
   * {@code http://solr1:8083/solr?collection=coll1}
   */
  private static String removePrefix(String url) throws SQLException {
    String uriString;
    if (url.startsWith("jdbc:solr://")) {
      uriString = url.substring("jdbc:".length());
    } else if (url.startsWith("jdbc:solr:http://") || url.startsWith("jdbc:solr:https://")) {
      uriString = url.substring("jdbc:solr:".length());
    } else {
      throw new SQLException(
          String.format(
              Locale.ROOT,
              "Invalid JDBC URL '%s'. Expected prefixes: "
                  + "'jdbc:solr://', 'jdbc:solr:http://', or 'jdbc:solr:https://'.",
              url));
    }
    return uriString;
  }

  /** Decode the uri query parameters and put them onto {@code props}. */
  private static void loadParams(URI uri, Properties props) {
    String query = uri.getRawQuery();
    if (query != null) {
      for (String param : query.split("&")) {
        String[] pair = param.split("=", 2);
        String key = URLDecoder.decode(pair[0], StandardCharsets.UTF_8);
        String value = pair.length > 1 ? URLDecoder.decode(pair[1], StandardCharsets.UTF_8) : "";
        props.put(key, value);
      }
    }
  }

  private static boolean isHttpScheme(String scheme) {
    return "http".equals(scheme) || "https".equals(scheme);
  }

  public record JdbcConnectionMetadata(
      String originalUrl,
      CloudSolrClient.CloudSolrClientConnection solrConnection,
      String collection,
      Properties properties) {}
}
