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

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.solr.SolrTestCase;
import org.junit.Assert;
import org.junit.Test;

public class SolrJdbcUrlParserTest extends SolrTestCase {

  private static final Map<String, List<String>> ZOOKEEPERS =
      Map.of(
          "zoo1", List.of("zoo1"),
          "zoo1,zoo2,zoo3", List.of("zoo1", "zoo2", "zoo3"),
          "zoo1:9983,zoo2:9983,zoo3:9983", List.of("zoo1:9983", "zoo2:9983", "zoo3:9983"));
  private static final List<String> CHROOTS = List.of("", "/", "/foo", "/foo/bar");
  private static final Map<String, Map<String, String>> PARAMS =
      Map.of(
          "collection=collection1", Map.of("collection", "collection1"),
          "collection=collection1&test=test1",
              Map.of("collection", "collection1", "test", "test1"));

  private static final List<String> HTTP_URLS =
      List.of(
          "http://solr1/solr",
          "http://solr2:8080/solr",
          "http://solr3/foo/bar",
          "https://solr4/solr",
          "https://solr5:8443/solr",
          "https://solr6:8443/foo/bar");

  private static <T> boolean collectionEqual(Collection<T> coll, Collection<T> coll2) {
    return coll.size() == coll2.size()
        && Set.copyOf(coll).containsAll(coll2)
        && Set.copyOf(coll2).containsAll(coll);
  }

  @Test
  public void testProcessZookeeperUrl() throws Exception {
    for (String zkHostString : ZOOKEEPERS.keySet()) {
      for (String chroot : CHROOTS) {
        for (String paramString : PARAMS.keySet()) {
          String url = "jdbc:solr://" + zkHostString + chroot + "?" + paramString;

          var jdbcConnectionMetadata = DriverImpl.SolrJdbcUrlParser.parse(url, new Properties());
          var solrConnection = jdbcConnectionMetadata.solrConnection();
          List<String> expectedZookeepers = ZOOKEEPERS.get(zkHostString);
          Assert.assertTrue(solrConnection.isZookeeper());
          Assert.assertTrue(collectionEqual(expectedZookeepers, solrConnection.quorumItems()));
          String expectedChroot = chroot.isBlank() ? null : chroot;
          Assert.assertEquals(expectedChroot, solrConnection.zkChroot());

          verifyParams(jdbcConnectionMetadata.properties(), PARAMS.get(paramString));
        }
      }
    }
  }

  @Test
  public void testProcessHttpUrl() throws SQLException {
    for (String httpString : HTTP_URLS) {
      for (String paramString : PARAMS.keySet()) {
        String url = "jdbc:solr:" + httpString + "?" + paramString;

        var jdbcConnectionMetadata = DriverImpl.SolrJdbcUrlParser.parse(url, new Properties());
        var solrConnection = jdbcConnectionMetadata.solrConnection();
        Assert.assertFalse(solrConnection.isZookeeper());
        Assert.assertEquals(1, solrConnection.quorumItems().size());
        Assert.assertEquals(httpString, solrConnection.quorumItems().getFirst());

        verifyParams(jdbcConnectionMetadata.properties(), PARAMS.get(paramString));
      }
    }
  }

  private static void verifyParams(Properties actualProps, Map<String, String> expectedProps) {
    Assert.assertFalse(actualProps.containsKey("collection"));
    for (Map.Entry<String, String> param : expectedProps.entrySet()) {
      if (!"collection".equals(param.getKey())) {
        Assert.assertEquals(param.getValue(), actualProps.getProperty(param.getKey()));
      }
    }
  }
}
