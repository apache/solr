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
package org.apache.solr.client.solrj.impl;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.solr.SolrTestCase;
import org.junit.Assert;
import org.junit.Test;

public class CloudSolrClientConnectionTest extends SolrTestCase {

  private static <T> boolean collectionEqual(Collection<T> coll, Collection<T> coll2) {
    return coll.size() == coll2.size()
        && Set.copyOf(coll).containsAll(coll2)
        && Set.copyOf(coll2).containsAll(coll);
  }

  @Test
  public void testBuildQuorumForZk() {
    CloudSolrClient.CloudSolrClientConnection parsed =
        CloudSolrClient.CloudSolrClientConnection.parse(
            "zookeeper1:2181,zookeeper2:2181,zookeeper3:2181/solr");
    Assert.assertTrue(parsed.isZk());
    List<String> expectedQuorum = List.of("zookeeper1:2181", "zookeeper2:2181", "zookeeper3:2181");
    Assert.assertTrue(collectionEqual(expectedQuorum, parsed.quorumItems()));
    Assert.assertEquals("/solr", parsed.zkChroot());
  }

  @Test
  public void testBuildQuorumForZkIpV4() {
    CloudSolrClient.CloudSolrClientConnection parsed =
        CloudSolrClient.CloudSolrClientConnection.parse(
            "192.0.2.10:2181,192.0.2.11:2181,192.0.2.12:2181/solr");
    Assert.assertTrue(parsed.isZk());
    List<String> expectedQuorum = List.of("192.0.2.10:2181", "192.0.2.11:2181", "192.0.2.12:2181");
    Assert.assertTrue(collectionEqual(expectedQuorum, parsed.quorumItems()));
    Assert.assertEquals("/solr", parsed.zkChroot());
  }

  @Test
  public void testBuildQuorumForZkIpV6() {
    CloudSolrClient.CloudSolrClientConnection parsed =
        CloudSolrClient.CloudSolrClientConnection.parse(
            "[2001:db8:85a3::8a2e:370:7334]:2181,"
                + "[2001:db8:85a3::8a2e:370:7335]:2181,"
                + "[2001:db8:85a3::8a2e:370:7336]:2181/solr");
    Assert.assertTrue(parsed.isZk());
    List<String> expectedQuorum =
        List.of(
            "[2001:db8:85a3::8a2e:370:7334]:2181",
            "[2001:db8:85a3::8a2e:370:7335]:2181",
            "[2001:db8:85a3::8a2e:370:7336]:2181");
    Assert.assertTrue(collectionEqual(expectedQuorum, parsed.quorumItems()));
    Assert.assertEquals("/solr", parsed.zkChroot());
  }

  @Test
  public void testBuildQuorumForZkNoChroot() {
    CloudSolrClient.CloudSolrClientConnection parsed =
        CloudSolrClient.CloudSolrClientConnection.parse(
            "zookeeper1:2181,zookeeper2:2181,zookeeper3:2181");
    Assert.assertTrue(parsed.isZk());
    List<String> expectedQuorum = List.of("zookeeper1:2181", "zookeeper2:2181", "zookeeper3:2181");
    Assert.assertTrue(collectionEqual(expectedQuorum, parsed.quorumItems()));
    Assert.assertNull(parsed.zkChroot());
  }

  @Test
  public void testBuildQuorumForHttp() {
    CloudSolrClient.CloudSolrClientConnection parsed =
        CloudSolrClient.CloudSolrClientConnection.parse(
            "http://solr1:8983/solr,http://solr2:8983/solr,http://solr3:8983/solr");
    Assert.assertFalse(parsed.isZk());
    List<String> expectedQuorum =
        List.of("http://solr1:8983/solr", "http://solr2:8983/solr", "http://solr3:8983/solr");
    Assert.assertTrue(collectionEqual(expectedQuorum, parsed.quorumItems()));
    Assert.assertNull(parsed.zkChroot());
  }

  @Test
  public void testBuildQuorumForHttps() {
    CloudSolrClient.CloudSolrClientConnection parsed =
        CloudSolrClient.CloudSolrClientConnection.parse(
            "https://solr1:8983/solr,https://solr2:8983/solr,https://solr3:8983/solr");
    Assert.assertFalse(parsed.isZk());
    List<String> expectedQuorum =
        List.of("https://solr1:8983/solr", "https://solr2:8983/solr", "https://solr3:8983/solr");
    Assert.assertTrue(collectionEqual(expectedQuorum, parsed.quorumItems()));
    Assert.assertNull(parsed.zkChroot());
  }

  @Test
  public void testBuildQuorumForIpV4() {
    CloudSolrClient.CloudSolrClientConnection parsed =
        CloudSolrClient.CloudSolrClientConnection.parse(
            "http://192.0.2.10:8983/solr,"
                + "http://192.0.2.11:8983/solr,"
                + "http://192.0.2.12:8983/solr");
    Assert.assertFalse(parsed.isZk());
    List<String> expectedQuorum =
        List.of(
            "http://192.0.2.10:8983/solr",
            "http://192.0.2.11:8983/solr",
            "http://192.0.2.12:8983/solr");
    Assert.assertTrue(collectionEqual(expectedQuorum, parsed.quorumItems()));
    Assert.assertNull(parsed.zkChroot());
  }

  @Test
  public void testBuildQuorumForIpV6() {
    CloudSolrClient.CloudSolrClientConnection parsed =
        CloudSolrClient.CloudSolrClientConnection.parse(
            "http://[2001:db8:85a3::8a2e:370:7334]:8983/solr,"
                + "http://[2001:db8:85a3::8a2e:370:7334]:8983/solr,"
                + "http://[2001:db8:85a3::8a2e:370:7334]:8983/solr");
    Assert.assertFalse(parsed.isZk());
    List<String> expectedQuorum =
        List.of(
            "http://[2001:db8:85a3::8a2e:370:7334]:8983/solr",
            "http://[2001:db8:85a3::8a2e:370:7334]:8983/solr",
            "http://[2001:db8:85a3::8a2e:370:7334]:8983/solr");
    Assert.assertTrue(collectionEqual(expectedQuorum, parsed.quorumItems()));
    Assert.assertNull(parsed.zkChroot());
  }

  @Test
  public void testEmptyOrNullConnStringException() {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> CloudSolrClient.CloudSolrClientConnection.parse("   "));
  }
}
