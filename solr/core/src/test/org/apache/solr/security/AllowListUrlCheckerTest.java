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

package org.apache.solr.security;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.junit.Test;

/** Tests {@link AllowListUrlChecker}. */
public class AllowListUrlCheckerTest extends SolrTestCaseJ4 {

  @Test
  public void testAllowAll() throws Exception {
    AllowListUrlChecker checker = new AllowListUrlChecker(urls("http://cde:8983"));
    SolrException e =
        expectThrows(
            SolrException.class, () -> checker.checkAllowList(urls("abc-1.com:8983/solr")));
    assertThat(e.code(), is(SolrException.ErrorCode.FORBIDDEN.code));

    AllowListUrlChecker.ALLOW_ALL.checkAllowList(urls("abc-1.com:8983/solr"));
  }

  @Test
  public void testNoInput() throws Exception {
    assertThat(
        new AllowListUrlChecker(Collections.emptyList()).getHostAllowList().isEmpty(), is(true));
  }

  @Test
  public void testSingleHost() throws Exception {
    AllowListUrlChecker checker = new AllowListUrlChecker(urls("http://abc-1.com:8983/solr"));
    checker.checkAllowList(urls("http://Abc-1.Com:8983/solr"));
  }

  @Test
  public void testMultipleHosts() throws Exception {
    AllowListUrlChecker checker =
        new AllowListUrlChecker(
            urls("http://abc-1.com:8983", "http://abc-2.com:8983", "http://ABC-3.com:8983"));
    checker.checkAllowList(
        urls(
            "http://abc-3.com:8983/solr",
            "http://abc-1.com:8983/solr",
            "http://abc-2.com:8983/solr"));
  }

  @Test
  public void testNoProtocol() throws Exception {
    AllowListUrlChecker checker =
        new AllowListUrlChecker(
            urls("http://abc-1.com:8983", "http://abc-2.com:8983", "abc-3.com:8983"));
    checker.checkAllowList(urls("abc-1.com:8983/solr", "abc-3.com:8983/solr"));
  }

  @Test
  public void testDisallowedHost() throws Exception {
    AllowListUrlChecker checker =
        new AllowListUrlChecker(
            urls("http://abc-1.com:8983", "http://abc-2.com:8983", "http://abc-3.com:8983"));
    SolrException e =
        expectThrows(
            SolrException.class, () -> checker.checkAllowList(urls("http://abc-4.com:8983/solr")));
    assertThat(e.code(), is(SolrException.ErrorCode.FORBIDDEN.code));
    assertThat(e.getMessage(), containsString("http://abc-4.com:8983/solr"));
  }

  @Test
  public void testDisallowedHostNoProtocol() throws Exception {
    AllowListUrlChecker checker =
        new AllowListUrlChecker(
            urls("http://abc-1.com:8983", "http://abc-2.com:8983", "http://abc-3.com:8983"));
    SolrException e =
        expectThrows(
            SolrException.class,
            () -> checker.checkAllowList(urls("abc-1.com:8983/solr", "abc-4.com:8983/solr")));
    assertThat(e.code(), is(SolrException.ErrorCode.FORBIDDEN.code));
    assertThat(e.getMessage(), containsString("abc-4.com:8983/solr"));
  }

  @Test
  public void testProtocols() throws Exception {
    AllowListUrlChecker checker =
        new AllowListUrlChecker(
            urls("http://abc-1.com:8983", "http://abc-2.com:8983", "http://abc-3.com:8983"));
    checker.checkAllowList(urls("https://abc-1.com:8983/solr", "https://abc-2.com:8983/solr"));
    checker.checkAllowList(urls("s3://abc-1.com:8983/solr"));
  }

  @Test
  public void testInvalidUrlInConstructor() {
    MalformedURLException e =
        expectThrows(
            MalformedURLException.class,
            () -> new AllowListUrlChecker(urls("http/abc-1.com:8983")));
    assertThat(e.getMessage(), containsString("http/abc-1.com:8983"));
  }

  @Test
  public void testInvalidUrlInCheck() throws Exception {
    AllowListUrlChecker checker =
        new AllowListUrlChecker(
            urls("http://abc-1.com:8983", "http://abc-2.com:8983", "http://abc-3.com:8983"));
    MalformedURLException e =
        expectThrows(
            MalformedURLException.class,
            () -> checker.checkAllowList(urls("http://abc-1.com:8983", "abc-2")));
    assertThat(e.getMessage(), containsString("abc-2"));
  }

  @Test
  public void testCoreSpecific() throws Exception {
    // Cores are removed completely, so it doesn't really matter if they were set in config.
    AllowListUrlChecker checker =
        new AllowListUrlChecker(
            urls("http://abc-1.com:8983/solr/core1", "http://abc-2.com:8983/solr2/core2"));
    checker.checkAllowList(urls("abc-1.com:8983/solr/core3", "http://abc-2.com:8983/solr"));
  }

  @Test
  public void testHostParsingUnsetEmpty() throws Exception {
    assertThat(AllowListUrlChecker.parseHostPorts(Collections.emptyList()).isEmpty(), is(true));
  }

  @Test
  public void testHostParsingSingle() throws Exception {
    assertThat(
        AllowListUrlChecker.parseHostPorts(urls("http://abc-1.com:8983/solr/core1")),
        equalTo(hosts("abc-1.com:8983")));
  }

  @Test
  public void testHostParsingMulti() throws Exception {
    assertThat(
        AllowListUrlChecker.parseHostPorts(
            urls("http://abc-1.com:8983/solr/core1", "http://abc-1.com:8984/solr")),
        equalTo(hosts("abc-1.com:8983", "abc-1.com:8984")));
  }

  @Test
  public void testHostParsingIpv4() throws Exception {
    assertThat(
        AllowListUrlChecker.parseHostPorts(
            urls("http://10.0.0.1:8983/solr/core1", "http://127.0.0.1:8984/solr")),
        equalTo(hosts("10.0.0.1:8983", "127.0.0.1:8984")));
  }

  @Test
  public void testHostParsingIpv6() throws Exception {
    assertThat(
        AllowListUrlChecker.parseHostPorts(
            urls(
                "http://[2001:abc:abc:0:0:123:456:1234]:8983/solr/core1",
                "http://[::1]:8984/solr")),
        equalTo(hosts("[2001:abc:abc:0:0:123:456:1234]:8983", "[::1]:8984")));
  }

  @Test
  public void testHostParsingHttps() throws Exception {
    assertThat(
        AllowListUrlChecker.parseHostPorts(urls("https://abc-1.com:8983/solr/core1")),
        equalTo(hosts("abc-1.com:8983")));
  }

  @Test
  public void testHostParsingNoProtocol() throws Exception {
    assertThat(
        AllowListUrlChecker.parseHostPorts(urls("abc-1.com:8983/solr")),
        equalTo(AllowListUrlChecker.parseHostPorts(urls("http://abc-1.com:8983/solr"))));
    assertThat(
        AllowListUrlChecker.parseHostPorts(urls("abc-1.com:8983/solr")),
        equalTo(AllowListUrlChecker.parseHostPorts(urls("https://abc-1.com:8983/solr"))));
  }

  @Test
  public void testLiveNodesToHostUrlCache() throws Exception {
    // Given some live nodes defined in the cluster state.
    Set<String> liveNodes = Set.of("1.2.3.4:8983_solr", "1.2.3.4:9000_", "1.2.3.4:9001_solr-2");
    ClusterState clusterState1 = new ClusterState(liveNodes, new HashMap<>());

    // When we call the AllowListUrlChecker.checkAllowList method on both valid and invalid urls.
    AtomicInteger callCount = new AtomicInteger();
    AllowListUrlChecker checker =
        new AllowListUrlChecker(List.of()) {
          @Override
          Set<String> buildLiveHostUrls(Set<String> liveNodes) {
            callCount.incrementAndGet();
            return super.buildLiveHostUrls(liveNodes);
          }
        };
    for (int i = 0; i < 3; i++) {
      checker.checkAllowList(
          List.of("1.2.3.4:8983", "1.2.3.4:9000", "1.2.3.4:9001"), clusterState1);
      SolrException exception =
          expectThrows(
              SolrException.class,
              () -> checker.checkAllowList(List.of("1.1.3.4:8983"), clusterState1));
      assertThat(exception.code(), equalTo(SolrException.ErrorCode.FORBIDDEN.code));
    }
    // Then we verify that the AllowListUrlChecker caches the live host urls and only builds them
    // once.
    assertThat(callCount.get(), equalTo(1));

    // And when the ClusterState live nodes change.
    liveNodes = Set.of("2.3.4.5:8983_solr", "2.3.4.5:9000_", "2.3.4.5:9001_solr-2");
    ClusterState clusterState2 = new ClusterState(liveNodes, new HashMap<>());
    for (int i = 0; i < 3; i++) {
      checker.checkAllowList(
          List.of("2.3.4.5:8983", "2.3.4.5:9000", "2.3.4.5:9001"), clusterState2);
      SolrException exception =
          expectThrows(
              SolrException.class,
              () -> checker.checkAllowList(List.of("1.1.3.4:8983"), clusterState2));
      assertThat(exception.code(), equalTo(SolrException.ErrorCode.FORBIDDEN.code));
    }
    // Then the AllowListUrlChecker rebuilds the cache of live host urls.
    assertThat(callCount.get(), equalTo(2));
  }

  private static List<String> urls(String... urls) {
    return Arrays.asList(urls);
  }

  private static Set<String> hosts(String... hosts) {
    return new HashSet<>(Arrays.asList(hosts));
  }
}
