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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

/** Tests {@link AllowListUrlChecker}. */
public class AllowListUrlCheckerTest extends SolrTestCaseJ4 {

  @Test
  public void testAllowAll() throws Exception {
    AllowListUrlChecker checker = new AllowListUrlChecker(urls("http://cde:8983"));
    SolrException e =
        expectThrows(
            SolrException.class, () -> checker.checkAllowList(urls("abc-1.com:8983/solr")));
    MatcherAssert.assertThat(e.code(), is(SolrException.ErrorCode.FORBIDDEN.code));

    AllowListUrlChecker.ALLOW_ALL.checkAllowList(urls("abc-1.com:8983/solr"));
  }

  @Test
  public void testNoInput() throws Exception {
    MatcherAssert.assertThat(
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
    MatcherAssert.assertThat(e.code(), is(SolrException.ErrorCode.FORBIDDEN.code));
    MatcherAssert.assertThat(e.getMessage(), containsString("http://abc-4.com:8983/solr"));
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
    MatcherAssert.assertThat(e.code(), is(SolrException.ErrorCode.FORBIDDEN.code));
    MatcherAssert.assertThat(e.getMessage(), containsString("abc-4.com:8983/solr"));
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
    MatcherAssert.assertThat(e.getMessage(), containsString("http/abc-1.com:8983"));
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
    MatcherAssert.assertThat(e.getMessage(), containsString("abc-2"));
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
    MatcherAssert.assertThat(
        AllowListUrlChecker.parseHostPorts(Collections.emptyList()).isEmpty(), is(true));
  }

  @Test
  public void testHostParsingSingle() throws Exception {
    MatcherAssert.assertThat(
        AllowListUrlChecker.parseHostPorts(urls("http://abc-1.com:8983/solr/core1")),
        equalTo(hosts("abc-1.com:8983")));
  }

  @Test
  public void testHostParsingMulti() throws Exception {
    MatcherAssert.assertThat(
        AllowListUrlChecker.parseHostPorts(
            urls("http://abc-1.com:8983/solr/core1", "http://abc-1.com:8984/solr")),
        equalTo(hosts("abc-1.com:8983", "abc-1.com:8984")));
  }

  @Test
  public void testHostParsingIpv4() throws Exception {
    MatcherAssert.assertThat(
        AllowListUrlChecker.parseHostPorts(
            urls("http://10.0.0.1:8983/solr/core1", "http://127.0.0.1:8984/solr")),
        equalTo(hosts("10.0.0.1:8983", "127.0.0.1:8984")));
  }

  @Test
  public void testHostParsingIpv6() throws Exception {
    MatcherAssert.assertThat(
        AllowListUrlChecker.parseHostPorts(
            urls(
                "http://[2001:abc:abc:0:0:123:456:1234]:8983/solr/core1",
                "http://[::1]:8984/solr")),
        equalTo(hosts("[2001:abc:abc:0:0:123:456:1234]:8983", "[::1]:8984")));
  }

  @Test
  public void testHostParsingHttps() throws Exception {
    MatcherAssert.assertThat(
        AllowListUrlChecker.parseHostPorts(urls("https://abc-1.com:8983/solr/core1")),
        equalTo(hosts("abc-1.com:8983")));
  }

  @Test
  public void testHostParsingNoProtocol() throws Exception {
    MatcherAssert.assertThat(
        AllowListUrlChecker.parseHostPorts(urls("abc-1.com:8983/solr")),
        equalTo(AllowListUrlChecker.parseHostPorts(urls("http://abc-1.com:8983/solr"))));
    MatcherAssert.assertThat(
        AllowListUrlChecker.parseHostPorts(urls("abc-1.com:8983/solr")),
        equalTo(AllowListUrlChecker.parseHostPorts(urls("https://abc-1.com:8983/solr"))));
  }

  private static List<String> urls(String... urls) {
    return Arrays.asList(urls);
  }

  private static Set<String> hosts(String... hosts) {
    return new HashSet<>(Arrays.asList(hosts));
  }
}
