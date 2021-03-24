package org.apache.solr.security;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.Test;

import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;

/**
 * Tests {@link AllowListUrlChecker}.
 */
public class AllowListUrlCheckerTest extends SolrTestCaseJ4 {

    @Test
    public void testAllowAll() throws Exception {
        AllowListUrlChecker checker = new AllowListUrlChecker("http://cde:8983");
        SolrException e = expectThrows(SolrException.class, () -> checker.checkAllowList(urls("abc-1.com:8983/solr")));
        assertThat(e.code(), is(SolrException.ErrorCode.FORBIDDEN.code));

        AllowListUrlChecker.ALLOW_ALL.checkAllowList(urls("abc-1.com:8983/solr"));
    }

    @Test
    public void testNoInput() throws Exception {
        assertNull(new AllowListUrlChecker(null).getHostAllowList());
        assertNull(new AllowListUrlChecker("").getHostAllowList());
    }

    @Test
    public void testSingleHost() throws Exception {
        AllowListUrlChecker checker = new AllowListUrlChecker("http://abc-1.com:8983/solr");
        checker.checkAllowList(urls("http://abc-1.com:8983/solr"));
    }

    @Test
    public void testMultipleHostsInConstructor() throws Exception {
        AllowListUrlChecker checker = new AllowListUrlChecker("http://abc-1.com:8983, http://abc-2.com:8983,http://abc-3.com:8983,");
        checker.checkAllowList(urls("http://abc-1.com:8983/solr"));
        checker.checkAllowList(urls("http://abc-2.com:8983/solr"));
        checker.checkAllowList(urls("http://abc-3.com:8983/solr"));
    }

    @Test
    public void testMultipleHostsInCheck() throws Exception {
        AllowListUrlChecker checker = new AllowListUrlChecker("http://abc-1.com:8983, http://abc-2.com:8983, http://abc-3.com:8983");
        checker.checkAllowList(urls("http://abc-3.com:8983/solr", "http://abc-1.com:8983/solr", "http://abc-2.com:8983/solr"));
    }

    @Test
    public void testNoProtocol() throws Exception {
        AllowListUrlChecker checker = new AllowListUrlChecker("http://abc-1.com:8983, http://abc-2.com:8983, abc-3.com:8983");
        checker.checkAllowList(urls("abc-1.com:8983/solr", "abc-3.com:8983/solr"));
    }

    @Test
    public void testDisallowedHost() throws Exception {
        AllowListUrlChecker checker = new AllowListUrlChecker("http://abc-1.com:8983, http://abc-2.com:8983, http://abc-3.com:8983");
        SolrException e = expectThrows(SolrException.class, () -> checker.checkAllowList(urls("http://abc-4.com:8983/solr")));
        assertThat(e.code(), is(SolrException.ErrorCode.FORBIDDEN.code));
        assertThat(e.getMessage(), containsString("http://abc-4.com:8983/solr"));
    }

    @Test
    public void testDisallowedHostNoProtocol() throws Exception {
        AllowListUrlChecker checker = new AllowListUrlChecker("http://abc-1.com:8983, http://abc-2.com:8983, http://abc-3.com:8983");
        SolrException e = expectThrows(SolrException.class, () -> checker.checkAllowList(urls("abc-1.com:8983/solr", "abc-4.com:8983/solr")));
        assertThat(e.code(), is(SolrException.ErrorCode.FORBIDDEN.code));
        assertThat(e.getMessage(), containsString("abc-4.com:8983/solr"));

    }

    @Test
    public void testProtocolHttps() throws Exception {
        AllowListUrlChecker checker = new AllowListUrlChecker("http://abc-1.com:8983, http://abc-2.com:8983, http://abc-3.com:8983");
        checker.checkAllowList(urls("https://abc-1.com:8983/solr", "https://abc-2.com:8983/solr"));
    }

    @Test
    public void testInvalidUrlInConstructor() {
        MalformedURLException e = expectThrows(MalformedURLException.class, () -> new AllowListUrlChecker("h://abc-1.com:8983"));
        assertThat(e.getMessage(), containsString("h://abc-1.com:8983"));
    }

    @Test
    public void testInvalidUrlInCheck() throws Exception {
        AllowListUrlChecker checker = new AllowListUrlChecker("http://abc-1.com:8983, http://abc-2.com:8983, http://abc-3.com:8983");
        MalformedURLException e = expectThrows(MalformedURLException.class, () -> checker.checkAllowList(urls("http://abc-1.com:8983", "abc-2")));
        assertThat(e.getMessage(), containsString("abc-2"));
    }

    @Test
    public void testCoreSpecific() throws Exception {
        // Cores are removed completely so it doesn't really matter if they were set in config.
        AllowListUrlChecker checker = new AllowListUrlChecker("http://abc-1.com:8983/solr/core1, http://abc-2.com:8983/solr2/core2");
        checker.checkAllowList(urls("abc-1.com:8983/solr/core3", "http://abc-2.com:8983/solr"));
    }

    @Test
    public void testHostParsingUnsetEmpty() throws Exception {
        assertThat(AllowListUrlChecker.parseHostPorts(null), nullValue());
        assertThat(AllowListUrlChecker.parseHostPorts(""), nullValue());
    }

    @Test
    public void testHostParsingSingle() throws Exception {
        assertThat(AllowListUrlChecker.parseHostPorts("http://abc-1.com:8983/solr/core1"),
                equalTo(hosts("abc-1.com:8983")));
    }

    @Test
    public void testHostParsingMulti() throws Exception {
        assertThat(AllowListUrlChecker.parseHostPorts("http://abc-1.com:8983/solr/core1,http://abc-1.com:8984/solr"),
        equalTo(hosts("abc-1.com:8983", "abc-1.com:8984")));
    }

    @Test
    public void testHostParsingIpv4() throws Exception {
        assertThat(AllowListUrlChecker.parseHostPorts("http://10.0.0.1:8983/solr/core1,http://127.0.0.1:8984/solr"),
                equalTo(hosts("10.0.0.1:8983", "127.0.0.1:8984")));
    }

    @Test
    public void testHostParsingIpv6() throws Exception {
        assertThat(AllowListUrlChecker.parseHostPorts("http://[2001:abc:abc:0:0:123:456:1234]:8983/solr/core1,http://[::1]:8984/solr"),
                equalTo(hosts("[2001:abc:abc:0:0:123:456:1234]:8983", "[::1]:8984")));
    }

    @Test
    public void testHostParsingHttps() throws Exception {
        assertThat(AllowListUrlChecker.parseHostPorts("https://abc-1.com:8983/solr/core1"),
                equalTo(hosts("abc-1.com:8983")));
    }

    @Test
    public void testHostParsingNoProtocol() throws Exception {
        assertThat(AllowListUrlChecker.parseHostPorts("abc-1.com:8983/solr"),
                equalTo(AllowListUrlChecker.parseHostPorts("http://abc-1.com:8983/solr")));
        assertThat(AllowListUrlChecker.parseHostPorts("abc-1.com:8983/solr"),
                equalTo(AllowListUrlChecker.parseHostPorts("https://abc-1.com:8983/solr")));
    }

    private static List<String> urls(String... urls) {
        return Arrays.asList(urls);
    }

    private static Set<String> hosts(String... hosts) {
        return new HashSet<>(Arrays.asList(hosts));
    }
}
