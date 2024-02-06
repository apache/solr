package org.apache.solr.client.solrj.impl;

import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.embedded.SSLConfig;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.eclipse.jetty.util.HttpCookieStore;

import java.net.CookieStore;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class HttpSolrClientBuilderBase {
    protected Long idleTimeoutMillis;
    protected Long connectionTimeoutMillis;
    protected Long requestTimeoutMillis;
    protected String basicAuthAuthorizationStr;
    protected Boolean followRedirects;
    protected String baseSolrUrl;
    protected RequestWriter requestWriter;
    protected ResponseParser responseParser;
    protected String defaultCollection;
    protected Set<String> urlParamNames;
    protected Long keyStoreReloadIntervalSecs;
    protected SSLConfig sslConfig;;
    protected Integer maxConnectionsPerHost;
    protected ExecutorService executor;
    protected CookieStore cookieStore = HttpSolrClientBuilderBase.getDefaultCookieStore();
    protected String proxyHost;
    protected int proxyPort;
    protected boolean proxyIsSocks4;
    protected boolean proxyIsSecure;

    public HttpSolrClientBuilderBase() {}

    public  HttpSolrClientBuilderBase(String baseSolrUrl) {
        this.baseSolrUrl = baseSolrUrl;
    }

    protected static CookieStore getDefaultCookieStore() {
        if (Boolean.getBoolean("solr.http.disableCookies")) {
            return new HttpCookieStore.Empty();
        }
        /*
         * We could potentially have a Supplier<CookieStore> if we ever need further customization support,
         * but for now it's only either Empty or default (in-memory).
         */
        return null;
    }

    /**
     * Provides a {@link RequestWriter} for created clients to use when handing requests.
     */
    public HttpSolrClientBuilderBase withRequestWriter(RequestWriter requestWriter) {
        this.requestWriter = requestWriter;
        return this;
    }

    /**
     * Provides a {@link ResponseParser} for created clients to use when handling requests.
     */
    public HttpSolrClientBuilderBase withResponseParser(ResponseParser responseParser) {
        this.responseParser = responseParser;
        return this;
    }

    /**
     * Sets a default for core or collection based requests.
     */
    public HttpSolrClientBuilderBase withDefaultCollection(String defaultCoreOrCollection) {
        this.defaultCollection = defaultCoreOrCollection;
        return this;
    }

    public HttpSolrClientBuilderBase withFollowRedirects(boolean followRedirects) {
        this.followRedirects = followRedirects;
        return this;
    }

    public HttpSolrClientBuilderBase withExecutor(ExecutorService executor) {
        this.executor = executor;
        return this;
    }

    public HttpSolrClientBuilderBase withSSLConfig(SSLConfig sslConfig) {
        this.sslConfig = sslConfig;
        return this;
    }

    public HttpSolrClientBuilderBase withBasicAuthCredentials(String user, String pass) {
        if (user != null || pass != null) {
            if (user == null || pass == null) {
                throw new IllegalStateException(
                        "Invalid Authentication credentials. Either both username and password or none must be provided");
            }
        }
        this.basicAuthAuthorizationStr = Http2SolrClient.basicAuthCredentialsToAuthorizationString(user, pass);
        return this;
    }

    /**
     * Expert Method
     *
     * @param urlParamNames set of param keys that are only sent via the query string. Note that the
     *                      param will be sent as a query string if the key is part of this Set or the SolrRequest's
     *                      query params.
     * @see org.apache.solr.client.solrj.SolrRequest#getQueryParams
     */
    public HttpSolrClientBuilderBase withTheseParamNamesInTheUrl(Set<String> urlParamNames) {
        this.urlParamNames = urlParamNames;
        return this;
    }

    /**
     * Set maxConnectionsPerHost for http1 connections, maximum number http2 connections is limited
     * to 4
     */
    public HttpSolrClientBuilderBase withMaxConnectionsPerHost(int max) {
        this.maxConnectionsPerHost = max;
        return this;
    }

    /**
     * Set the scanning interval to check for updates in the Key Store used by this client. If the
     * interval is unset, 0 or less, then the Key Store Scanner is not created, and the client will
     * not attempt to update key stores. The minimum value between checks is 1 second.
     *
     * @param interval Interval between checks
     * @param unit     The unit for the interval
     * @return This builder
     */
    public HttpSolrClientBuilderBase withKeyStoreReloadInterval(long interval, TimeUnit unit) {
        this.keyStoreReloadIntervalSecs = unit.toSeconds(interval);
        if (this.keyStoreReloadIntervalSecs == 0 && interval > 0) {
            this.keyStoreReloadIntervalSecs = 1L;
        }
        return this;
    }
    public HttpSolrClientBuilderBase withIdleTimeout(long idleConnectionTimeout, TimeUnit unit) {
        this.idleTimeoutMillis = TimeUnit.MILLISECONDS.convert(idleConnectionTimeout, unit);
        return this;
    }

    public Long getIdleTimeoutMillis() {
        return idleTimeoutMillis;
    }

    public HttpSolrClientBuilderBase withConnectionTimeout(long connectionTimeout, TimeUnit unit) {
        this.connectionTimeoutMillis = TimeUnit.MILLISECONDS.convert(connectionTimeout, unit);
        return this;
    }

    public Long getConnectionTimeout() {
        return connectionTimeoutMillis;
    }

    /**
     * Set a timeout in milliseconds for requests issued by this client.
     *
     * @param requestTimeout The timeout in milliseconds
     * @return this Builder.
     */
    public HttpSolrClientBuilderBase withRequestTimeout(long requestTimeout, TimeUnit unit) {
        this.requestTimeoutMillis = TimeUnit.MILLISECONDS.convert(requestTimeout, unit);
        return this;
    }

    /**
     * Set a cookieStore other than the default ({@code java.net.InMemoryCookieStore})
     *
     * @param cookieStore The CookieStore to set. {@code null} will set the default.
     * @return this Builder
     */
    public HttpSolrClientBuilderBase withCookieStore(CookieStore cookieStore) {
        this.cookieStore = cookieStore;
        return this;
    }

    /**
     * Setup a proxy
     *
     * @param host     The proxy host
     * @param port     The proxy port
     * @param isSocks4 If true creates an SOCKS 4 proxy, otherwise creates an HTTP proxy
     * @param isSecure If true enables the secure flag on the proxy
     * @return this Builder
     */
    public HttpSolrClientBuilderBase withProxyConfiguration(
            String host, int port, boolean isSocks4, boolean isSecure) {
        this.proxyHost = host;
        this.proxyPort = port;
        this.proxyIsSocks4 = isSocks4;
        this.proxyIsSecure = isSecure;
        return this;
    }

    /**
     * Setup basic authentication from a string formatted as username:password. If the string is
     * Null then it doesn't do anything.
     *
     * @param credentials The username and password formatted as username:password
     * @return this Builder
     */
    public HttpSolrClientBuilderBase withOptionalBasicAuthCredentials(String credentials) {
        if (credentials != null) {
            if (credentials.indexOf(':') == -1) {
                throw new IllegalStateException(
                        "Invalid Authentication credential formatting. Provide username and password in the 'username:password' format.");
            }
            String username = credentials.substring(0, credentials.indexOf(':'));
            String password = credentials.substring(credentials.indexOf(':') + 1, credentials.length());
            withBasicAuthCredentials(username, password);
        }
        return this;
    }
}
