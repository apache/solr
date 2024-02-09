package org.apache.solr.client.solrj.impl;

import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.CookieStore;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HttpSolrClientJdkImpl extends Http2SolrClientBase {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final String USER_AGENT = "Solr[" + MethodHandles.lookup().lookupClass().getName() + "] 1.0";

    private HttpClient client;

    private ExecutorService executor;

    protected HttpSolrClientJdkImpl(String serverBaseUrl, HttpSolrClientBuilderBase builder) {
        super(serverBaseUrl, builder);

        HttpClient.Redirect followRedirects = Boolean.TRUE.equals(builder.followRedirects) ? HttpClient.Redirect.NORMAL : HttpClient.Redirect.NEVER;
        this.executor = Executors.newCachedThreadPool(Executors.defaultThreadFactory());
        this.client = HttpClient.newBuilder().executor(executor).followRedirects(followRedirects).build();

        updateDefaultMimeTypeForParser();

        assert ObjectReleaseTracker.track(this);
    }

    private HttpRequest.BodyPublisher preparePostPutRequest(HttpRequest.Builder reqb, SolrRequest<?> solrRequest, ModifiableSolrParams requestParams) throws IOException {
        RequestWriter.ContentWriter contentWriter = requestWriter.getContentWriter(solrRequest);

        Collection<ContentStream> streams = null;
        if(contentWriter == null) {
            streams = requestWriter.getContentStreams(solrRequest);
        }

        String contentType = "application/x-www-form-urlencoded";
        if(contentWriter != null && contentWriter.getContentType() != null) {
            contentType = contentWriter.getContentType();
        }
        reqb.header("Content-Type", contentType);

        if(isMultipart(streams)) {
            throw new UnsupportedOperationException("This client does not support multipart.");
        }

        if (contentWriter != null) {
            //TODO:  There is likely a more memory-efficient way to do this!
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            contentWriter.write(baos);
            byte[] bytes = baos.toByteArray();
            return HttpRequest.BodyPublishers.ofByteArray(bytes);
        } else if (streams != null && streams.size() == 1) {
            ContentStream contentStream = streams.iterator().next();
            InputStream is = contentStream.getStream();
            return HttpRequest.BodyPublishers.ofInputStream(() -> is);
        } else if(requestParams != null) {
            return HttpRequest.BodyPublishers.ofString(requestParams.toString());
        } else {
            return HttpRequest.BodyPublishers.noBody();
        }
    }

    @Override
    public NamedList<Object> request(SolrRequest<?> solrRequest, String collection) throws SolrServerException, IOException {
        checkClosed();
        if (ClientUtils.shouldApplyDefaultCollection(collection, solrRequest)) {
            collection = defaultCollection;
        }
        String url = getRequestPath(solrRequest, collection);
        ResponseParser parser = responseParser(solrRequest);
        ModifiableSolrParams queryParams = initalizeSolrParams(solrRequest);
        ModifiableSolrParams requestParams = null;
        if (urlParamNames != null && !urlParamNames.isEmpty()) {
            requestParams = queryParams;
            queryParams = calculateQueryParams(urlParamNames, requestParams);
            queryParams.add(calculateQueryParams(solrRequest.getQueryParams(), requestParams));
        }
        Throwable abortCause = null;
        try {
            var reqb = HttpRequest.newBuilder();
            switch(solrRequest.getMethod()) {
                case GET: {
                    validateGetRequest(solrRequest);
                    reqb.GET();
                    break;
                }
                case POST: {
                    reqb.POST(preparePostPutRequest(reqb, solrRequest, requestParams));
                    break;
                }
                case PUT: {
                    reqb.PUT(preparePostPutRequest(reqb, solrRequest, requestParams));
                    break;
                }
                case DELETE: {
                    //TODO:  Delete requests are sent as POST, so should we support method=DELETE??
                    reqb.DELETE();
                    break;
                }
                default: {
                    throw new IllegalStateException("Unsupported method: " + solrRequest.getMethod());
                }
            }
            decorateRequest(reqb, solrRequest);
            reqb.uri(new URI(url + "?" + queryParams));
            HttpResponse<InputStream> resp = client.send(reqb.build(), HttpResponse.BodyHandlers.ofInputStream());
            return processErrorsAndResponse(solrRequest, resp, url);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            abortCause = e;
            throw new RuntimeException(e);
        } catch (HttpTimeoutException e) {
            throw new SolrServerException(
                    "Timeout occurred while waiting response from server at: " + url, e);
        }  /*TODO catch (ExecutionException e) {
            Throwable cause = e.getCause();
            abortCause = cause;
            if (cause instanceof ConnectException) {
                throw new SolrServerException("Server refused connection at: " + url, cause);
            }
            if (cause instanceof SolrServerException) {
                throw (SolrServerException) cause;
            } else if (cause instanceof IOException) {
                throw new SolrServerException(
                        "IOException occurred when talking to server at: " + url, cause);
            }
            throw new SolrServerException(cause.getMessage(), cause);
        } */ catch (SolrException se) {
            abortCause =se;
            throw se;
        } catch (URISyntaxException | RuntimeException re) {
            abortCause = re;
            throw new SolrServerException(re);
        } finally {
            if (abortCause != null /* && req != null*/) {
                //TODO
            }
        }
    }

    private void setBasicAuthHeader(SolrRequest<?> solrRequest, HttpRequest.Builder reqb) {
        if (solrRequest.getBasicAuthUser() != null && solrRequest.getBasicAuthPassword() != null) {
            String encoded =
                    basicAuthCredentialsToAuthorizationString(
                            solrRequest.getBasicAuthUser(), solrRequest.getBasicAuthPassword());
            reqb.header("Authorization", encoded);
        } else if (basicAuthAuthorizationStr != null) {
            reqb.header("Authorization", basicAuthAuthorizationStr);
        }
    }


    private void decorateRequest(HttpRequest.Builder reqb, SolrRequest<?> solrRequest) {
        if (requestTimeoutMillis > 0) {
            reqb.timeout(Duration.of(requestTimeoutMillis, ChronoUnit.MILLIS));
        } else {
            reqb.timeout(Duration.of(idleTimeoutMillis, ChronoUnit.MILLIS));
        }
        reqb.header("User-Agent", USER_AGENT);
        setBasicAuthHeader(solrRequest, reqb);
        Map<String, String> headers = solrRequest.getHeaders();
        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                reqb.header(entry.getKey(), entry.getValue());
            }
        }
    }
//Content-Type: text/html; charset=utf-8
//Content-Type: multipart/form-data; boundary=something

    private static final Pattern MIME_TYPE_PATTERN = Pattern.compile("^(.*) .*$");
    private static final Pattern CHARSET_PATTERN = Pattern.compile("(?i)^.*charset=(.*)?(?:;| |$)");

    private NamedList<Object> processErrorsAndResponse(SolrRequest<?> solrRequest, HttpResponse<InputStream> resp, String url)  throws SolrServerException {
        ResponseParser parser =
                solrRequest.getResponseParser() == null ? this.parser : solrRequest.getResponseParser();
        String contentType = resp.headers().firstValue("Content-Type").orElse(null);
        contentType = contentType == null ? "" : contentType;
        Matcher mimeTypeMatcher = MIME_TYPE_PATTERN.matcher(contentType);
        Matcher encodingMatcher = CHARSET_PATTERN.matcher(contentType);
        String mimeType = mimeTypeMatcher.find() ? mimeTypeMatcher.group(1) : null;
        String encoding = encodingMatcher.find() ? encodingMatcher.group(1) : null;
        String method = resp.request() == null ? null : resp.request().method();
        InputStream is = resp.body();
        int status = resp.statusCode();
        String reason = statusToReasonPhrase(status);
        return processErrorsAndResponse(
                status, reason, method, parser, is, mimeType, encoding, isV2ApiRequest(solrRequest), url);

    }

    @Override
    public void close() throws IOException {
        // TODO: Java 21 adds close/autoclosable to HttpClient.
        // Once we require Java 21, we should use it.  Perhaps
        // also we can use the default executor.

        try {
            //TODO: 60 seconds?
            executor.awaitTermination(60, TimeUnit.MILLISECONDS);
            executor.shutdown();
        } catch (InterruptedException ie) {
           executor.shutdownNow();
           Thread.currentThread().interrupt();
        }
        executor = null;
        client = null;
    }

    private void checkClosed() {
        if(client==null) {
            throw new IllegalStateException("This is closed and cannot be reused.");
        }
    }

    @Override
    protected boolean isFollowRedirects() {
        return client.followRedirects() != HttpClient.Redirect.NEVER;
    }

    @Override
    protected boolean processorAcceptsMimeType(Collection<String> processorSupportedContentTypes, String mimeType) {
        return false;
    }

    @Override
    protected String allProcessorSupportedContentTypesCommaDelimited(Collection<String> processorSupportedContentTypes) {
        return null;
    }

    /**
     * Taken from https://stackoverflow.com/questions/63540068/is-there-a-way-to-fetch-the-reason-phrase-from-the-status-line-of-a-http-1-1-res
     * and pruned to only include the more-common phrases.  If not one of these the code is returned as a String.
     * @param statusCode
     * @return the phrase
     */
    private String statusToReasonPhrase(int statusCode) {
        switch(statusCode) {
            case (200): return "OK";
            case (204): return "No Content";
            case (301): return "Moved Permanently";
            case (302): return "Moved Temporarily";
            case (304): return "Not Modified";
            case (400): return "Bad Request";
            case (401): return "Unauthorized";
            case (403): return "Forbidden";
            case (404): return "Not Found";
            case (405): return "Method Not Allowed";
            case (413): return "Request Entity Too Large";
            case (414): return "Request-URI Too Long";
            case (415): return "Unsupported Media Type";
            case (422): return "Unprocessable Entity";
            case (500): return "Server Error";
            case (503): return "Service Unavailable";
            default: return "" + statusCode;
        }
    }


    public static class Builder extends HttpSolrClientBuilderBase {

        public Builder() {
            super();
        }
        public  Builder(String baseSolrUrl) {
           super(baseSolrUrl);
        }

        public <B extends Http2SolrClientBase> B build(Class<B> type) {
            return type.cast(build());
        }
        public HttpSolrClientJdkImpl build() {
            if (idleTimeoutMillis == null || idleTimeoutMillis <= 0) {
                idleTimeoutMillis = (long) HttpClientUtil.DEFAULT_SO_TIMEOUT;
            }
            if (connectionTimeoutMillis == null) {
                connectionTimeoutMillis = (long) HttpClientUtil.DEFAULT_CONNECT_TIMEOUT;
            }
            //TODO: if really not supported, move to Htt2SolrCLient
            if (keyStoreReloadIntervalSecs != null && keyStoreReloadIntervalSecs > 0) {
                log.warn("keyStoreReloadIntervalSecs not supported by this client.");
            }
            return new HttpSolrClientJdkImpl(baseSolrUrl, this);
        }

        /**
         * {@inheritDoc}
         */
        public HttpSolrClientJdkImpl.Builder withRequestWriter(RequestWriter requestWriter) {
            super.withRequestWriter(requestWriter);
            return this;
        }

        /**
         * {@inheritDoc}
         */
        public HttpSolrClientJdkImpl.Builder withResponseParser(ResponseParser responseParser) {
            super.withResponseParser(responseParser);
            return this;
        }

        /**
         * {@inheritDoc}
         */
        public HttpSolrClientJdkImpl.Builder withDefaultCollection(String defaultCoreOrCollection) {
            super.withDefaultCollection(defaultCoreOrCollection);
            return this;
        }
        /**
         * {@inheritDoc}
         */
        @Override
        public HttpSolrClientJdkImpl.Builder withFollowRedirects(boolean followRedirects) {
            super.withFollowRedirects(followRedirects);
            return this;
        }
        /**
         * {@inheritDoc}
         */
        @Override
        public HttpSolrClientJdkImpl.Builder withExecutor(ExecutorService executor) {
            super.withExecutor(executor);
            return this;
        }

        public HttpSolrClientJdkImpl.Builder withBasicAuthCredentials(String user, String pass) {
            super.withBasicAuthCredentials(user, pass);
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public HttpSolrClientJdkImpl.Builder withTheseParamNamesInTheUrl(Set<String> urlParamNames) {
            super.withTheseParamNamesInTheUrl(urlParamNames);
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public HttpSolrClientJdkImpl.Builder withMaxConnectionsPerHost(int max) {
            super.withMaxConnectionsPerHost(max);
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public HttpSolrClientJdkImpl.Builder withKeyStoreReloadInterval(long interval, TimeUnit unit) {
            super.withKeyStoreReloadInterval(interval, unit);
            return this;
        }

        /**
         * @deprecated Please use {@link #withIdleTimeout(long, TimeUnit)}
         */
        @Deprecated(since = "9.2")
        public HttpSolrClientJdkImpl.Builder idleTimeout(int idleConnectionTimeout) {
            withIdleTimeout(idleConnectionTimeout, TimeUnit.MILLISECONDS);
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public HttpSolrClientJdkImpl.Builder withIdleTimeout(long idleConnectionTimeout, TimeUnit unit) {
            super.withIdleTimeout(idleConnectionTimeout, unit);
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public HttpSolrClientJdkImpl.Builder withConnectionTimeout(long connectionTimeout, TimeUnit unit) {
            super.withConnectionTimeout(connectionTimeout, unit);
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public HttpSolrClientJdkImpl.Builder withRequestTimeout(long requestTimeout, TimeUnit unit) {
            super.withRequestTimeout(requestTimeout, unit);
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public HttpSolrClientJdkImpl.Builder withCookieStore(CookieStore cookieStore) {
            super.withCookieStore(cookieStore);
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public HttpSolrClientJdkImpl.Builder withProxyConfiguration(
                String host, int port, boolean isSocks4, boolean isSecure) {
            super.withProxyConfiguration(host, port, isSocks4, isSecure);
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public HttpSolrClientJdkImpl.Builder withOptionalBasicAuthCredentials(String credentials) {
            super.withOptionalBasicAuthCredentials(credentials);
            return this;
        }
    }
}
