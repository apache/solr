package org.apache.solr.client.solrj.impl;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.V2Request;

import java.net.MalformedURLException;
import java.net.URL;

public abstract class Http2SolrClientBase extends SolrClient {

    protected static final String DEFAULT_PATH = "/select";

    /** The URL of the Solr server. */
    protected final String serverBaseUrl;

    protected RequestWriter requestWriter = new BinaryRequestWriter();

    protected Http2SolrClientBase(String serverBaseUrl, HttpSolrClientBuilderBase builder) {
        if (serverBaseUrl != null) {
            if (!serverBaseUrl.equals("/") && serverBaseUrl.endsWith("/")) {
                serverBaseUrl = serverBaseUrl.substring(0, serverBaseUrl.length() - 1);
            }

            if (serverBaseUrl.startsWith("//")) {
                serverBaseUrl = serverBaseUrl.substring(1, serverBaseUrl.length());
            }
            this.serverBaseUrl = serverBaseUrl;
        } else {
            this.serverBaseUrl = null;
        }
    }

    protected String getRequestPath(SolrRequest<?> solrRequest, String collection)
            throws MalformedURLException {
        String basePath = solrRequest.getBasePath() == null ? serverBaseUrl : solrRequest.getBasePath();
        if (collection != null) basePath += "/" + collection;

        if (solrRequest instanceof V2Request) {
            if (System.getProperty("solr.v2RealPath") == null) {
                basePath = changeV2RequestEndpoint(basePath);
            } else {
                basePath = serverBaseUrl + "/____v2";
            }
        }
        String path = requestWriter.getPath(solrRequest);
        if (path == null || !path.startsWith("/")) {
            path = DEFAULT_PATH;
        }

        return basePath + path;
    }

    protected String changeV2RequestEndpoint(String basePath) throws MalformedURLException {
        URL oldURL = new URL(basePath);
        String newPath = oldURL.getPath().replaceFirst("/solr", "/api");
        return new URL(oldURL.getProtocol(), oldURL.getHost(), oldURL.getPort(), newPath).toString();
    }
}
