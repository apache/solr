package org.apache.solr.client.solrj.impl;

import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.eclipse.jetty.http.MimeTypes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.solr.common.util.Utils.getObjectByPath;

public abstract class Http2SolrClientBase extends SolrClient {

    protected static final String DEFAULT_PATH = "/select";
    protected static final Charset FALLBACK_CHARSET = StandardCharsets.UTF_8;
    private static final List<String> errPath = Arrays.asList("metadata", "error-class");

    /** The URL of the Solr server. */
    protected final String serverBaseUrl;

    protected RequestWriter requestWriter = new BinaryRequestWriter();

    // updating parser instance needs to go via the setter to ensure update of defaultParserMimeTypes
    protected ResponseParser parser = new BinaryResponseParser();

    protected Set<String> defaultParserMimeTypes;

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

    protected abstract boolean isFollowRedirects() ;

    @SuppressWarnings({"unchecked", "rawtypes"})
    protected NamedList<Object> processErrorsAndResponse(
            int httpStatus,
            String responseReason,
            String responseMethod,
            final ResponseParser processor,
            InputStream is,
            String mimeType,
            String encoding,
            final boolean isV2Api,
            String urlExceptionMessage)
            throws SolrServerException {
        boolean shouldClose = true;
        try {
            // handle some http level checks before trying to parse the response
            switch (httpStatus) {
                case 200: //OK
                case 400: //Bad Request
                case 409: //Conflict
                    break;
                case 301: //Moved Permanently
                case 302: //Moved Temporarily
                    if (!isFollowRedirects()) {
                        throw new SolrServerException(
                                "Server at " + urlExceptionMessage + " sent back a redirect (" + httpStatus + ").");
                    }
                    break;
                default:
                    if (processor == null || mimeType == null) {
                        throw new BaseHttpSolrClient.RemoteSolrException(
                                urlExceptionMessage,
                                httpStatus,
                                "non ok status: " + httpStatus + ", message:" + responseReason,
                                null);
                    }
            }

            if (wantStream(processor)) {
                // no processor specified, return raw stream
                NamedList<Object> rsp = new NamedList<>();
                rsp.add("stream", is);
                rsp.add("responseStatus", httpStatus);
                // Only case where stream should not be closed
                shouldClose = false;
                return rsp;
            }

            checkContentType(processor, is, mimeType, encoding, httpStatus, urlExceptionMessage);

            NamedList<Object> rsp;
            try {
                rsp = processor.processResponse(is, encoding);
            } catch (Exception e) {
                throw new BaseHttpSolrClient.RemoteSolrException(urlExceptionMessage, httpStatus, e.getMessage(), e);
            }

            Object error = rsp == null ? null : rsp.get("error");
            if (rsp != null && error == null && processor instanceof NoOpResponseParser) {
                error = rsp.get("response");
            }
            if (error != null
                    && (String.valueOf(getObjectByPath(error, true, errPath))
                    .endsWith("ExceptionWithErrObject"))) {
                throw BaseHttpSolrClient.RemoteExecutionException.create(urlExceptionMessage, rsp);
            }
            if (httpStatus != 200 && !isV2Api) {
                NamedList<String> metadata = null;
                String reason = null;
                try {
                    if (error != null) {
                        reason = (String) Utils.getObjectByPath(error, false, Collections.singletonList("msg"));
                        if (reason == null) {
                            reason =
                                    (String) Utils.getObjectByPath(error, false, Collections.singletonList("trace"));
                        }
                        Object metadataObj =
                                Utils.getObjectByPath(error, false, Collections.singletonList("metadata"));
                        if (metadataObj instanceof NamedList) {
                            metadata = (NamedList<String>) metadataObj;
                        } else if (metadataObj instanceof List) {
                            // NamedList parsed as List convert to NamedList again
                            List<Object> list = (List<Object>) metadataObj;
                            metadata = new NamedList<>(list.size() / 2);
                            for (int i = 0; i < list.size(); i += 2) {
                                metadata.add((String) list.get(i), (String) list.get(i + 1));
                            }
                        } else if (metadataObj instanceof Map) {
                            metadata = new NamedList((Map) metadataObj);
                        }
                    }
                } catch (Exception ex) {
                    /* Ignored */
                }
                if (reason == null) {
                    StringBuilder msg = new StringBuilder();
                    msg.append(responseReason)
                            .append("\n")
                            .append("request: ")
                            .append(responseMethod);
                    if (error != null) {
                        msg.append("\n\nError returned:\n").append(error);
                    }
                    reason = java.net.URLDecoder.decode(msg.toString(), FALLBACK_CHARSET);
                }
                BaseHttpSolrClient.RemoteSolrException rss =
                        new BaseHttpSolrClient.RemoteSolrException(urlExceptionMessage, httpStatus, reason, null);
                if (metadata != null) rss.setMetadata(metadata);
                throw rss;
            }
            return rsp;
        } finally {
            if (shouldClose) {
                try {
                    is.close();
                } catch (IOException e) {
                    // quitely
                }
            }
        }
    }

    protected boolean wantStream(final ResponseParser processor) {
        return processor == null || processor instanceof InputStreamResponseParser;
    }

    protected abstract boolean processorAcceptsMimeType(Collection<String> processorSupportedContentTypes, String mimeType) ;

    protected abstract String allProcessorSupportedContentTypesCommaDelimited(Collection<String> processorSupportedContentTypes);

    /**
     * Validates that the content type in the response can be processed by the Response Parser. Throws
     * a {@code RemoteSolrException} if not.
     */
    private void checkContentType(
            ResponseParser processor,
            InputStream is,
            String mimeType,
            String encoding,
            int httpStatus,
            String urlExceptionMessage) {
        if (mimeType == null
                || (processor == this.parser && defaultParserMimeTypes.contains(mimeType))) {
            // Shortcut the default scenario
            return;
        }
        final Collection<String> processorSupportedContentTypes = processor.getContentTypes();
        if (processorSupportedContentTypes != null && !processorSupportedContentTypes.isEmpty()) {
            boolean processorAcceptsMimeType = processorAcceptsMimeType(processorSupportedContentTypes, mimeType);
            if (!processorAcceptsMimeType) {
                // unexpected mime type
                final String allSupportedTypes = allProcessorSupportedContentTypesCommaDelimited(processorSupportedContentTypes);
                String prefix =
                        "Expected mime type in [" + allSupportedTypes + "] but got " + mimeType + ". ";
                String exceptionEncoding = encoding != null ? encoding : FALLBACK_CHARSET.name();
                try {
                    ByteArrayOutputStream body = new ByteArrayOutputStream();
                    is.transferTo(body);
                    throw new BaseHttpSolrClient.RemoteSolrException(
                            urlExceptionMessage, httpStatus, prefix + body.toString(exceptionEncoding), null);
                } catch (IOException e) {
                    throw new BaseHttpSolrClient.RemoteSolrException(
                            urlExceptionMessage,
                            httpStatus,
                            "Could not parse response with encoding " + exceptionEncoding,
                            e);
                }
            }
        }
    }

    public ResponseParser getParser() {
        return parser;
    }

    protected void setParser(ResponseParser parser) {
        this.parser = parser;
        updateDefaultMimeTypeForParser();
    }

    protected void updateDefaultMimeTypeForParser() {
        defaultParserMimeTypes =
                parser.getContentTypes().stream()
                        .map(ct -> MimeTypes.getContentTypeWithoutCharset(ct).trim().toLowerCase(Locale.ROOT))
                        .collect(Collectors.toSet());
    }
}
