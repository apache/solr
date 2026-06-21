package org.apache.solr.client.solrj.impl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.api.Response.Listener;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.util.BufferUtil;

public abstract class SolrBufferingResponseListener extends Listener.Adapter {

    private MutableDirectBuffer buffer;
    private String mediaType;
    private String encoding;
    private AtomicInteger pos = new AtomicInteger();


    public SolrBufferingResponseListener(MutableDirectBuffer buffer) {

        this.buffer = buffer;
    }

    @Override
    public void onHeaders(Response response) {
        super.onHeaders(response);

        Request request = response.getRequest();
        HttpFields headers = response.getHeaders();
        long length = headers.getLongField(HttpHeader.CONTENT_LENGTH);
        if (HttpMethod.HEAD.is(request.getMethod()))
            length = 0;


        String contentType = headers.get(HttpHeader.CONTENT_TYPE);
        if (contentType != null) {
            String media = contentType;

            String charset = "charset=";
            int index = contentType.toLowerCase(Locale.ENGLISH).indexOf(charset);
            if (index > 0) {
                media = contentType.substring(0, index);
                String encoding = contentType.substring(index + charset.length());
                // Sometimes charsets arrive with an ending semicolon.
                int semicolon = encoding.indexOf(';');
                if (semicolon > 0)
                    encoding = encoding.substring(0, semicolon).trim();
                // Sometimes charsets are quoted.
                int lastIndex = encoding.length() - 1;
                if (encoding.charAt(0) == '"' && encoding.charAt(lastIndex) == '"')
                    encoding = encoding.substring(1, lastIndex).trim();
                this.encoding = encoding;
            }

            int semicolon = media.indexOf(';');
            if (semicolon > 0)
                media = media.substring(0, semicolon).trim();
            this.mediaType = media;
        }
    }

    @Override
    public void onContent(Response response, ByteBuffer content) {


        int len = content.remaining();
        buffer.putBytes(pos.getAndUpdate(operand -> operand + len), content, len);
    }

    @Override
    public abstract void onComplete(Result result);

    public String getMediaType() {
        return mediaType;
    }

    public String getEncoding() {
        return encoding;
    }


    public MutableDirectBuffer getContent() {
        if (buffer == null)
            return buffer;
        buffer.byteBuffer().position(0 + buffer.wrapAdjustment());
        buffer.byteBuffer().limit(pos.get() + buffer.wrapAdjustment());
        return buffer;
    }

//    public InputStream getContentAsInputStream() {
//        if (buffer == null)
//            return new ByteArrayInputStream(new byte[0]);
//        return new ByteArrayInputStream(buffer.array(), buffer.arrayOffset(), buffer.remaining());
//    }
}
