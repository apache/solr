package org.apache.solr.client.solrj.impl;

import org.agrona.ExpandableDirectByteBuffer;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpVersion;

import java.util.List;

public class HttpByeBufferContentResponse implements Response
{
    private final Response response;
    private final ExpandableDirectByteBuffer content;
    private final String mediaType;
    private final String encoding;

    public HttpByeBufferContentResponse(Response response, ExpandableDirectByteBuffer content, String mediaType, String encoding)
    {
        this.response = response;
        this.content = content;
        this.mediaType = mediaType;
        this.encoding = encoding;
    }

    @Override
    public Request getRequest()
    {
        return response.getRequest();
    }

    @Override
    public <T extends ResponseListener> List<T> getListeners(Class<T> listenerClass)
    {
        return response.getListeners(listenerClass);
    }

    @Override
    public HttpVersion getVersion()
    {
        return response.getVersion();
    }

    @Override
    public int getStatus()
    {
        return response.getStatus();
    }

    @Override
    public String getReason()
    {
        return response.getReason();
    }

    @Override
    public HttpFields getHeaders()
    {
        return response.getHeaders();
    }

    @Override
    public boolean abort(Throwable cause)
    {
        return response.abort(cause);
    }


    public String getMediaType()
    {
        return mediaType;
    }


    public String getEncoding()
    {
        return encoding;
    }


    public ExpandableDirectByteBuffer getContent()
    {
        return content;
    }

    @Override
    public String toString()
    {
        return String.format("%s[%s %d %s - %d bytes]",
            HttpByeBufferContentResponse.class.getSimpleName(),
            getVersion(),
            getStatus(),
            getReason(),
            getContent().byteBuffer().limit());
    }
}
