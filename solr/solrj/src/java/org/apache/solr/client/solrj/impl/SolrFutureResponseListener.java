//
// ========================================================================
// Copyright (c) 1995-2021 Mort Bay Consulting Pty Ltd and others.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License v. 2.0 which is available at
// https://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
// ========================================================================
//

package org.apache.solr.client.solrj.impl;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.eclipse.jetty.client.HttpContentResponse;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.client.util.BufferingResponseListener;

/**
 * A {@link BufferingResponseListener} that is also a {@link Future}, to allow applications
 * to block (indefinitely or for a timeout) until {@link #onComplete(Result)} is called,
 * or to {@link #cancel(boolean) abort} the request/response conversation.
 * <p>
 * Typical usage is:
 * <pre>
 * Request request = httpClient.newRequest(...)...;
 * FutureResponseListener listener = new FutureResponseListener(request);
 * request.send(listener); // Asynchronous send
 * ContentResponse response = listener.get(5, TimeUnit.SECONDS); // Timed block
 * </pre>
 */
public class SolrFutureResponseListener extends SolrBufferingResponseListener implements Future<ContentResponse>
{
    private final CountDownLatch latch = new CountDownLatch(1);
    private final Request request;
    private volatile ContentResponse response;
    private volatile Throwable failure;
    private volatile Throwable responseFailure;
    private volatile boolean cancelled;



    public SolrFutureResponseListener(Request request, MutableDirectBuffer buffer)
    {
        super(buffer);
        this.request = request;
    }

    public Request getRequest()
    {
        return request;
    }

    @Override
    public void onComplete(Result result)
    {
        response = new HttpContentResponse(result.getResponse(), null, getMediaType(), getEncoding());
        failure = result.getFailure();
        responseFailure = result.getResponseFailure();
        latch.countDown();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning)
    {
        cancelled = true;
        return request.abort(new CancellationException());
    }

    @Override
    public boolean isCancelled()
    {
        return cancelled;
    }

    @Override
    public boolean isDone()
    {
        return latch.getCount() == 0 || isCancelled();
    }

    @Override
    public ContentResponse get() throws InterruptedException, ExecutionException
    {
        latch.await();
        return getResult();
    }

    @Override
    public ContentResponse get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
        boolean expired = !latch.await(timeout, unit);
        if (expired)
            throw new TimeoutException();
        return getResult();
    }

    private ContentResponse getResult() throws ExecutionException
    {
        if (isCancelled())
            throw (CancellationException)new CancellationException().initCause(failure);
        if (failure != null)
        {
            // A complete HTTP response can arrive even though the overall exchange is marked failed:
            // when the server fails fast on a streamed request (e.g. an update with a bad field) it
            // sends the error response (status + body) and then RST_STREAM's the still-uploading
            // request body. That surfaces here as a request-side failure (EofException "dropped")
            // while the response side succeeded. Prefer the real response so the caller can decode it
            // into a proper RemoteSolrException instead of an opaque transport reset.
            if (responseFailure == null && response != null && response.getStatus() > 0)
                return response;
            throw new ExecutionException(failure);
        }
        return response;
    }
}
