package org.apache.solr.util.stats;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricsContext;

import io.opentelemetry.api.common.Attributes;

/**
 * OTEL instrumentation wrapper around {@link ExecutorService}.
 * Based on {@link com.codahale.metrics.InstrumentedExecutorService}.
 */
public class OtelInstrumentedExecutorService implements ExecutorService, SolrInfoBean {
    private final ExecutorService delegate;
    private final SolrMetricsContext ctx;
    private final String name;

    public OtelInstrumentedExecutorService(ExecutorService delegate, SolrMetricsContext ctx, String name) {
        this.delegate = delegate;
        this.ctx = ctx;
        this.name = name;
    }

    @Override
    public void execute(Runnable command) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'execute'");
    }

    @Override
    public void shutdown() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'shutdown'");
    }

    @Override
    public List<Runnable> shutdownNow() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'shutdownNow'");
    }

    @Override
    public boolean isShutdown() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isShutdown'");
    }

    @Override
    public boolean isTerminated() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isTerminated'");
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'awaitTermination'");
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'submit'");
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'submit'");
    }

    @Override
    public Future<?> submit(Runnable task) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'submit'");
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'invokeAll'");
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'invokeAll'");
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'invokeAny'");
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'invokeAny'");
    }

    private class InstrumentedRunnable implements Runnable {
    }

    private class InstrumentedCallable implements Callable {

    }

    @Override
    public void initializeMetrics(SolrMetricsContext parentContext, Attributes attributes, String scope) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'initializeMetrics'");
    }

    @Override
    public SolrMetricsContext getSolrMetricsContext() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getSolrMetricsContext'");
    }

    @Override
    public String getName() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getName'");
    }

    @Override
    public String getDescription() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getDescription'");
    }

    @Override
    public Category getCategory() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getCategory'");
    }
}
