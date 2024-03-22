package org.apache.solr.client.solrj.impl;

import org.apache.solr.client.solrj.util.AsyncListener;
import org.apache.solr.common.util.NamedList;
import org.junit.Assert;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

public class DebugAsyncListener implements AsyncListener<NamedList<Object>> {

  private final CountDownLatch cdl;

  private final Semaphore wait = new Semaphore(1);

  public volatile boolean onStartCalled;

  public volatile boolean latchCounted;

  public volatile NamedList<Object> onSuccessResult = null;

  public volatile Throwable onFailureResult = null;

  public DebugAsyncListener(CountDownLatch cdl) {
    this.cdl = cdl;
  }

  @Override
  public void onStart() {
    onStartCalled = true;
  }

  public void pause() {
    try {
      wait.acquire();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }

  public void unPause() {
    wait.release();
  }

  @Override
  public void onSuccess(NamedList<Object> entries) {
    pause();
    onSuccessResult = entries;
    if (latchCounted) {
      Assert.fail("either 'onSuccess' or 'onFailure' should be called exactly once.");
    }
    cdl.countDown();
    latchCounted = true;
    unPause();
  }

  @Override
  public void onFailure(Throwable throwable) {
    pause();
    onFailureResult = throwable;
    if (latchCounted) {
      Assert.fail("either 'onSuccess' or 'onFailure' should be called exactly once.");
    }
    cdl.countDown();
    latchCounted = true;
    unPause();
  }
}
