package org.apache.solr.common.util;

import org.jctools.queues.MpmcUnboundedXaddArrayQueue;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class MPMCQueue {
  public static class RunnableBlockingQueue implements BlockingQueue<Runnable> {
    private final MpmcUnboundedXaddArrayQueue<Runnable> mpmc = new MpmcUnboundedXaddArrayQueue<>(32);

    public RunnableBlockingQueue() {

    }

    @Override public Runnable remove() {
      throw new UnsupportedOperationException();
    }

    @Override public Runnable poll() {
      return mpmc.relaxedPoll();
    }

    @Override public Runnable element() {
      throw new UnsupportedOperationException();
    }

    @Override public Runnable peek() {
      return mpmc.relaxedPeek();
    }

    @Override public Runnable take() throws InterruptedException {
      return mpmc.relaxedPoll();
    }

    @Override public Runnable poll(long timeout, TimeUnit unit) throws InterruptedException {
      return mpmc.relaxedPoll();
    }

    @Override public int remainingCapacity() {
      return mpmc.capacity() - mpmc.size();
    }

    @Override public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean containsAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override public void clear() {
      throw new UnsupportedOperationException();
    }

    @Override public boolean equals(Object o) {
      return false;
    }

    @Override public int hashCode() {
      return 0;
    }

    @Override public boolean addAll(Collection<? extends Runnable> c) {
      throw new UnsupportedOperationException();
    }

    @Override public int size() {
      return mpmc.size();
    }

    @Override public boolean isEmpty() {
      return mpmc.isEmpty();
    }

    @Override public boolean contains(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override public Iterator<Runnable> iterator() {
      return mpmc.iterator();
    }

    @Override public Object[] toArray() {
      return new Object[0];
    }

    @Override public <T> T[] toArray(T[] a) {
      throw new UnsupportedOperationException();
    }

    @Override public int drainTo(Collection<? super Runnable> c, int maxElements) {
      throw new UnsupportedOperationException();
    }

    @Override public int drainTo(Collection<? super Runnable> c) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean offer(Runnable e, long timeout, TimeUnit unit) throws InterruptedException {
      return mpmc.relaxedOffer(e);
    }

    @Override public void put(Runnable e) throws InterruptedException {
      mpmc.relaxedOffer(e);
    }

    @Override public boolean offer(Runnable e) {
      return mpmc.relaxedOffer(e);
    }

    @Override public boolean add(Runnable e) {
      throw new UnsupportedOperationException();
    }
  }
}
