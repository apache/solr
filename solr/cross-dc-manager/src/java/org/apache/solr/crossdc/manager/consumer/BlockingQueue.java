package org.apache.solr.crossdc.manager.consumer;

import java.util.concurrent.ArrayBlockingQueue;

public class BlockingQueue<E> extends ArrayBlockingQueue<E> {

    public BlockingQueue(int capacity) {
        super(capacity);
    }


    @Override
    public boolean offer(E r) {
        //return super.offer(r);
        try {
            super.put(r);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        return true;
    }


}
