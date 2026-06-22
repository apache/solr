package org.apache.solr.common.util;

public class ExpandableBuffers {
//  public final static ThreadLocal<MutableDirectBuffer> buffer1 = new ThreadLocal<>() {
//
//    protected MutableDirectBuffer initialValue() {
//      return new ExpandableDirectByteBuffer(8192);
//    }
//  };
//
//  public final static ThreadLocal<MutableDirectBuffer> buffer2 = new ThreadLocal<>() {
//
//    protected MutableDirectBuffer initialValue() {
//      return new ExpandableDirectByteBuffer(8192);
//    }
//  };

  public static class Holder {
    private static final ArrayByteBufferPool pool = new ArrayByteBufferPool();

    static {
      // Wire the Solr singleton pool's accounting into the central BufferMetrics registry.
      // We register only THIS pool (not every AbstractByteBufferPool) so that Jetty's own internal
      // pools are not double-counted. Retained gauges read live from the pool; allocated counters
      // are cumulative.
      BufferMetrics metrics = BufferMetrics.getInstance();
      metrics.registerDirectRetainedSupplier(pool::getDirectMemory);
      metrics.registerHeapAgronaRetainedSupplier(pool::getHeapMemory);
      metrics.registerDirectAllocatedSupplier(pool::getDirectAllocated);
      metrics.registerHeapAgronaAllocatedSupplier(pool::getHeapAllocated);
    }
  }

  public static ByteBufferPool getInstance() {
    return Holder.pool;
  }

//  static {
//    SolrQTP.registerThreadLocal(buffer1);
//    SolrQTP.registerThreadLocal(buffer2);
//  }
}
