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
  }

  public static ByteBufferPool getInstance() {
    return Holder.pool;
  }

//  static {
//    SolrQTP.registerThreadLocal(buffer1);
//    SolrQTP.registerThreadLocal(buffer2);
//  }
}
