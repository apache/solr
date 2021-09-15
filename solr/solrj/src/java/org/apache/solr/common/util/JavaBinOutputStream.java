/// *
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
// package org.apache.solr.common.util;
//
// import com.google.errorprone.annotations.DoNotCall;
//
// import java.io.Closeable;
// import java.io.IOException;
// import java.io.OutputStream;
//
/// ** Single threaded buffered OutputStream
// *  Internal Solr use only, subject to change.
// */
// public final class JavaBinOutputStream {
//  private final OutputStream out;
//
//  private final boolean isFastOutputStream;
//  private final byte[] buf;
// // private long written;  // how many bytes written to the underlying stream
//  private int pos;
//
//  public JavaBinOutputStream(OutputStream w) {
//    // match jetty output buffer
//    this(w, new byte[32768], 0);
//  }
//
//  public JavaBinOutputStream(OutputStream sink, byte[] tempBuffer, int start) {
//    this.out = sink;
//    this.buf = tempBuffer;
//    this.pos = start;
//    if (sink instanceof FastOutputStream) {
//      isFastOutputStream = true;
//      if (tempBuffer != null) {
//        throw new IllegalArgumentException("FastInputStream cannot bass a buffer to
// JavaBinInputStream");
//      }
//    } else {
//      isFastOutputStream = false;
//    }
//  }
//
//  public static JavaBinOutputStream wrap(FastOutputStream sink) {
//    return new JavaBinOutputStream(sink, null, 0);
//  }
//
//  public static JavaBinOutputStream wrap(OutputStream sink) {
//   return new JavaBinOutputStream(sink);
//  }
//
//  public void write(int b) throws IOException {
//    if (buf == null) {
//      out.write((byte) b);
//      return;
//    }
//
//    try {
//      buf[pos++] = (byte) b;
//    } catch (ArrayIndexOutOfBoundsException e) {
//
//        flush(buf, 0, buf.length);
//        pos=0;
//
//        buf[pos++] = (byte) b;
//
//    }
//  }
//
//  public void write(byte[] b) throws IOException {
//    if (buf == null) {
//      out.write(b, 0, b.length);
//      return;
//    }
//    write(b,0,b.length);
//  }
//
//  public void write(byte b) throws IOException {
//    if (buf == null) {
//      out.write(b);
//      return;
//    }
//
//
////    try {
////    buf[pos++] = b;
////    } catch (ArrayIndexOutOfBoundsException e) {
////      flush(buf, 0, buf.length);
////      pos=0;
////
////      buf[pos++] = b;
////    }
//    if (pos >= buf.length) {
//      //written += pos;
//      flush(buf, 0, buf.length);
//      pos=0;
//    }
//    buf[pos++] = b;
//  }
//
//  public void write(byte[] arr, int off, int len) throws IOException {
//    if (buf == null) {
//      out.write(arr, off, len);
//      return;
//    }
//
//    for(;;) {
//      int space = buf.length - pos;
//
//      if (len <= space) {
//        System.arraycopy(arr, off, buf, pos, len);
//        pos += len;
//        return;
//      } else if (len > buf.length) {
//        if (pos>0) {
//          flush(buf,0,pos);  // flush
//          pos=0;
//        }
//        // don't buffer, just write to sink
//        flush(arr, off, len);
//        return;
//      }
//
//      // buffer is too big to fit in the free space, but
//      // not big enough to warrant writing on its own.
//      // write whatever we can fit, then flush and iterate.
//
//      System.arraycopy(arr, off, buf, pos, space);
//      flush(buf, 0, buf.length);
//      pos = 0;
//      off += space;
//      len -= space;
//    }
//  }
//
//
//  /** reserve at least len bytes at the end of the buffer.
//   * Invalid if len &gt; buffer.length
//   */
//  public void reserve(int len) throws IOException {
//    if (buf == null) {
//      if (isFastOutputStream) {
//        ((FastOutputStream)out).reserve(len);
//      }
//      return;
//    }
//    if (len > (buf.length - pos))
//      if (pos > 0) {
//        flush(buf, 0, pos);
//        pos=0;
//      }
//  }
//
//  ////////////////// DataOutput methods ///////////////////
//  public void writeBoolean(boolean v) throws IOException {
//    if (buf == null) {
//      if (v) {
//        out.write((byte) 1);
//      } else {
//        out.write((byte) 0);
//      }
//      return;
//    }
//
//    write(v ? 1:0);
//  }
//
//  public void writeByte(int v) throws IOException {
//    if (buf == null) {
//      out.write((byte) v);
//      return;
//    }
//
//    try {
//      buf[pos++] = (byte) v;
//    } catch (ArrayIndexOutOfBoundsException e) {
//      flush(buf, 0, buf.length);
//      pos=0;
//
//      buf[pos++] = (byte) v;
//    }
//  }
//
//  public void writeShort(int v) throws IOException {
//    write((byte)(v >>> 8));
//    write((byte)v);
//  }
//
//  public void writeChar(int v) throws IOException {
//    writeShort(v);
//  }
//
//  public void writeInt(int v) throws IOException {
//    if (buf == null) {
//      out.write((byte) (v >>> 24));
//      out.write((byte) (v >>> 16));
//      out.write((byte) (v >>> 8));
//      out.write((byte) (v));
//      pos += 4;
//      return;
//    }
//
//    if (4 > (buf.length - pos))
//      if (pos > 0) {
//        flush(buf, 0, pos);
//        pos=0;
//      }
//    buf[pos] = (byte)(v>>>24);
//    buf[pos+1] = (byte)(v>>>16);
//    buf[pos+2] = (byte)(v>>>8);
//    buf[pos+3] = (byte)(v);
//    pos+=4;
//  }
//
//  public void writeLong(long v) throws IOException {
//    if (buf == null) {
//      out.write((byte) (v >>> 56));
//      out.write((byte) (v >>> 48));
//      out.write((byte) (v >>> 40));
//      out.write((byte) (v >>> 32));
//      out.write((byte) (v >>> 24));
//      out.write((byte) (v >>> 16));
//      out.write((byte) (v >>> 8));
//      out.write((byte) (v));
//      pos += 8;
//      return;
//    }
//
//
//    if (8 > (buf.length - pos))
//      if (pos > 0) {
//        flush(buf, 0, pos);
//        pos=0;
//      }
//    buf[pos] = (byte)(v>>>56);
//    buf[pos+1] = (byte)(v>>>48);
//    buf[pos+2] = (byte)(v>>>40);
//    buf[pos+3] = (byte)(v>>>32);
//    buf[pos+4] = (byte)(v>>>24);
//    buf[pos+5] = (byte)(v>>>16);
//    buf[pos+6] = (byte)(v>>>8);
//    buf[pos+7] = (byte)(v);
//    pos+=8;
//  }
//
//  public void writeFloat(float v) throws IOException {
//    writeInt(Float.floatToRawIntBits(v));
//  }
//
//  public void writeDouble(double v) throws IOException {
//    writeLong(Double.doubleToRawLongBits(v));
//  }
//
//  @DoNotCall
//  public void writeBytes(String s) throws IOException {
//    throw new UnsupportedOperationException();
//  }
//
//  @DoNotCall
//  public void writeChars(String s) throws IOException {
//    throw new UnsupportedOperationException();
//  }
//
//  @DoNotCall
//  public void writeUTF(String s) {
//    throw new UnsupportedOperationException();
//  }
//
//  public void flush() throws IOException {
//    if (buf == null) {
//      out.flush();
//      return;
//    }
//
//    if (pos > 0) {
//      flush(buf, 0, pos);
//      pos=0;
//    }
//    if (out != null) out.flush();
//  }
//
//  public void close() throws IOException {
//
//    if (buf == null) {
//      if (out != null) out.close();
//      return;
//    }
//
//    if (pos > 0) {
//      flush(buf, 0, pos);
//      pos=0;
//    }
//    if (out != null) out.close();
//  }
//
//  /** Only flushes the buffer of the FastOutputStream, not that of the
//   * underlying stream.
//   */
//  public void flushBuffer() throws IOException {
//    if (buf == null) {
//      if (isFastOutputStream) {
//        ((FastOutputStream) out).flushBuffer();
//      }
//      return;
//    }
//
//    if (pos > 0) {
//      flush(buf, 0, pos);
//      pos=0;
//    }
//  }
//
//  /** All writes to the sink will go through this method */
//  public void flush(byte[] buf, int offset, int len) throws IOException {
//    out.write(buf, offset, len);
//  }
//
//
//  /**Copies a {@link Utf8CharSequence} without making extra copies
//   */
//  public void writeUtf8CharSeq(Utf8CharSequence utf8) throws IOException {
//    if (buf == null) {
//      if (utf8 instanceof ByteArrayUtf8CharSequence) {
//        out.write(((ByteArrayUtf8CharSequence) utf8).getBuf());
//        return;
//      }
//      utf8.write(out);
//      return;
//    }
//
//    int start = 0;
//    int totalWritten = 0;
//    while (true) {
//      final int size = utf8.size();
//      if (!(totalWritten < size)) break;
//      if (pos >= buf.length) flushBuffer();
//      int sz = utf8.write(start, buf, pos);
//      pos += sz;
//      totalWritten += sz;
//      start += sz;
//    }
//  }
//
//  public OutputStream getOutPut() {
//    return out;
//  }
// }
