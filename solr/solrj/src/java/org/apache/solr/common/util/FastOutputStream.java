/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.common.util;

import com.google.common.primitives.Longs;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

/** Single threaded buffered OutputStream
 *  Internal Solr use only, subject to change.
 */
public class FastOutputStream extends JavaBinOutputStream implements DataOutput {
  protected final SolrDataOutputStream dout;
  //protected final ExpandableDirectBufferOutputStream byteArrayOut;

  public FastOutputStream(OutputStream out) {
    this.dout = new SolrDataOutputStream(out);
   // byteArrayOut = null;
  }

//  public FastOutputStream(int sz) {
//    super(new ExpandableDirectBufferOutputStream(new ExpandableDirectByteBuffer(sz)));
//
//    byteArrayOut = (ExpandableDirectBufferOutputStream) out;
//    this.dout = new SolrDataOutputStream(byteArrayOut);
//  }

//  public byte[] getData() {
//    return byteArrayOut.buffer().byteArray();
//  }

  @Override
  public void write(int b) throws IOException {
    dout.writeByte(b);
  }

  @Override
  public void write(byte b[]) throws IOException {
    dout.write(b,0,b.length);
  }

  @Override public void write(byte[] b, int off, int len) throws IOException {
    dout.write(b, off, len);
  }

  @Override public void writeBoolean(boolean v) throws IOException {
    dout.writeBoolean(v);
  }

  @Override public void writeByte(int v) throws IOException {
    dout.writeByte(v);
  }

  @Override public void writeShort(int v) throws IOException {
    writeShort((short) v);
  }

  @Override public void writeChar(int v) throws IOException {
    dout.writeChar(v);
  }

  @Override public void writeInt(int v) throws IOException {
    dout.write(0xFF & v);
    dout.write(0xFF & (v >> 8));
    dout.write(0xFF & (v >> 16));
    dout.write(0xFF & (v >> 24));
  }

  @Override public void writeLong(long v) throws IOException {
    byte[] bytes = Longs.toByteArray(Long.reverseBytes(v));
    write(bytes, 0, bytes.length);
  }

  @Override public void writeFloat(float v) throws IOException {
    writeInt(Float.floatToIntBits(v));
  }

  @Override
  public void writeShort(short v) throws IOException {
    dout.write(0xFF & v);
    dout.write(0xFF & (v >> 8));
  }

  @Override public void writeDouble(double v) throws IOException {
    writeLong(Double.doubleToLongBits(v));
  }

  @Override public void writeBytes(String s) throws IOException {
    throw new UnsupportedEncodingException();
  }

  @Override public void writeChars(String s) throws IOException {
    throw new UnsupportedEncodingException();
  }

  @Override public void writeUTF(String s) throws IOException {
    throw new UnsupportedEncodingException();
  }

  /** reserve at least len bytes at the end of the buffer.
   * Invalid if len &gt; buffer.length
   */
  public void reserve(int len) throws IOException {

  }

  /** Only flushes the buffer of the FastOutputStream, not that of the
   * underlying stream.
   */
  public void flushBuffer() throws IOException {
    dout.flush();
  }

  public void flush() throws IOException {
    dout.flush();
  }

  @Override
  public void close() throws IOException {
    super.close();
    dout.close();
  }

  /** All writes to the sink will go through this method */
  //  public void flush(byte[] buf, int offset, int len) throws IOException {
  //  //  out.write(buf, offset, len);
  //  }

  /** Returns the number of bytes actually written to the underlying OutputStream, not including
   * anything currently buffered by this class itself.
   */
  public int written() {
    return dout.getWritten();
  }

  /** Resets the count returned by written() */
  public void setWritten(int written) {
    dout.setWritten(written);
  }

  /**Copies a {@link Utf8CharSequence} without making extra copies
   */
//  public void writeUtf8CharSeq(Utf8CharSequence utf8) throws IOException {
//    utf8.write(dout);
//  }

  public long size() {
    return dout.size();
  }

  private static class SolrDataOutputStream extends DataOutputStream {

    /**
     * Creates a new data output stream to write data to the specified
     * underlying output stream. The counter <code>written</code> is
     * set to zero.
     *
     * @param out the underlying output stream, to be saved for later
     *            use.
     * @see FilterOutputStream#out
     */
    public SolrDataOutputStream(OutputStream out) {
      super(out);
    }

    int getWritten() {
      return this.written;
    }

    void setWritten(int written) {
      this.written = written;
    }
  }
}
