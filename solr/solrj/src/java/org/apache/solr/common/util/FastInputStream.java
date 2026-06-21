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

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import org.eclipse.jetty.io.RuntimeIOException;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** Single threaded buffered InputStream
 *  Internal Solr use only, subject to change.
 */
public class FastInputStream extends JavaBinInputStream implements DataInputInputStream, DataInput {
  private final SolrDataInputStream din;
//  public final stati ThreadLocal<byte[]> THREAD_LOCAL_BYTEARRAY= new ThreadLocal<>(){
//    protected byte[] initialValue() {
//      return new byte[8192];
//    }
//  };

 // protected long readFromStream; // number of bytes read from the underlying inputstream

  public FastInputStream(InputStream in) {
    this(in, 0);
  }

  public FastInputStream(InputStream in, int start) {
   // super(new FastInputStream.SolrDataInputStream(in));
    this.din = new FastInputStream.SolrDataInputStream(in);
//    if (start != 0) {
//      try {
//        position(start);
//      } catch (IOException e) {
//        throw new RuntimeIOException(e);
//      }
//    }
    //   flush();
  }

  public static FastInputStream wrap(InputStream in) {
    return (in instanceof FastInputStream) ? (FastInputStream)in : new FastInputStream(in);
  }

  @Override
  public int read() throws IOException {
    return din.read();
  }

//  public int peek() throws IOException {
//    return din.p();
//  }

//  /** Returns the internal buffer used for caching */
//  public byte[] getBuffer() {
//    return buffer;
//  }

//  @Override
//  public long position() throws IOException {
//    return readBytes;
//   // return super.position();
//  }

//  /** Current position within the internal buffer */
//  public int getPositionInBuffer() {
//    return pos;
//  }
//
//  /** Current end-of-data position within the internal buffer.  This is one past the last valid byte. */
//  public int getEndInBuffer() {
//    return pos + avail;
//  }


//  @Override
//  public int skipBytes(int n) throws IOException {
//    return (int) skipBytes(n);
//  }

  @Override
  public boolean readBoolean() throws IOException {
    return din.readBoolean();
  }

  @Override
  public void close() throws IOException {
    super.close();
    // must close din to release the underlying HTTP response InputStream; without this,
    // SolrStream.close() leaves the HTTP/2 response stream open, contaminating the next
    // request on the same connection with unconsumed response data from the prior request
    IOUtils.closeQuietly(din);
  }

  @Override
  public void readFully(byte b[]) throws IOException {
    readFully(b, 0, b.length);
  }

  @Override
  public void readFully(byte b[], int off, int len) throws IOException {
    din.readFully(b, off, len);
  }

  @Override
  public int skipBytes(int n) throws IOException {
    return (int) din.skip(n);
  }

  @Override
  public byte readByte() throws IOException {
    return din.readByte();
  }

  @Override
  public int readUnsignedByte() throws IOException {
    return din.readUnsignedByte();
  }


  @Override
  public short readShort() throws IOException {
    return (short) readUnsignedShort();
  }

  @Override
  public void readFully(JavaBinInputStream dis, byte[] bytes) throws IOException {
    din.readFully(bytes);
  }

  @Override
  public int readUnsignedShort() throws IOException {
    byte b1 = readAndCheckByte();
    byte b2 = readAndCheckByte();

    return Ints.fromBytes((byte) 0, (byte) 0, b2, b1);
  }

  @Override
  public char readChar() throws IOException {
    return din.readChar();
  }

  @Override
  public int readInt() throws IOException {
    byte b1 = readAndCheckByte();
    byte b2 = readAndCheckByte();
    byte b3 = readAndCheckByte();
    byte b4 = readAndCheckByte();

    return Ints.fromBytes(b4, b3, b2, b1);
  }

  @Override
  public long readLong() throws IOException {
    byte b1 = readAndCheckByte();
    byte b2 = readAndCheckByte();
    byte b3 = readAndCheckByte();
    byte b4 = readAndCheckByte();
    byte b5 = readAndCheckByte();
    byte b6 = readAndCheckByte();
    byte b7 = readAndCheckByte();
    byte b8 = readAndCheckByte();

    return Longs.fromBytes(b8, b7, b6, b5, b4, b3, b2, b1);
  }

  @Override
  public float readFloat() throws IOException {
    return Float.intBitsToFloat(readInt());
  }

  @Override
  public double readDouble() throws IOException {
    return Double.longBitsToDouble(readLong());
  }

  @Override
  public String readLine() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String readUTF() throws IOException {
    throw new UnsupportedOperationException();
  }

  private byte readAndCheckByte() throws IOException, EOFException {
    int b1 = din.read();

    if (-1 == b1) {
      throw new EOFException();
    }

    return (byte) b1;
  }

  private static class SolrDataInputStream extends DataInputStream {

    public SolrDataInputStream(InputStream in) {
      super(in);
    }

  }
}
