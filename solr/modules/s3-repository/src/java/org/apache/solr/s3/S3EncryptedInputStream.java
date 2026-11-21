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
package org.apache.solr.s3;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Plaintext of an object read through the S3 encryption client. The plaintext is released before
 * the object's authentication tag has been checked, so this stream turns a failed check, which the
 * client reports as a {@link RuntimeException}, into an {@link IOException}, and does not hand out
 * the final bytes of the object until the check has passed.
 */
class S3EncryptedInputStream extends FilterInputStream {

  private final String s3Path;
  private final long length;
  private long position;
  private boolean endChecked;

  /**
   * @param in the decrypted stream; it must not convert runtime exceptions itself
   * @param length the plaintext length of the object
   */
  S3EncryptedInputStream(InputStream in, String s3Path, long length) {
    super(in);
    this.s3Path = s3Path;
    this.length = length;
  }

  @Override
  public int read() throws IOException {
    int b;
    try {
      b = in.read();
    } catch (RuntimeException e) {
      throw readFailure(e);
    }
    if (b < 0) {
      checkComplete();
    } else {
      advance(1);
    }
    return b;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int n;
    try {
      n = in.read(b, off, len);
    } catch (RuntimeException e) {
      throw readFailure(e);
    }
    if (n < 0) {
      checkComplete();
    } else {
      advance(n);
    }
    return n;
  }

  /** Never skips past the end, since a resumed read cannot be positioned beyond it. */
  @Override
  public long skip(long n) throws IOException {
    long toSkip = Math.min(n, length - position);
    if (toSkip <= 0) {
      return 0;
    }
    long skipped;
    try {
      skipped = in.skip(toSkip);
    } catch (RuntimeException e) {
      throw readFailure(e);
    }
    advance(skipped);
    return skipped;
  }

  @Override
  public int available() throws IOException {
    try {
      return in.available();
    } catch (RuntimeException e) {
      throw readFailure(e);
    }
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public void mark(int readlimit) {}

  @Override
  public void reset() throws IOException {
    throw new IOException("mark/reset not supported");
  }

  /**
   * Once the last plaintext byte has been read, reads on to the end of the stream so that a failed
   * authentication check surfaces now rather than on a read the caller may never make.
   */
  private void advance(long n) throws IOException {
    position += n;
    if (position >= length && !endChecked) {
      endChecked = true;
      int next;
      try {
        next = in.read();
      } catch (RuntimeException e) {
        throw readFailure(e);
      }
      if (next >= 0) {
        throw new S3Exception(
            "Encrypted S3 object '" + s3Path + "' is longer than its length of " + length);
      }
    }
  }

  private void checkComplete() throws IOException {
    if (position < length) {
      throw new S3Exception(
          "Encrypted S3 object '"
              + s3Path
              + "' ended after "
              + position
              + " of "
              + length
              + " bytes, before it was authenticated");
    }
  }

  private S3Exception readFailure(RuntimeException e) {
    if (S3StorageClient.isAuthenticationFailure(e)) {
      return new S3Exception(
          "Encrypted S3 object '"
              + s3Path
              + "' failed authentication: it was modified after it was written,"
              + " or was written with a different key",
          e);
    }
    return new S3Exception("Failed to read encrypted S3 object '" + s3Path + "'", e);
  }
}
