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

import org.apache.lucene.store.BufferedIndexInput;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Locale;

class S3IndexInput extends BufferedIndexInput {

    static final int LOCAL_BUFFER_SIZE = 16 * 1024;

    private final InputStream inputStream;
    private final long length;

    private long position;

    S3IndexInput(InputStream inputStream, String path, long length) {
        super(path);

        this.inputStream = inputStream;
        this.length = length;
    }

    @Override
    protected void readInternal(ByteBuffer b) throws IOException {

        int expectedLength = b.remaining();

        byte[] localBuffer;
        if (b.hasArray()) {
            localBuffer = b.array();
        } else {
            localBuffer = new byte[LOCAL_BUFFER_SIZE];
        }

        // We have no guarantee we read all the requested bytes from the underlying InputStream
        // in a single call. Loop until we reached the requested number of bytes.
        while (b.hasRemaining()) {
            int read;

            if (b.hasArray()) {
                read = inputStream.read(localBuffer, b.position(), b.remaining());
            } else {
                read = inputStream.read(localBuffer, 0, Math.min(b.remaining(), LOCAL_BUFFER_SIZE));
            }

            // Abort if we can't read any more data
            if (read < 0) {
                break;
            }

            if (b.hasArray()) {
                b.position(b.position() + read);
            } else {
                b.put(localBuffer, 0, read);
            }
        }

        if (b.remaining() > 0) {
            throw new IOException(String.format(Locale.ROOT, "Failed to read %d bytes; only %d available", expectedLength, (expectedLength - b.remaining())));
        }

        position += expectedLength;
    }

    @Override
    protected void seekInternal(long pos) throws IOException {
        if (pos > length()) {
            throw new EOFException("read past EOF: pos=" + pos + " vs length=" + length() + ": " + this);
        }

        long diff = pos - this.position;

        // If we seek forward, skip unread bytes
        if (diff > 0) {
            inputStream.skip(diff);
            position = pos;
        } else if (diff < 0) {
            throw new IOException("Cannot seek backward");
        }
    }

    @Override
    public final long length() {
        return length;
    }

    @Override
    public void close() throws IOException {
        this.inputStream.close();
    }

}
