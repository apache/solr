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

package org.apache.solr.zero.util;

/**
 * FileTransferCounter contains counts of expected and actual files transferred, and actual bytes
 * transferred in Zero store operations
 */
public class FileTransferCounter {
  private long expectedFilesTransferred;
  private long actualFilesTransferred;
  private long bytesTransferred;

  public long getExpectedFilesTransferred() {
    return expectedFilesTransferred;
  }

  public long getActualFilesTransferred() {
    return actualFilesTransferred;
  }

  public long getBytesTransferred() {
    return bytesTransferred;
  }

  public void incrementActualFilesTransferred() {
    this.actualFilesTransferred++;
  }

  public void incrementBytesTransferred(long additionalBytesTransferred) {
    this.bytesTransferred += additionalBytesTransferred;
  }

  public void setExpectedFilesTransferred(long expectedFilesTransferred) {
    this.expectedFilesTransferred = expectedFilesTransferred;
  }
}
