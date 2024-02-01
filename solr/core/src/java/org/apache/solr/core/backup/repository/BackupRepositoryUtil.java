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
package org.apache.solr.core.backup.repository;

import java.io.IOException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.core.DirectoryFactory;

/** Utility methods for {@link BackupRepository}. */
public class BackupRepositoryUtil {

  /**
   * Copy a file from a source {@link Directory} to a destination {@link Directory} without
   * verifying the checksum.
   *
   * @param sourceDir The source directory hosting the file to be copied.
   * @param sourceFileName The name of the file to be copied.
   * @param destDir The destination directory to copy the file to.
   * @param destFileName The name of the copied file at destination.
   */
  public static void copyFileNoChecksum(
      Directory sourceDir, String sourceFileName, Directory destDir, String destFileName)
      throws IOException {
    boolean success = false;
    try (IndexInput is = sourceDir.openInput(sourceFileName, DirectoryFactory.IOCONTEXT_NO_CACHE);
        IndexOutput os = destDir.createOutput(destFileName, DirectoryFactory.IOCONTEXT_NO_CACHE)) {
      os.copyBytes(is, is.length());
      success = true;
    } finally {
      if (!success) {
        IOUtils.deleteFilesIgnoringExceptions(destDir, destFileName);
      }
    }
  }
}
