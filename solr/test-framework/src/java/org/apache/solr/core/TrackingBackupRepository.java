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

package org.apache.solr.core;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.store.Directory;
import org.apache.solr.core.backup.repository.FilterBackupRepository;

public class TrackingBackupRepository extends FilterBackupRepository {
  private static final List<URI> COPIED_FILES = Collections.synchronizedList(new ArrayList<>());
  private static final List<URI> DIRECTORIES_CREATED =
      Collections.synchronizedList(new ArrayList<>());
  private static final List<URI> OUTPUTS_CREATED = Collections.synchronizedList(new ArrayList<>());

  @Override
  public OutputStream createOutput(URI path) throws IOException {
    OUTPUTS_CREATED.add(path);
    return super.createOutput(path);
  }

  @Override
  public void createDirectory(URI path) throws IOException {
    DIRECTORIES_CREATED.add(path);
    super.createDirectory(path);
  }

  @Override
  public void copyIndexFileFrom(
      Directory sourceDir, String sourceFileName, URI destDir, String destFileName)
      throws IOException {
    COPIED_FILES.add(resolve(destDir, destFileName));
    super.copyIndexFileFrom(sourceDir, sourceFileName, destDir, destFileName);
  }

  /**
   * @return list of files were copied by using {@link #copyFileFrom(Directory, String, URI)}
   */
  public static List<URI> copiedFiles() {
    return new ArrayList<>(COPIED_FILES);
  }

  public static List<URI> directoriesCreated() {
    return new ArrayList<>(DIRECTORIES_CREATED);
  }

  public static List<URI> outputsCreated() {
    return new ArrayList<>(OUTPUTS_CREATED);
  }

  /** Clear all tracking data */
  public static void clear() {
    COPIED_FILES.clear();
    DIRECTORIES_CREATED.clear();
    OUTPUTS_CREATED.clear();
  }
}
