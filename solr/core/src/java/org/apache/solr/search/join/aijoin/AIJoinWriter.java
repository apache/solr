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
package org.apache.solr.search.join.aijoin;

import java.io.IOException;
import java.util.Map;
import org.apache.lucene.index.IndexWriter;
import org.apache.solr.search.join.aijoin.AIJoinUtil.JoinColumnModel;

/**
 * Isolates every write-side representation of a join batch behind this one write-only surface, so
 * {@link AIJoinIndex} and {@link AIJoinUtil} deal only in {@link JoinColumnModel} and never need to
 * import a particular field-writing API themselves. Two sibling package-private implementations
 * exist, selected by {@link AIJoinIndex#DOC_WRITER_DELEGATE}: {@code AIJoinColumnWriter}, built on
 * {@code org.apache.lucene.document.column}, and {@link AIJoinDocWriter}, built on the plain {@link
 * org.apache.lucene.document.Document} / {@link IndexWriter#addDocuments} API.
 */
abstract class AIJoinWriter {

  protected AIJoinWriter() {}

  /**
   * Builds the pair columns for every entry in {@code mappings} (doc-map plus edges, keyed by pair
   * field name) and writes them as a single indexing call + commit, so a batch's columns all land
   * at doc 0 of their own sidecar segment.
   */
  abstract void writeJoinColumns(IndexWriter writer, Map<String, JoinColumnModel> mappings)
      throws IOException;
}
