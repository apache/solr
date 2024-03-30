/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.lucene.monitor;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReader;

public class DocumentBatchVisitor implements Closeable, Supplier<LeafReader> {

  private final DocumentBatch batch;
  private final List<Document> docs;

  private DocumentBatchVisitor(DocumentBatch batch, List<Document> docs) {
    this.batch = batch;
    this.docs = docs;
  }

  public static DocumentBatchVisitor of(Analyzer analyzer, List<Document> docs) {
    return new DocumentBatchVisitor(
        DocumentBatch.of(analyzer, docs.toArray(new Document[0])), docs);
  }

  @Override
  public void close() throws IOException {
    batch.close();
  }

  @Override
  public LeafReader get() {
    return batch.get();
  }

  public int size() {
    return docs.size();
  }

  @Override
  public String toString() {
    return docs.stream().map(Document::toString).collect(Collectors.joining(" "));
  }
}
