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

import java.io.IOException;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;

public class MonitorDataValues {

  private SortedDocValues queryIdIt;
  private SortedDocValues cacheIdIt;
  private SortedDocValues mqIt;
  private SortedDocValues payloadIt;
  private NumericDocValues versionIt;
  private int currentDoc = DocIdSetIterator.NO_MORE_DOCS;
  private LeafReader reader;

  public void update(LeafReaderContext context) throws IOException {
    reader = context.reader();
    cacheIdIt = reader.getSortedDocValues(MonitorFields.CACHE_ID);
    queryIdIt = reader.getSortedDocValues(MonitorFields.QUERY_ID);
    mqIt = reader.getSortedDocValues(MonitorFields.MONITOR_QUERY);
    payloadIt = reader.getSortedDocValues(MonitorFields.PAYLOAD);
    versionIt = reader.getNumericDocValues(MonitorFields.VERSION);
    currentDoc = DocIdSetIterator.NO_MORE_DOCS;
  }

  public void advanceTo(int doc) {
    currentDoc = doc;
  }

  public String getQueryId() throws IOException {
    queryIdIt.advanceExact(currentDoc);
    return queryIdIt.lookupOrd(queryIdIt.ordValue()).utf8ToString();
  }

  public String getCacheId() throws IOException {
    cacheIdIt.advanceExact(currentDoc);
    return cacheIdIt.lookupOrd(cacheIdIt.ordValue()).utf8ToString();
  }

  public String getMq() throws IOException {
    if (mqIt != null && mqIt.advanceExact(currentDoc)) {
      return mqIt.lookupOrd(mqIt.ordValue()).utf8ToString();
    }
    return reader.document(currentDoc).get(MonitorFields.MONITOR_QUERY);
  }

  public String getPayload() throws IOException {
    if (payloadIt != null) {
      if (payloadIt.advanceExact(currentDoc)) {
        return payloadIt.lookupOrd(payloadIt.ordValue()).utf8ToString();
      }
      return null;
    }
    return reader.document(currentDoc).get(MonitorFields.PAYLOAD);
  }

  public long getVersion() throws IOException {
    versionIt.advanceExact(currentDoc);
    return versionIt.longValue();
  }
}
