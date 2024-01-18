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
package org.apache.solr.parser;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

/**
 * Queries can implement this to indicate that they store a startOffset. The startOffset would be
 * the character position in the input text where the earliest Term in the query was derived from.
 */
public interface OffsetHolder {

  /** Returns the startOffset associated with this Query or null if there is none. */
  Integer getStartOffset();

  boolean hasStartOffset();

  /**
   * Returns a startOffset from a Query which might not implement OffsetHolder. If the Query IS an
   * OffsetHolder, its startOffset is returned. If the Query is a BooleanQuery the method is called
   * recursively on its first clause.
   */
  static Integer getStartOffset(Query q) {
    if (q instanceof OffsetHolder) {
      return ((OffsetHolder) q).getStartOffset();
    }
    if (q instanceof BooleanQuery) {
      BooleanQuery bq = (BooleanQuery) q;
      if (bq.clauses() != null && !bq.clauses().isEmpty()) {
        return getStartOffset(bq.clauses().get(0).getQuery());
      }
    }
    return null;
  }
}
