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
import java.util.function.BiPredicate;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BytesRef;

public class QueryTermFilterVisitor implements BiPredicate<String, BytesRef> {

  private final QueryIndex.QueryTermFilter queryTermFilter;

  public QueryTermFilterVisitor(IndexReader reader) throws IOException {
    this.queryTermFilter = new QueryIndex.QueryTermFilter(reader);
  }

  @Override
  public boolean test(String field, BytesRef bytesRef) {
    return queryTermFilter.test(field, bytesRef);
  }
}
