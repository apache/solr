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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;

public class SimpleMatcherProxy extends CollectingMatcherProxy<QueryMatch> {

  public SimpleMatcherProxy(IndexSearcher searcher, ScoreMode scoreMode) {
    super(searcher, scoreMode);
  }

  @Override
  public QueryMatch resolve(QueryMatch match1, QueryMatch match2) {
    return match1;
  }

  @Override
  protected QueryMatch doMatch(String queryId, int doc, Scorable scorer) throws IOException {
    super.consumeMatch(queryId, doc);
    return new QueryMatch(queryId);
  }
}
