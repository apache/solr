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
package org.apache.solr.search;

public class ScoreDoc2 extends org.apache.lucene.search.ScoreDoc {

  public ScoreDoc2(int doc, float score) {
    super(doc, score);
  }

  /**
   * Original index of the doc in the result set.
   *
   * Only set/used by {@link QueryRescorer#rescore} and Solr's LTRRescorer.
   *
   * Could shardIndex be used for this instead?
   */
  public int index = -1;

  // A convenience method for debugging.
  @Override
  public String toString() {
    return "doc=" + doc + " score=" + score + " shardIndex=" + shardIndex + " index=" + index;
  }
}
