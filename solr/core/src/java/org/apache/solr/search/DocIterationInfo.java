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

/** Information for the current document in the <code>DocIterator</code>. */
public interface DocIterationInfo {

  /**
   * Returns the score for the document just returned by <code>nextDoc()</code>
   *
   * <p>The value returned may be meaningless depending on the context in which the DocIterator
   * instance was retrieved.
   */
  public float score();

  /**
   * Returns the query match score in case of rerank queries
   *
   * @return the query match score in case of a rerank query, null otherwise.
   */
  public default Float matchScore() {
    return null;
  }
}
