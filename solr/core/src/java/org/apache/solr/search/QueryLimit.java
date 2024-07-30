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

import org.apache.lucene.index.QueryTimeout;

public interface QueryLimit extends QueryTimeout {
  /**
   * A value representing the portion of the specified limit that has been consumed. Reading this
   * value should never affect the outcome (other than the time it takes to do it).
   *
   * @return an expression of the amount of the limit used so far, numeric if possible, if
   *     non-numeric it should have toString() suitable for logging or similar expression to the
   *     user.
   */
  Object currentValue();
}
