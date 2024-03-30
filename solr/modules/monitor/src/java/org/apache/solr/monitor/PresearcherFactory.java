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

package org.apache.solr.monitor;

import org.apache.lucene.monitor.MultipassTermFilteredPresearcher;
import org.apache.lucene.monitor.Presearcher;
import org.apache.lucene.monitor.TermFilteredPresearcher;

public class PresearcherFactory {

  public static final String TERM_FILTERED = TermFilteredPresearcher.class.getSimpleName();
  public static final String MULTI_PASS_TERM_FILTERED =
      MultipassTermFilteredPresearcher.class.getSimpleName();

  public static Presearcher build() {
    return build(null);
  }

  public static Presearcher build(Object type) {
    if (TERM_FILTERED.equals(type)) {
      return new TermFilteredPresearcher();
    }
    if (MULTI_PASS_TERM_FILTERED.equals(type)) {
      return new MultipassTermFilteredPresearcher(3);
    }
    return Presearcher.NO_FILTERING;
  }
}
