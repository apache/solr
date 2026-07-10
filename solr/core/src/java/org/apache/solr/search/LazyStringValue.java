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

import java.io.IOException;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;

final class LazyStringValue {

  final SortedDocValues dv;
  final int ord;
  final int missingOrd;

  LazyStringValue(SortedDocValues dv, int ord, int missingOrd) {
    this.dv = dv;
    this.ord = ord;
    this.missingOrd = missingOrd;
  }

  BytesRef materialize() throws IOException {
    if (ord == missingOrd || ord < 0) {
      return null;
    }
    return dv.lookupOrd(ord);
  }
}
