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

package org.apache.solr.util;

import java.io.IOException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;

public class DocValuesUtil {

  public static NumericDocValues getNumeric(LeafReader reader, String field) throws IOException {
    return DocValues.unwrapSingleton(DocValues.getSortedNumeric(reader, field));
  }

  public static NumericDocValues getNumeric(
      LeafReader reader, String field, SortField.Type fieldType) throws IOException {
    try {
      // Works if the field was written with NumericDocValues.
      // TODO: Once Trie and Point Numeric Fields are removed, this won't be needed
      return DocValues.getNumeric(reader, field);
    } catch (IllegalStateException | IOException ise) {
      // Works if the field was written with SortedNumericDocValues but is single valued
      return SortedNumericSelector.wrap(DocValues.getSortedNumeric(reader, field), null, fieldType);
    }
  }
}
