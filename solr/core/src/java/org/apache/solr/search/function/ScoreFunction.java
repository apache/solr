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
package org.apache.solr.search.function;

import java.io.IOException;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.FloatDocValues;
import org.apache.lucene.search.Scorable;
import org.apache.solr.common.SolrException;

/** Returns the score of the current hit. */
public final class ScoreFunction extends ValueSource {

  @Override
  public FunctionValues getValues(Map<Object, Object> context, LeafReaderContext readerContext)
      throws IOException {
    Scorable scorer = (Scorable) context.get("scorer");
    if (scorer == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "score() function cannot access the document scores");
    }

    return new FloatDocValues(this) {
      @Override
      public float floatVal(int doc) throws IOException {
        assert scorer.docID() == doc
            : "Expected scorer to be positioned on doc: " + doc + ", but was: " + scorer.docID();
        return scorer.score();
      }

      @Override
      public boolean exists(int doc) {
        assert scorer.docID() == doc
            : "Expected scorer to be positioned on doc: " + doc + ", but was: " + scorer.docID();
        return true;
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof ScoreFunction;
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }

  @Override
  public String description() {
    return "score()";
  }
}
