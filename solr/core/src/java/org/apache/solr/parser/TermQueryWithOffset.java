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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;

/**
 * A TermQuery that also has an Integer startOffset taken from the Token that gave rise to the
 * contained Term.
 */
public class TermQueryWithOffset extends TermQuery implements OffsetHolder {

  private final Integer startOffset;

  public TermQueryWithOffset(Term t, Integer startOffset) {
    super(t);
    this.startOffset = startOffset;
  }

  public Integer getStartOffset() {
    return startOffset;
  }

  public boolean hasStartOffset() {
    return startOffset != null;
  }

  /**
   * Equality is based on the contained Term and ignores the startOffset. A TermQueryWithOffset will
   * consider itself equal to a TermQuery as long as the contained Terms are themselves equal.
   *
   * <p>Note that this relationship is not currently symmetric. A TermQuery that isn't a
   * TermQueryWithOffset will not consider itself equal to any TermQueryWithOffset because
   * TermQuery.equals requires class equality. This could be fixed by updating TermQuery.equals
   * inside the lucene codebase.
   */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof TermQuery)) {
      return false;
    }
    return getTerm().equals(((TermQuery) other).getTerm());
  }
}
