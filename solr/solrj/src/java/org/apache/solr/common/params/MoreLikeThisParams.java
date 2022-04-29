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
package org.apache.solr.common.params;

import java.util.Locale;

/** Params for {@code MoreLikeThisHandler}. */
public interface MoreLikeThisParams {
  public static final String MLT = "mlt";

  public static final String PREFIX = "mlt.";

  public static final String SIMILARITY_FIELDS = PREFIX + "fl";
  public static final String MIN_TERM_FREQ = PREFIX + "mintf";
  public static final String MAX_DOC_FREQ = PREFIX + "maxdf";
  public static final String MAX_DOC_FREQ_PCT = PREFIX + "maxdfpct";
  public static final String MIN_DOC_FREQ = PREFIX + "mindf";
  public static final String MIN_WORD_LEN = PREFIX + "minwl";
  public static final String MAX_WORD_LEN = PREFIX + "maxwl";
  public static final String MAX_QUERY_TERMS = PREFIX + "maxqt";
  public static final String MAX_NUM_TOKENS_PARSED = PREFIX + "maxntp";
  public static final String BOOST = PREFIX + "boost"; // boost or not?
  public static final String QF = PREFIX + "qf"; // boosting applied to mlt fields

  // the /mlt request handler uses 'rows'
  public static final String DOC_COUNT = PREFIX + "count";

  // Do you want to include the original document in the results or not
  public static final String MATCH_INCLUDE = PREFIX + "match.include";

  // If multiple docs are matched in the query, what offset do you want?
  public static final String MATCH_OFFSET = PREFIX + "match.offset";

  // Do you want to include the original document in the results or not
  public static final String INTERESTING_TERMS =
      PREFIX + "interestingTerms"; // false,details,(list or true)

  // the default doc count
  public static final int DEFAULT_DOC_COUNT = 5;

  public enum TermStyle {
    NONE,
    LIST,
    DETAILS;

    public static TermStyle get(String p) {
      if (p != null) {
        p = p.toUpperCase(Locale.ROOT);
        if (p.equals("DETAILS")) {
          return DETAILS;
        } else if (p.equals("LIST")) {
          return LIST;
        }
      }
      return NONE;
    }
  }
}
