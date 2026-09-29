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
package org.apache.solr.analysis;

import java.util.Map;
import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenStream;

/**
 * Factory for {@link ZeroPositionIncrementFilter}.
 *
 * <p>Use this on a query analyzer when later tokens should be synonyms of the first token rather
 * than a phrase. The shipped {@code ancestor_path} field types use it after {@code
 * PathHierarchyTokenizer} so Lucene 10 sequential path prefixes still match as ancestors.
 *
 * <pre class="prettyprint">
 * &lt;fieldType name="ancestor_path" class="solr.TextField"&gt;
 *   &lt;analyzer type="index"&gt;
 *     &lt;tokenizer class="solr.KeywordTokenizerFactory"/&gt;
 *   &lt;/analyzer&gt;
 *   &lt;analyzer type="query"&gt;
 *     &lt;tokenizer class="solr.PathHierarchyTokenizerFactory" delimiter="/"/&gt;
 *     &lt;filter class="solr.ZeroPositionIncrementFilterFactory"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * @lucene.spi {@value #NAME}
 */
public class ZeroPositionIncrementFilterFactory extends TokenFilterFactory {

  /** SPI name */
  public static final String NAME = "zeroPositionIncrement";

  /** Creates a new ZeroPositionIncrementFilterFactory */
  public ZeroPositionIncrementFilterFactory(Map<String, String> args) {
    super(args);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  /** Default ctor for compatibility with SPI */
  public ZeroPositionIncrementFilterFactory() {
    throw defaultCtorException();
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new ZeroPositionIncrementFilter(input);
  }
}
