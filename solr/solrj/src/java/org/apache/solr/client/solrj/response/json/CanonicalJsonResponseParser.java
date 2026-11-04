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

package org.apache.solr.client.solrj.response.json;

import java.io.IOException;
import java.io.InputStream;
import org.apache.solr.client.solrj.response.ResponseCanonicalizer;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.JsonTextWriter;
import org.apache.solr.common.util.NamedList;

/**
 * A JSON parser that converts the response to the canonical shape SolrJ's response objects expect
 * -- {@link NamedList} trees with {@link org.apache.solr.common.SolrDocumentList} for document
 * sections -- so that {@code QueryResponse} and its siblings can read a JSON response. It also asks
 * for {@code json.nl=map}, without which a {@code NamedList} cannot be reconstructed.
 *
 * <p>Callers that re-serialise the response or read its raw structure want {@link
 * JsonMapResponseParser} instead; this conversion would change what they see.
 */
public class CanonicalJsonResponseParser extends JsonMapResponseParser {

  private static final SolrParams CANONICAL_PARAMS =
      SolrParams.of(JsonTextWriter.JSON_NL_STYLE, JsonTextWriter.JSON_NL_MAP);

  /**
   * Asks for {@code json.nl=map}, so that a {@link NamedList} written by the server arrives as a
   * JSON object and {@link #processResponse} can restore it as a {@code NamedList}. Under the
   * default {@code json.nl=flat} the keys and values are flattened into one array, and the
   * structure cannot be recovered.
   */
  @Override
  public SolrParams getAdditionalRequestParams() {
    return CANONICAL_PARAMS;
  }

  @Override
  public NamedList<Object> processResponse(InputStream body, String encoding) throws IOException {
    return ResponseCanonicalizer.canonicalize(super.processResponse(body, encoding));
  }
}
