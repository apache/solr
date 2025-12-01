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
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.List;
import org.apache.solr.client.solrj.request.json.JacksonContentWriter;
import org.apache.solr.client.solrj.response.ResponseParser;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;

/**
 * Parses JSON, deserializing to a domain type (class) via Jackson data-bind.
 *
 * @lucene.experimental
 */
public class JacksonDataBindResponseParser<T> extends ResponseParser {

  private final Class<T> typeParam;

  public JacksonDataBindResponseParser(Class<T> typeParam) {
    this.typeParam = typeParam;
  }

  @Override
  public String getWriterType() {
    return "json";
  }

  @Override
  public Collection<String> getContentTypes() {
    return List.of("application/json");
  }

  // TODO it'd be nice if the ResponseParser could receive the mime type so it can parse
  //  accordingly, maybe json, cbor, smile

  @Override
  public NamedList<Object> processResponse(InputStream stream, String encoding) throws IOException {
    // TODO SOLR-17549 for error handling, implying a significant ResponseParser API change
    // TODO generalize to CBOR, Smile, ...
    var mapper = JacksonContentWriter.DEFAULT_MAPPER;

    final T parsedVal;
    if (encoding == null) {
      parsedVal = mapper.readValue(stream, typeParam);
    } else {
      parsedVal = mapper.readValue(new InputStreamReader(stream, encoding), typeParam);
    }

    return SimpleOrderedMap.of("response", parsedVal);
  }
}
