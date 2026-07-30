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
package org.apache.solr.client.solrj.response;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;

/**
 * Simply puts the InputStream into an entry in a NamedList named "stream".
 *
 * @see InputStreamResponse
 */
public class InputStreamResponseParser extends ResponseParser {

  public static String STREAM_KEY = "stream";
  public static String HTTP_STATUS_KEY = "responseStatus";

  private final String writerType;

  public InputStreamResponseParser(String writerType) {
    this.writerType = writerType;
  }

  /**
   * When using a {@link InputStreamResponseParser}, the raw output is available in the response
   * under the key {@link #STREAM_KEY}.
   */
  public static String consumeResponseToString(NamedList<Object> response) throws IOException {
    assert response != null;
    String output;
    // Would be nice to validate the STREAM_KEY value is present
    try (InputStream responseStream = (InputStream) response.get(STREAM_KEY)) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      responseStream.transferTo(baos);
      output = baos.toString(StandardCharsets.UTF_8);
    }
    return output;
  }

  @Override
  public String getWriterType() {
    return writerType;
  }

  @Override
  public NamedList<Object> processResponse(InputStream body, String encoding) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getContentTypes() {
    return Set.of(); // don't enforce
  }

  public static NamedList<Object> createInputStreamNamedList(
      int httpStatus, InputStream inputStream) {
    final var nl = new SimpleOrderedMap<>();
    nl.add(STREAM_KEY, inputStream);
    nl.add(HTTP_STATUS_KEY, httpStatus);
    return nl;
  }
}
