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
import java.util.Locale;
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
  public static String HTTP_REASON_KEY = "responseReason";

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

  /**
   * Throws if the response's HTTP status was not 2xx.
   *
   * <p>{@code SolrClient}s skip their usual non-2xx check when an {@link
   * InputStreamResponseParser} is in use, since the raw stream is handed back regardless of
   * status. Callers that read the stream under {@link #STREAM_KEY} directly -- rather than via
   * {@link #consumeResponseToString}, which does not check either -- should call this first.
   */
  public static void checkHttpStatus(NamedList<Object> response) throws IOException {
    checkHttpStatus(response, null);
  }

  /**
   * As {@link #checkHttpStatus(NamedList)}, appending {@code detail} to the exception message
   * when the status is not 2xx -- e.g. the request URL, or a body already consumed for another
   * purpose.
   */
  public static void checkHttpStatus(NamedList<Object> response, String detail)
      throws IOException {
    Object status = response.get(HTTP_STATUS_KEY);
    if (status instanceof Integer httpStatus && (httpStatus < 200 || httpStatus >= 300)) {
      Object reason = response.get(HTTP_REASON_KEY);
      String msg =
          reason instanceof String r && !r.isEmpty()
              ? String.format(Locale.ROOT, "Unexpected HTTP status [%d %s] in response", httpStatus, r)
              : String.format(Locale.ROOT, "Unexpected HTTP status [%d] in response", httpStatus);
      throw new IOException(detail == null ? msg : msg + ": " + detail);
    }
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
    return createInputStreamNamedList(httpStatus, null, inputStream);
  }

  /**
   * As {@link #createInputStreamNamedList(int, InputStream)}, also recording the HTTP reason
   * phrase (e.g. "Bad Request") under {@link #HTTP_REASON_KEY}, when known, so callers building
   * an error message have more to go on than the bare status code.
   */
  public static NamedList<Object> createInputStreamNamedList(
      int httpStatus, String reason, InputStream inputStream) {
    final var nl = new SimpleOrderedMap<>();
    nl.add(STREAM_KEY, inputStream);
    nl.add(HTTP_STATUS_KEY, httpStatus);
    if (reason != null) {
      nl.add(HTTP_REASON_KEY, reason);
    }
    return nl;
  }
}
