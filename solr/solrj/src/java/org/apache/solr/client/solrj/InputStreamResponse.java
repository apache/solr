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
package org.apache.solr.client.solrj;

import static org.apache.solr.client.solrj.impl.InputStreamResponseParser.HTTP_STATUS_KEY;
import static org.apache.solr.client.solrj.impl.InputStreamResponseParser.STREAM_KEY;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.util.function.Function;
import org.apache.solr.client.solrj.impl.InputStreamResponseParser;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the NamedList response format created by {@link InputStreamResponseParser}.
 *
 * <p>Particularly useful when targeting APIs that return arbitrary or binary data (e.g. replication
 * APIs for fetching index files)
 */
public class InputStreamResponse extends SimpleSolrResponse {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // True if the HTTP status is '200 OK', false otherwise
  public static final Function<Integer, Boolean> HTTP_OK_VALIDATOR = (status) -> 200 == status;
  // True if the HTTP status is neither a 4xx or a 5xx error.
  public static final Function<Integer, Boolean> NON_ERROR_CODE_VALIDATOR =
      (status) -> status < 399;

  @Override
  public void setResponse(NamedList<Object> rsp) {
    if (rsp.get(STREAM_KEY) == null) {
      throw new IllegalArgumentException(
          "Missing key '"
              + STREAM_KEY
              + "'; "
              + getClass().getSimpleName()
              + " can only be used with requests or clients configured to use "
              + InputStreamResponseParser.class.getSimpleName());
    }
    super.setResponse(rsp);
  }

  public int getHttpStatus() {
    return (int) getResponse().get(HTTP_STATUS_KEY);
  }

  /**
   * Access the server response as an {@link InputStream}, regardless of the HTTP status code
   *
   * <p>Caller is responsible for consuming and closing the stream, and releasing it from the
   * tracking done by {@link ObjectReleaseTracker}. No validation is done on the HTTP status code.
   */
  public InputStream getResponseStream() {
    final NamedList<Object> resp = getResponse();

    return (InputStream) resp.get(STREAM_KEY);
  }

  /**
   * Access the server response as an {@link InputStream}, after ensuring that the HTTP status code
   * is 200 ('OK')
   *
   * <p>Caller is responsible for consuming and closing the stream, and releasing it from the
   * tracking done by {@link ObjectReleaseTracker}.
   */
  public InputStream getResponseStreamIfSuccessful() {
    return getResponseStreamIfSuccessful(HTTP_OK_VALIDATOR);
  }

  /**
   * Access the server response as an {@link InputStream}, after ensuring the HTTP status code
   * passes a provided validator.
   *
   * @param statusValidator a function that returns true iff the response body should be returned
   */
  public InputStream getResponseStreamIfSuccessful(Function<Integer, Boolean> statusValidator) {
    validateExpectedStatus(statusValidator);
    return getResponseStream();
  }

  private void validateExpectedStatus(Function<Integer, Boolean> statusChecker) {
    final var httpStatus = getHttpStatus();
    if (!statusChecker.apply(httpStatus)) {
      try {
        log.error(
            "Request returned unexpected HTTP status code {}; response content: {}",
            httpStatus,
            consumeAndStringifyForLogging(getResponseStream()));
      } catch (IOException e) {
        log.error("could not print error", e);
      }
      throw new SolrException(
          SolrException.ErrorCode.getErrorCode(httpStatus),
          "Unexpected status code [" + httpStatus + "] on response.");
    }
  }

  private String consumeAndStringifyForLogging(InputStream inputStream) throws IOException {
    final var baos = new ByteArrayOutputStream();
    try {
      inputStream.transferTo(baos);
      return baos.toString(Charset.defaultCharset());
    } finally {
      ObjectReleaseTracker.release(inputStream);
      IOUtils.closeQuietly(baos);
      IOUtils.closeQuietly(inputStream);
    }
  }
}
