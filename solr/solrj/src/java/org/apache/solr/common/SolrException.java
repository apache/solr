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
package org.apache.solr.common;

import static org.apache.solr.client.api.model.ErrorInfo.ERROR_CLASS;
import static org.apache.solr.client.api.model.ErrorInfo.ROOT_ERROR_CLASS;

import java.util.List;
import java.util.Map;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;

/** */
public class SolrException extends RuntimeException {

  /**
   * This list of valid HTTP Status error codes that Solr may return when there is a "Server Side"
   * error.
   *
   * @since solr 1.2
   */
  public enum ErrorCode {
    BAD_REQUEST(400),
    UNAUTHORIZED(401),
    FORBIDDEN(403),
    NOT_FOUND(404),
    CONFLICT(409),
    UNSUPPORTED_MEDIA_TYPE(415),
    TOO_MANY_REQUESTS(429),
    SERVER_ERROR(500),
    SERVICE_UNAVAILABLE(503),
    GATEWAY_TIMEOUT(504),
    INVALID_STATE(510),
    UNKNOWN(0);
    public final int code;

    ErrorCode(int c) {
      code = c;
    }

    public static ErrorCode getErrorCode(int c) {
      for (ErrorCode err : values()) {
        if (err.code == c) return err;
      }
      return UNKNOWN;
    }
  }

  public SolrException(ErrorCode code, String msg) {
    super(msg);
    this.code = code.code;
  }

  public SolrException(ErrorCode code, String msg, Throwable th) {
    super(msg, th);
    this.code = code.code;
  }

  public SolrException(ErrorCode code, Throwable th) {
    super(th);
    this.code = code.code;
  }

  /**
   * Constructor that can set arbitrary http status code. Not for use in Solr, but may be used by
   * clients in subclasses to capture errors returned by the servlet container or other HTTP
   * proxies.
   */
  protected SolrException(int code, String msg, Throwable th) {
    super(msg, th);
    this.code = code;
  }

  int code;
  protected NamedList<String> metadata;
  protected List<Map<String, Object>> details;

  /**
   * The HTTP Status code associated with this Exception. For SolrExceptions thrown by Solr "Server
   * Side", this should be a valid {@link ErrorCode}, however client side exceptions may contain an
   * arbitrary error code based on the behavior of the Servlet Container hosting Solr, or any HTTP
   * Proxies that may exist between the client and the server.
   *
   * @return The HTTP Status code associated with this Exception
   */
  public int code() {
    return code;
  }

  public void setMetadata(NamedList<String> metadata) {
    this.metadata = metadata;
  }

  public NamedList<String> getMetadata() {
    return metadata;
  }

  public String getMetadata(String key) {
    return (metadata != null && key != null) ? metadata.get(key) : null;
  }

  public void setMetadata(String key, String value) {
    if (key == null || value == null)
      throw new IllegalArgumentException("Exception metadata cannot be null!");

    if (metadata == null) metadata = new SimpleOrderedMap<>();
    metadata.add(key, value);
  }

  public void setDetails(List<Map<String, Object>> details) {
    this.details = details;
  }

  public List<Map<String, Object>> getDetails() {
    return details;
  }

  public String getResponseMessage() {
    return getMessage();
  }

  public String getThrowable() {
    return getMetadata(ERROR_CLASS);
  }

  public String getRootThrowable() {
    return getMetadata(ROOT_ERROR_CLASS);
  }

  // TODO: This doesn't handle cause loops
  public static Throwable getRootCause(Throwable t) {
    while (true) {
      Throwable cause = t.getCause();
      if (cause != null) {
        t = cause;
      } else {
        break;
      }
    }
    return t;
  }

  /** Cause chains are shallow in practice; the cap only guards against a cyclic chain. */
  private static final int MAX_CAUSE_DEPTH = 100;

  /**
   * Whether {@code t} or anything in its cause chain is of the given type. Prefer this to {@link
   * #getRootCause} when classifying a failure, since a transport may report it wrapped at any
   * depth.
   */
  public static boolean hasCause(Throwable t, Class<? extends Throwable> type) {
    int depth = 0;
    for (Throwable cause = t;
        cause != null && depth++ < MAX_CAUSE_DEPTH;
        cause = cause.getCause()) {
      if (type.isInstance(cause)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Ensure that the provided tragic exception is wrapped in a 5xx SolrException
   *
   * <p>Tragic exceptions (those that Lucene's IndexWriter uses to signify it has become inoperable)
   * are expected to have a 5xx error code. This method takes an input tragic exception and adds the
   * expected wrapper, if necessary.
   *
   * @param e the exception to check the code on. If not a SolrException, then this method acts as a
   *     no-op.
   */
  public static SolrException wrapLuceneTragicExceptionIfNecessary(Exception e) {
    if (e instanceof SolrException solrException) {
      assert solrException.code() >= 500 && solrException.code() < 600;
      return solrException;
    }

    return new SolrException(ErrorCode.SERVER_ERROR, e.getMessage(), e);
  }
}
