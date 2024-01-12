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
package org.apache.solr.servlet;

import java.io.PrintWriter;
import java.io.StringWriter;
import org.apache.solr.api.ApiBag;
import org.apache.solr.client.api.model.ErrorInfo;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;

/** Response helper methods. */
public class ResponseUtils {
  private ResponseUtils() {}

  // System property to use if the Solr core does not exist or solr.hideStackTrace is not
  // configured. (i.e.: a lot of unit test).
  private static final boolean SYSTEM_HIDE_STACK_TRACES = Boolean.getBoolean("solr.hideStackTrace");

  /**
   * Adds the given Throwable's message to the given NamedList.
   *
   * <p>Primarily used by v1 code; v2 endpoints or dispatch code should call {@link
   * #getTypedErrorInfo(Throwable, Logger)}
   *
   * <p>If the response code is not a regular code, the Throwable's stack trace is both logged and
   * added to the given NamedList.
   *
   * <p>Status codes less than 100 are adjusted to be 500.
   *
   * @see #getTypedErrorInfo(Throwable, Logger)
   */
  public static int getErrorInfo(Throwable ex, NamedList<Object> info, Logger log) {
    return getErrorInfo(ex, info, log, false);
  }

  /**
   * Adds the given Throwable's message to the given NamedList.
   *
   * <p>Primarily used by v1 code; v2 endpoints or dispatch code should call {@link
   * #getTypedErrorInfo(Throwable, Logger)}
   *
   * <p>If the response code is not a regular code, the Throwable's stack trace is both logged and
   * added to the given NamedList.
   *
   * <p>Status codes less than 100 are adjusted to be 500.
   *
   * <p>Stack trace will not be output if hideTrace=true OR system property
   * solr.hideStackTrace=true.
   *
   * @see #getTypedErrorInfo(Throwable, Logger)
   */
  public static int getErrorInfo(
      Throwable ex, NamedList<Object> info, Logger log, boolean hideTrace) {
    int code = 500;
    if (ex instanceof SolrException) {
      SolrException solrExc = (SolrException) ex;
      code = solrExc.code();
      NamedList<String> errorMetadata = solrExc.getMetadata();
      if (errorMetadata == null) {
        errorMetadata = new NamedList<>();
      }
      errorMetadata.add(ErrorInfo.ERROR_CLASS, ex.getClass().getName());
      errorMetadata.add(
          ErrorInfo.ROOT_ERROR_CLASS, SolrException.getRootCause(ex).getClass().getName());
      info.add("metadata", errorMetadata);
      if (ex instanceof ApiBag.ExceptionWithErrObject) {
        ApiBag.ExceptionWithErrObject exception = (ApiBag.ExceptionWithErrObject) ex;
        info.add("details", exception.getErrs());
      }
    }

    for (Throwable th = ex; th != null; th = th.getCause()) {
      String msg = th.getMessage();
      if (msg != null) {
        info.add("msg", msg);
        break;
      }
    }

    // For any regular code, don't include the stack trace
    if (code == 500 || code < 100) {
      // hide all stack traces, as configured
      if (!hideStackTrace(hideTrace)) {
        StringWriter sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        info.add("trace", sw.toString());
      }
      log.error("500 Exception", ex);

      // non standard codes have undefined results with various servers
      if (code < 100) {
        log.warn("invalid return code: {}", code);
        code = 500;
      }
    }

    info.add("code", code);
    return code;
  }

  /**
   * Adds information about the given Throwable to a returned {@link ErrorInfo}
   *
   * <p>Primarily used by v2 API code, which can handle such typed information.
   *
   * <p>Status codes less than 100 are adjusted to be 500.
   *
   * @see #getErrorInfo(Throwable, NamedList, Logger)
   */
  public static ErrorInfo getTypedErrorInfo(Throwable ex, Logger log) {
    return getTypedErrorInfo(ex, log, false);
  }

  /**
   * Adds information about the given Throwable to a returned {@link ErrorInfo}
   *
   * <p>Primarily used by v2 API code, which can handle such typed information.
   *
   * <p>Status codes less than 100 are adjusted to be 500.
   *
   * <p>Stack trace will not be output if hideTrace=true OR system property
   * solr.hideStackTrace=true.
   *
   * @see #getErrorInfo(Throwable, NamedList, Logger)
   */
  public static ErrorInfo getTypedErrorInfo(Throwable ex, Logger log, boolean hideTrace) {
    final ErrorInfo errorInfo = new ErrorInfo();
    int code = 500;
    if (ex instanceof SolrException) {
      SolrException solrExc = (SolrException) ex;
      code = solrExc.code();
      errorInfo.metadata = new ErrorInfo.ErrorMetadata();
      errorInfo.metadata.errorClass = ex.getClass().getName();
      errorInfo.metadata.rootErrorClass = SolrException.getRootCause(ex).getClass().getName();
      if (ex instanceof ApiBag.ExceptionWithErrObject) {
        ApiBag.ExceptionWithErrObject exception = (ApiBag.ExceptionWithErrObject) ex;
        errorInfo.details = exception.getErrs();
      }
    }

    for (Throwable th = ex; th != null; th = th.getCause()) {
      String msg = th.getMessage();
      if (msg != null) {
        errorInfo.msg = msg;
        break;
      }
    }

    // For any regular code, don't include the stack trace
    if (code == 500 || code < 100) {
      if (!hideStackTrace(hideTrace)) {
        StringWriter sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        errorInfo.trace = sw.toString();
      }
      log.error("500 Exception", ex);

      // non standard codes have undefined results with various servers
      if (code < 100) {
        log.warn("invalid return code: {}", code);
        code = 500;
      }
    }

    errorInfo.code = code;
    return errorInfo;
  }

  private static boolean hideStackTrace(final boolean hideTrace) {
    return hideTrace || SYSTEM_HIDE_STACK_TRACES;
  }
}
