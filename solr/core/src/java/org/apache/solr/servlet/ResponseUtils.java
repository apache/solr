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
import java.util.HashMap;
import java.util.Map;
import org.apache.solr.api.ApiBag;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.jersey.ErrorInfo;
import org.slf4j.Logger;

/** Response helper methods. */
public class ResponseUtils {
  private ResponseUtils() {}

  public static int getErrorInfo(Throwable ex, NamedList<Object> info, Logger log) {
    final ErrorInfo errorInfo = getTypedErrorInfo(ex, log);

    Map<String, Object> errorInfoMap = new HashMap<>();
    errorInfoMap = errorInfo.toMap(errorInfoMap);
    for (Map.Entry<String, Object> entry : errorInfoMap.entrySet()) {
      info.add(entry.getKey(), entry.getValue());
    }

    return errorInfo.code;
  }

  /**
   * Adds the given Throwable's message to the given NamedList.
   *
   * <p>If the response code is not a regular code, the Throwable's stack trace is both logged and
   * added to the given NamedList.
   *
   * <p>Status codes less than 100 are adjusted to be 500.
   */
  public static ErrorInfo getTypedErrorInfo(Throwable ex, Logger log) {
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
      StringWriter sw = new StringWriter();
      ex.printStackTrace(new PrintWriter(sw));
      SolrException.log(log, ex);
      errorInfo.trace = sw.toString();

      // non standard codes have undefined results with various servers
      if (code < 100) {
        log.warn("invalid return code: {}", code);
        code = 500;
      }
    }

    errorInfo.code = code;
    return errorInfo;
  }
}
