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

import java.util.List;
import java.util.Map;
import org.apache.solr.common.util.Utils;

/**
 * Subclass of SolrException that allows us to wrap arbitrary error objects, and pass them in the
 * errorMessage while preserving the error object structure when returning a response to the user.
 */
public final class SolrErrorWrappingException extends SolrException {

  public SolrErrorWrappingException(ErrorCode code, String msg, List<Map<String, Object>> errs) {
    super(code, msg);
    setDetails(errs);
  }

  @Override
  public String getResponseMessage() {
    return super.getMessage();
  }

  @Override
  public String getMessage() {
    return constructMessage(super.getMessage(), getDetails());
  }

  public static String constructMessage(String message, Object details) {
    String builtMessage = "";
    if (message != null) {
      builtMessage = message;
    } else {
      builtMessage = SolrErrorWrappingException.class.getName();
    }
    if (details != null) {
      builtMessage += ", errors: " + Utils.toJSONString(details);
    }
    return builtMessage;
  }
}
