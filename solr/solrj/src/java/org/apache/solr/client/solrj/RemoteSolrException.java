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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.solr.common.SolrErrorWrappingException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;

/**
 * Subclass of SolrException that allows us to capture an arbitrary HTTP status code and error
 * details that may have been returned by the remote server or a proxy along the way.
 */
public final class RemoteSolrException extends SolrException {

  private String remoteErrorMessageSuffix = "";
  private final boolean skipRetry;

  /**
   * @param remoteHost the host the error was received from
   * @param code Arbitrary HTTP status code
   * @param msg Exception Message
   * @param th Throwable to wrap with this Exception
   */
  public RemoteSolrException(String remoteHost, int code, String msg, Throwable th) {
    super(code, "Error from server at " + remoteHost + ": " + msg, th);
    skipRetry = false;
  }

  /**
   * @param remoteHost the host the error was received from
   * @param code Arbitrary HTTP status code
   * @param remoteError Error response sent back from remoteHost
   */
  @SuppressWarnings("unchecked")
  public RemoteSolrException(String remoteHost, int code, Object remoteError) {
    this(remoteHost, code, remoteError, false);
  }

  /**
   * @param remoteHost the host the error was received from
   * @param code Arbitrary HTTP status code
   * @param remoteError Error response sent back from remoteHost
   * @param skipRetry Skip retry if the client is set to retry on failure
   */
  @SuppressWarnings("unchecked")
  public RemoteSolrException(String remoteHost, int code, Object remoteError, boolean skipRetry) {
    super(ErrorCode.getErrorCode(code), "Error from server at " + remoteHost);
    this.skipRetry = skipRetry;
    setDetails(List.of(Map.of("remoteHost", remoteHost, "remoteError", remoteError)));
    if (remoteError != null) {
      String remoteErrorMessageSuffix = getRemoteErrorMessageSuffix(remoteError, "");
      if (remoteErrorMessageSuffix.length() > 2) {
        this.remoteErrorMessageSuffix = remoteErrorMessageSuffix;
      }

      Object metadataObj =
          Utils.getObjectByPath(remoteError, false, Collections.singletonList("metadata"));
      if (metadataObj instanceof NamedList) {
        setMetadata((NamedList<String>) metadataObj);
      } else if (metadataObj instanceof List) {
        // NamedList parsed as List convert to NamedList again
        List<Object> list = (List<Object>) metadataObj;
        setMetadata(new NamedList<>(list.size() / 2));
        for (int i = 0; i < list.size(); i += 2) {
          setMetadata((String) list.get(i), (String) list.get(i + 1));
        }
      } else if (metadataObj instanceof Map) {
        setMetadata(new NamedList<>((Map<String, String>) metadataObj));
      }
    }
  }

  private String getRemoteErrorMessageSuffix(Object remoteError, String startingJsonPath) {
    Object remoteErrorClass =
        Utils.getObjectByPath(remoteError, false, startingJsonPath + "/errorClass");
    Object remoteErrorMsg = Utils.getObjectByPath(remoteError, false, startingJsonPath + "/msg");
    String remoteErrorMessageSuffix = "";

    // Special cases for what the message should look like
    if (SolrErrorWrappingException.class.getName().equals(remoteErrorClass)) {
      remoteErrorMessageSuffix =
          SolrErrorWrappingException.constructMessage(
              remoteErrorMsg != null ? remoteErrorMsg.toString() : null,
              Utils.getObjectByPath(remoteError, false, "details"));
    }
    if (RemoteSolrException.class.getName().equals(remoteErrorClass)) {
      remoteErrorMessageSuffix += remoteErrorClass + ": " + remoteErrorMsg;
      // Drill down into the next remote error to get info from that
      String nextRemoteErrorMessage =
          getRemoteErrorMessageSuffix(remoteError, startingJsonPath + "/details[0]/remoteError");
      if (!nextRemoteErrorMessage.isEmpty()) {
        remoteErrorMessageSuffix += ": " + nextRemoteErrorMessage;
      }
    } else {
      if (remoteErrorClass != null) {
        remoteErrorMessageSuffix = remoteErrorMessageSuffix + remoteErrorClass + ": ";
      }
      if (remoteErrorMsg != null) {
        remoteErrorMessageSuffix = remoteErrorMessageSuffix + remoteErrorMsg;
      }
    }
    if (remoteErrorMessageSuffix.isEmpty()) {
      return "";
    } else {
      return ": " + remoteErrorMessageSuffix;
    }
  }

  @Override
  public String getResponseMessage() {
    return super.getMessage();
  }

  @Override
  public String getMessage() {
    return super.getMessage() + remoteErrorMessageSuffix;
  }

  public Object getRemoteErrorObject() {
    if (details != null && !details.isEmpty()) {
      return details.get(0).get("remoteError");
    } else {
      return null;
    }
  }

  public boolean shouldSkipRetry() {
    return skipRetry;
  }
}
