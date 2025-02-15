/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.security;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.RELOAD;
import static org.apache.solr.servlet.HttpSolrCall.shouldAudit;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.HttpStatus;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthorizationUtils {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private AuthorizationUtils() {
    /* Private ctor prevents instantiation */
  }

  public static class AuthorizationFailure {
    private final int statusCode;
    private final String message;

    public AuthorizationFailure(int statusCode, String message) {
      this.statusCode = statusCode;
      this.message = message;
    }

    public int getStatusCode() {
      return statusCode;
    }

    public String getMessage() {
      return message;
    }
  }

  public static AuthorizationFailure authorize(
      HttpServletRequest servletReq,
      HttpServletResponse response,
      CoreContainer cores,
      AuthorizationContext context)
      throws IOException {
    log.debug("AuthorizationContext : {}", context);
    final AuthorizationPlugin authzPlugin = cores.getAuthorizationPlugin();
    if (authzPlugin == null) {
      return null; // A 'null' failure retval indicates success
    }
    AuthorizationResponse authResponse = authzPlugin.authorize(context);
    int statusCode = authResponse.statusCode;

    if (statusCode == AuthorizationResponse.PROMPT.statusCode) {
      @SuppressWarnings({"unchecked"})
      Map<String, String> headers =
          (Map<String, String>) servletReq.getAttribute(AuthenticationPlugin.class.getName());
      if (headers != null) {
        for (Map.Entry<String, String> e : headers.entrySet())
          response.setHeader(e.getKey(), e.getValue());
      }
      if (log.isDebugEnabled()) {
        log.debug(
            "USER_REQUIRED {} {}",
            servletReq.getHeader("Authorization"),
            servletReq.getUserPrincipal());
      }
      if (shouldAudit(cores, AuditEvent.EventType.REJECTED)) {
        cores
            .getAuditLoggerPlugin()
            .doAudit(new AuditEvent(AuditEvent.EventType.REJECTED, servletReq, context));
      }
      return new AuthorizationFailure(
          statusCode, "Authentication failed, Response code: " + statusCode);
    }
    if (statusCode == AuthorizationResponse.FORBIDDEN.statusCode) {
      if (log.isDebugEnabled()) {
        log.debug(
            "UNAUTHORIZED auth header {} context : {}, msg: {}",
            servletReq.getHeader("Authorization"),
            context,
            authResponse.getMessage()); // nowarn
      }
      if (shouldAudit(cores, AuditEvent.EventType.UNAUTHORIZED)) {
        cores
            .getAuditLoggerPlugin()
            .doAudit(new AuditEvent(AuditEvent.EventType.UNAUTHORIZED, servletReq, context));
      }
      return new AuthorizationFailure(
          statusCode, "Unauthorized request, Response code: " + statusCode);
    }
    if (!(statusCode == HttpStatus.SC_ACCEPTED) && !(statusCode == HttpStatus.SC_OK)) {
      log.warn(
          "ERROR {} during authentication: {}", statusCode, authResponse.getMessage()); // nowarn
      if (shouldAudit(cores, AuditEvent.EventType.ERROR)) {
        cores
            .getAuditLoggerPlugin()
            .doAudit(new AuditEvent(AuditEvent.EventType.ERROR, servletReq, context));
      }
      return new AuthorizationFailure(
          statusCode, "ERROR during authorization, Response code: " + statusCode);
    }

    // No failures! Audit if necessary and return
    if (shouldAudit(cores, AuditEvent.EventType.AUTHORIZED)) {
      cores
          .getAuditLoggerPlugin()
          .doAudit(new AuditEvent(AuditEvent.EventType.AUTHORIZED, servletReq, context));
    }
    return null;
  }

  public static List<AuthorizationContext.CollectionRequest> getCollectionRequests(
      String path, List<String> collectionNames, SolrParams params) {
    final ArrayList<AuthorizationContext.CollectionRequest> collectionRequests = new ArrayList<>();
    if (collectionNames != null) {
      for (String collection : collectionNames) {
        collectionRequests.add(new AuthorizationContext.CollectionRequest(collection));
      }
    }

    // Extract collection name from the params in case of a Collection Admin request
    if (path.equals("/admin/collections")) {
      if (CREATE.isEqual(params.get("action"))
          || RELOAD.isEqual(params.get("action"))
          || DELETE.isEqual(params.get("action")))
        collectionRequests.add(new AuthorizationContext.CollectionRequest(params.get("name")));
      else if (params.get(COLLECTION_PROP) != null)
        collectionRequests.add(
            new AuthorizationContext.CollectionRequest(params.get(COLLECTION_PROP)));
    }

    return collectionRequests;
  }
}
