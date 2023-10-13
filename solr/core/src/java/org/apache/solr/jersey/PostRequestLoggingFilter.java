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

package org.apache.solr.jersey;

import static org.apache.solr.common.params.CommonParams.LOG_PARAMS_LIST;
import static org.apache.solr.jersey.MessageBodyReaders.CachingDelegatingMessageBodyReader.DESERIALIZED_REQUEST_BODY_KEY;
import static org.apache.solr.jersey.PostRequestLoggingFilter.PRIORITY;
import static org.apache.solr.jersey.RequestContextKeys.SOLR_QUERY_REQUEST;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Priority;
import javax.ws.rs.Path;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.servlet.HttpSolrCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MarkerFactory;

@Priority(PRIORITY)
public class PostRequestLoggingFilter implements ContainerResponseFilter {

  // Ensures that this filter runs AFTER response decoration, so that we can assume
  // QTime, etc. have been populated on the response.
  public static final int PRIORITY = PostRequestDecorationFilter.PRIORITY / 2;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // Use SolrCore and HttpSolrCall request loggers to maintain compatibility with logging dashboards
  // built for v1 APIs
  private static final Logger coreRequestLogger =
      LoggerFactory.getLogger(SolrCore.class.getName() + ".Request");
  private static final Logger slowCoreRequestLogger =
      LoggerFactory.getLogger(SolrCore.class.getName() + ".SlowRequest");
  private static final Logger nonCoreRequestLogger =
      LoggerFactory.getLogger(HttpSolrCall.class.getName());

  @Context private ResourceInfo resourceInfo;

  @Override
  public void filter(
      ContainerRequestContext requestContext, ContainerResponseContext responseContext)
      throws IOException {
    if (requestContext.getPropertyNames().contains(RequestContextKeys.NOT_FOUND_FLAG)) {
      return;
    }
    if (!responseContext.hasEntity()
        || !SolrJerseyResponse.class.isInstance(responseContext.getEntity())) {
      log.debug("Skipping v2 API logging because response is of an unexpected type");
      return;
    }
    final SolrJerseyResponse response = (SolrJerseyResponse) responseContext.getEntity();
    final SolrQueryRequest solrQueryRequest =
        (SolrQueryRequest) requestContext.getProperty(SOLR_QUERY_REQUEST);
    final var solrConfig =
        (solrQueryRequest.getCore() != null) ? solrQueryRequest.getCore().getSolrConfig() : null;

    final Logger requestLogger = (solrConfig != null) ? coreRequestLogger : nonCoreRequestLogger;
    final String templatedPath =
        buildTemplatedPath(requestContext.getUriInfo().getAbsolutePath().getPath());
    final String bodyVal = buildRequestBodyString(requestContext);
    requestLogger.info(
        MarkerFactory.getMarker(templatedPath),
        "method={} path={} query-params={{}} entity={} status={} QTime={}",
        requestContext.getMethod(),
        templatedPath,
        filterAndStringifyQueryParameters(requestContext.getUriInfo().getQueryParameters()),
        bodyVal,
        response.responseHeader.status,
        response.responseHeader.qTime);

    /* slowQueryThresholdMillis defaults to -1 in SolrConfig -- not enabled.*/
    if (slowCoreRequestLogger.isWarnEnabled()
        && solrConfig != null
        && solrConfig.slowQueryThresholdMillis >= 0
        && response.responseHeader.qTime >= solrConfig.slowQueryThresholdMillis) {
      slowCoreRequestLogger.warn(
          MarkerFactory.getMarker(templatedPath),
          "method={} path={} query-params={{}} entity={} status={} QTime={}",
          requestContext.getMethod(),
          templatedPath,
          filterAndStringifyQueryParameters(requestContext.getUriInfo().getQueryParameters()),
          response.responseHeader.status,
          response.responseHeader.qTime);
    }
  }

  private String buildTemplatedPath(String fallbackPath) {
    // We won't have a resource class or method in the case of a 404, so don't try to template out
    // the path-variables
    if (resourceInfo == null
        || resourceInfo.getResourceClass() == null
        || resourceInfo.getResourceMethod() == null) {
      return fallbackPath;
    }

    final var classPathAnnotation = resourceInfo.getResourceClass().getAnnotation(Path.class);
    final var classPathAnnotationVal =
        (classPathAnnotation != null) ? classPathAnnotation.value() : "";
    final var methodPathAnnotation = resourceInfo.getResourceMethod().getAnnotation(Path.class);
    final var methodPathAnnotationVal =
        (methodPathAnnotation != null) ? methodPathAnnotation.value() : "";

    return String.format(Locale.ROOT, "%s%s", classPathAnnotationVal, methodPathAnnotationVal)
        .replaceAll("//", "/");
  }

  public static String buildRequestBodyString(ContainerRequestContext requestContext) {
    if (requestContext.getProperty(DESERIALIZED_REQUEST_BODY_KEY) == null) {
      return "{}";
    }

    final Object deserializedBody = requestContext.getProperty(DESERIALIZED_REQUEST_BODY_KEY);
    if (deserializedBody instanceof JacksonReflectMapWriter) {
      return ((JacksonReflectMapWriter) requestContext.getProperty(DESERIALIZED_REQUEST_BODY_KEY))
          .jsonStr()
          .replace("\n", "");
    }

    final Object reflectWritable = Utils.getReflectWriter(deserializedBody);
    if (reflectWritable instanceof Utils.DelegateReflectWriter) {
      return Utils.toJSONString(reflectWritable).replaceAll("\n", "");
    }

    log.warn(
        "No reflection data found for request-body type {} for request {}; omitting request-body details from logging",
        deserializedBody.getClass().getName(),
        requestContext.getUriInfo().getPath());
    return "{}";
  }

  public static String filterAndStringifyQueryParameters(
      MultivaluedMap<String, String> unfilteredParams) {
    final var paramNamesToLog = getParamNamesToLog(unfilteredParams);
    final StringBuilder sb = new StringBuilder(128);
    unfilteredParams.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEachOrdered(
            entry -> {
              final String name = entry.getKey();
              if (!paramNamesToLog.contains(name)) return;

              for (String val : entry.getValue()) {
                if (sb.length() != 0) sb.append('&');
                StrUtils.partialURLEncodeVal(sb, name);
                sb.append('=');
                StrUtils.partialURLEncodeVal(sb, val);
              }
            });
    return sb.toString();
  }

  private static Set<String> getParamNamesToLog(MultivaluedMap<String, String> queryParameters) {
    if (CollectionUtil.isEmpty(queryParameters.get(LOG_PARAMS_LIST))) {
      return queryParameters.keySet();
    }

    final var paramsToLogStr = queryParameters.getFirst(LOG_PARAMS_LIST);
    if (StrUtils.isBlank(paramsToLogStr)) {
      return new HashSet<>(); // A value-less param means that no parameters should be logged
    }

    return Arrays.stream(paramsToLogStr.split(",")).collect(Collectors.toSet());
  }
}
