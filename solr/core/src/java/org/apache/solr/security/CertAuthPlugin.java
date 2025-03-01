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
package org.apache.solr.security;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.security.cert.X509Certificate;
import java.util.Map;
import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.HttpHeaders;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.security.cert.CertPrincipalResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An authentication plugin that sets principal based on the certificate subject */
public class CertAuthPlugin extends AuthenticationPlugin {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String PARAM_PRINCIPAL_RESOLVER = "principalResolver";
  private static final String PARAM_CLASS = "class";
  private static final String PARAM_PARAMS = "params";

  private static final CertPrincipalResolver DEFAULT_PRINCIPAL_RESOLVER =
      certificate -> certificate.getSubjectX500Principal();
  protected final CoreContainer coreContainer;
  private CertPrincipalResolver principalResolver;

  public CertAuthPlugin() {
    this(null);
  }

  public CertAuthPlugin(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }

  @Override
  public void init(Map<String, Object> pluginConfig) {
    principalResolver =
        resolveComponent(
            pluginConfig,
            PARAM_PRINCIPAL_RESOLVER,
            CertPrincipalResolver.class,
            DEFAULT_PRINCIPAL_RESOLVER,
            "principalResolver");
  }

  @SuppressWarnings("unchecked")
  private <T> T resolveComponent(
      Map<String, Object> pluginConfig,
      String configKey,
      Class<T> clazz,
      T defaultInstance,
      String componentName) {
    Map<String, Object> configMap = (Map<String, Object>) pluginConfig.get(configKey);
    if (this.coreContainer == null) {
      log.warn("No coreContainer configured. Using the default {}", componentName);
      return defaultInstance;
    }
    if (configMap == null) {
      log.warn("No {} configured. Using the default one", componentName);
      return defaultInstance;
    }

    String className = (String) configMap.get(PARAM_CLASS);
    if (StrUtils.isNullOrEmpty(className)) {
      log.warn("No {} class configured. Using the default one", componentName);
      return defaultInstance;
    }
    Map<String, Object> params = (Map<String, Object>) configMap.get(PARAM_PARAMS);
    if (params == null) {
      log.warn("No params found for {}. Using the default class", componentName);
      return defaultInstance;
    }

    log.info("Found a {} class: {}", componentName, className);
    return this.coreContainer
        .getResourceLoader()
        .newInstance(className, clazz, null, new Class<?>[] {Map.class}, new Object[] {params});
  }

  @Override
  public boolean doAuthenticate(
      HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
      throws Exception {
    X509Certificate[] certs =
        (X509Certificate[]) request.getAttribute("javax.servlet.request.X509Certificate");
    if (certs == null || certs.length == 0) {
      return sendError(response, "require certificate");
    }

    HttpServletRequest wrapped =
        wrapWithPrincipal(request, principalResolver.resolvePrincipal(certs[0]));
    numAuthenticated.inc();
    filterChain.doFilter(wrapped, response);
    return true;
  }

  private boolean sendError(HttpServletResponse response, String msg) throws IOException {
    numMissingCredentials.inc();
    response.setHeader(HttpHeaders.WWW_AUTHENTICATE, "Certificate");
    response.sendError(HttpServletResponse.SC_UNAUTHORIZED, msg);
    return false;
  }
}
