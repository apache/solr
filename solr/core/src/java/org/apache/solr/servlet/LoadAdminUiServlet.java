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

import com.google.common.net.HttpHeaders;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;

/**
 * A simple servlet to load the Solr Admin UI
 *
 * @since solr 4.0
 */
public final class LoadAdminUiServlet extends BaseSolrServlet {

  // check system properties for whether or not admin UI is disabled, default is false
  private static final boolean disabled =
      Boolean.parseBoolean(System.getProperty("disableAdminUI", "false"));
  // list of comma separated URLs to inject into the CSP connect-src directive
  public static final String SYSPROP_CSP_CONNECT_SRC_URLS = "solr.ui.headers.csp.connect-src.urls";

  @Override
  public void doGet(HttpServletRequest _request, HttpServletResponse _response) throws IOException {
    if (disabled) {
      _response.sendError(
          404,
          "Solr Admin UI is disabled. To enable it, change the default value of SOLR_ADMIN_UI_"
              + "ENABLED in bin/solr.in.sh or solr.in.cmd.");
      return;
    }
    HttpServletRequest request = ServletUtils.closeShield(_request, false);
    HttpServletResponse response = ServletUtils.closeShield(_response, false);

    response.addHeader(
        "X-Frame-Options", "DENY"); // security: SOLR-7966 - avoid clickjacking for admin interface

    // This attribute is set by the SolrDispatchFilter
    String admin = request.getRequestURI().substring(request.getContextPath().length());
    CoreContainer cores = (CoreContainer) request.getAttribute("org.apache.solr.CoreContainer");
    try (InputStream in = getServletContext().getResourceAsStream(admin)) {
      if (in != null && cores != null) {
        response.setCharacterEncoding("UTF-8");
        response.setContentType("text/html");
        String connectSrc = generateCspConnectSrc();
        response.setHeader(
            HttpHeaders.CONTENT_SECURITY_POLICY,
            "default-src 'none'; base-uri 'none'; connect-src "
                + connectSrc
                + "; form-action 'self'; font-src 'self'; frame-ancestors 'none'; img-src 'self' data:; media-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self'; worker-src 'self';");

        // We have to close this to flush OutputStreamWriter buffer
        try (Writer out =
            new OutputStreamWriter(
                CloseShieldOutputStream.wrap(response.getOutputStream()), StandardCharsets.UTF_8)) {
          String html =
              new String(in.readAllBytes(), StandardCharsets.UTF_8)
                  .replace("${version}", getSolrCorePackageSpecVersion());
          out.write(html);
        }
      } else {
        response.sendError(404);
      }
    }
  }

  /**
   * Retrieves the specification version of the SolrCore package.
   *
   * @return The specification version of the SolrCore class's package or Unknown if it's
   *     unavailable.
   */
  private String getSolrCorePackageSpecVersion() {
    Package pack = SolrCore.class.getPackage();
    return pack.getSpecificationVersion() != null ? pack.getSpecificationVersion() : "Unknown";
  }

  /**
   * Fetch the value of {@link #SYSPROP_CSP_CONNECT_SRC_URLS} system property, split by comma, and
   * concatenate them into a space-separated string that can be used in CSP connect-src directive
   */
  private String generateCspConnectSrc() {
    String cspURLs = System.getProperty(SYSPROP_CSP_CONNECT_SRC_URLS, "");
    List<String> props = new ArrayList<>(Arrays.asList(cspURLs.split(",")));
    props.add("'self'");
    return String.join(" ", props);
  }
}
