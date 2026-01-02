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
package org.apache.solr.bootstrap;

import jakarta.servlet.DispatcherType;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletContextEvent;
import java.lang.invoke.MethodHandles;
import java.util.EnumSet;
import java.util.Properties;
import org.apache.solr.servlet.CoreContainerProvider;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.util.configuration.SSLConfigurationsFactory;
import org.eclipse.jetty.ee10.servlet.FilterHolder;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.ee10.servlet.Source;
import org.eclipse.jetty.rewrite.handler.HeaderPatternRule;
import org.eclipse.jetty.rewrite.handler.RedirectRegexRule;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.rewrite.handler.RewritePatternRule;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.InetAccessHandler;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.util.StringUtil;
import org.eclipse.jetty.util.resource.ResourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the complete handler chain for Solr, including WebAppContext, security headers, URL
 * rewrites, IP access control, GZIP compression, and graceful shutdown. Mirrors the configuration
 * from jetty.xml and related files.
 */
public class HandlerChainBuilder {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final ServerConfiguration config;

  public HandlerChainBuilder(ServerConfiguration config) {
    this.config = config;
  }

  /**
   * Build the complete handler chain.
   *
   * @param server the Jetty server
   * @return the root Handler
   */
  public Handler buildHandlerChain(Server server) throws Exception {
    // Start with the webapp context
    Handler handler = createWebAppContext(server);

    // Wrap with GZIP handler
    if (config.isGzipEnabled()) {
      handler = wrapWithGzipHandler(handler);
    }

    // Wrap with IP access handler if configured
    if (config.hasIpAccessRules()) {
      handler = wrapWithIpAccessHandler(handler);
    }

    // Wrap with rewrite handler (security headers + URL rewrites)
    handler = wrapWithRewriteHandler(handler);

    return handler;
  }

  /**
   * Create ServletContextHandler for Solr webapp, configured programmatically without web.xml. Adds
   * CoreContainerProvider and SolrDispatchFilter directly via Java code, following JettySolrRunner
   * pattern.
   *
   * @param server the Jetty server
   * @return the ServletContextHandler
   */
  private ServletContextHandler createWebAppContext(Server server) throws Exception {
    String jettyBase = config.getJettyHome();
    if (jettyBase == null) {
      throw new IllegalStateException("jetty.home system property is required");
    }

    String contextPath = "/solr";

    // Use ServletContextHandler like JettySolrRunner (not WebAppContext)
    ServletContextHandler root =
        new ServletContextHandler(contextPath, ServletContextHandler.SESSIONS);
    root.setServer(server);

    // Set resource base to webapp directory for serving static files
    String webappDir = jettyBase + "/solr-webapp/webapp";
    root.setBaseResource(ResourceFactory.of(server).newResource(webappDir));

    // Configure welcome files (like web.xml <welcome-file-list>)
    root.setWelcomeFiles(new String[] {"index.html"});

    // Add CoreContainerProvider listener FIRST (like JettySolrRunner)
    root.addEventListener(
        new CoreContainerProvider() {
          @Override
          public void contextInitialized(ServletContextEvent event) {
            ServletContext ctx = event.getServletContext();

            // Set up node properties
            Properties nodeProperties = new Properties();
            // Port will be available after server starts
            try {
              int actualPort = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
              nodeProperties.setProperty("hostPort", String.valueOf(actualPort));
            } catch (Exception e) {
              log.warn("Could not determine actual port, using configured port", e);
              nodeProperties.setProperty("hostPort", String.valueOf(config.getPort()));
            }

            ctx.setAttribute(SolrDispatchFilter.PROPERTIES_ATTRIBUTE, nodeProperties);
            ctx.setAttribute(SolrDispatchFilter.SOLRHOME_ATTRIBUTE, config.getSolrHome());

            // Initialize SSL configurations (normally in jetty-ssl.xml)
            SSLConfigurationsFactory.current().init();

            log.info("Jetty properties: {}", nodeProperties);

            super.contextInitialized(event);
          }
        });

    // Add SolrDispatchFilter AFTER listener (like JettySolrRunner)
    FilterHolder dispatchFilter = root.getServletHandler().newFilterHolder(Source.EMBEDDED);
    dispatchFilter.setHeldClass(SolrDispatchFilter.class);
    dispatchFilter.setInitParameter(
        "excludePatterns", "/partials/.+,/libs/.+,/css/.+,/js/.+,/img/.+,/templates/.+,/ui/.*");
    root.addFilter(dispatchFilter, "/*", EnumSet.of(DispatcherType.REQUEST));

    // Add LoadAdminUI servlet for serving the Admin UI
    ServletHolder loadAdminUI = root.getServletHandler().newServletHolder(Source.EMBEDDED);
    loadAdminUI.setHeldClass(org.apache.solr.servlet.LoadAdminUiServlet.class);
    loadAdminUI.setName("LoadAdminUI");
    root.addServlet(loadAdminUI, "/index.html");

    // Add DefaultServlet to serve static files (CSS, JS, images, etc.)
    ServletHolder defaultServlet = root.getServletHandler().newServletHolder(Source.EMBEDDED);
    defaultServlet.setHeldClass(org.eclipse.jetty.ee10.servlet.DefaultServlet.class);
    defaultServlet.setName("default");
    defaultServlet.setInitParameter("dirAllowed", "false");
    defaultServlet.setInitParameter("redirectWelcome", "false");
    root.addServlet(defaultServlet, "/");

    log.info("Created ServletContextHandler at {} with webapp at {}", contextPath, webappDir);

    return root;
  }

  /**
   * Wrap handler with RewriteHandler for security headers and URL rewrites. Mirrors jetty.xml
   * RewriteHandler configuration.
   *
   * @param handler the handler to wrap
   * @return RewriteHandler
   */
  private RewriteHandler wrapWithRewriteHandler(Handler handler) {
    RewriteHandler rewrite = new RewriteHandler();
    rewrite.setOriginalPathAttribute("requestedPath");
    rewrite.setHandler(handler);

    // Security headers (from jetty.xml)
    addSecurityHeaders(rewrite);

    // Root redirect: / -> /solr/
    RedirectRegexRule rootRedirect = new RedirectRegexRule();
    rootRedirect.setRegex("^/$");
    rootRedirect.setLocation("/solr/");
    rewrite.addRule(rootRedirect);

    // V2 API rewrites: /v2/* -> /solr/____v2 and /api/* -> /solr/____v2
    RewritePatternRule v2Rule = new RewritePatternRule();
    v2Rule.setPattern("/v2/*");
    v2Rule.setReplacement("/solr/____v2");
    rewrite.addRule(v2Rule);

    RewritePatternRule apiRule = new RewritePatternRule();
    apiRule.setPattern("/api/*");
    apiRule.setReplacement("/solr/____v2");
    rewrite.addRule(apiRule);

    log.info("Created RewriteHandler with security headers and URL rewrites");

    return rewrite;
  }

  /**
   * Add security headers to RewriteHandler.
   *
   * @param rewrite the RewriteHandler to add rules to
   */
  private void addSecurityHeaders(RewriteHandler rewrite) {
    // Content-Security-Policy
    HeaderPatternRule csp = new HeaderPatternRule();
    csp.setPattern("/solr/*");
    csp.setHeaderName("Content-Security-Policy");
    csp.setHeaderValue(
        "default-src 'none'; base-uri 'none'; connect-src 'self'; form-action 'self'; "
            + "font-src 'self'; frame-ancestors 'none'; img-src 'self' data:; media-src 'self'; "
            + "style-src 'self' 'unsafe-inline'; script-src 'self'; worker-src 'self';");
    rewrite.addRule(csp);

    // X-Content-Type-Options
    HeaderPatternRule xcto = new HeaderPatternRule();
    xcto.setPattern("/solr/*");
    xcto.setHeaderName("X-Content-Type-Options");
    xcto.setHeaderValue("nosniff");
    rewrite.addRule(xcto);

    // X-Frame-Options
    HeaderPatternRule xfo = new HeaderPatternRule();
    xfo.setPattern("/solr/*");
    xfo.setHeaderName("X-Frame-Options");
    xfo.setHeaderValue("SAMEORIGIN");
    rewrite.addRule(xfo);

    // X-XSS-Protection
    HeaderPatternRule xxp = new HeaderPatternRule();
    xxp.setPattern("/solr/*");
    xxp.setHeaderName("X-XSS-Protection");
    xxp.setHeaderValue("1; mode=block");
    rewrite.addRule(xxp);
  }

  /**
   * Wrap handler with InetAccessHandler for IP-based access control.
   *
   * @param handler the handler to wrap
   * @return InetAccessHandler
   */
  private InetAccessHandler wrapWithIpAccessHandler(Handler handler) {
    InetAccessHandler ipHandler = new InetAccessHandler();
    ipHandler.setHandler(handler);

    // Parse includes and excludes
    String includes = config.getIpAccessIncludes();
    if (!includes.isEmpty()) {
      String[] patterns = StringUtil.csvSplit(includes);
      for (String pattern : patterns) {
        if (!pattern.trim().isEmpty()) {
          ipHandler.include(pattern.trim());
        }
      }
    }

    String excludes = config.getIpAccessExcludes();
    if (!excludes.isEmpty()) {
      String[] patterns = StringUtil.csvSplit(excludes);
      for (String pattern : patterns) {
        if (!pattern.trim().isEmpty()) {
          ipHandler.exclude(pattern.trim());
        }
      }
    }

    log.info("Created InetAccessHandler with includes='{}' and excludes='{}'", includes, excludes);

    return ipHandler;
  }

  /**
   * Wrap handler with GzipHandler for compression.
   *
   * @param handler the handler to wrap
   * @return GzipHandler
   */
  private GzipHandler wrapWithGzipHandler(Handler handler) {
    GzipHandler gzip = new GzipHandler();
    gzip.setHandler(handler);
    gzip.setMinGzipSize(config.getGzipMinSize());

    // Set included methods
    String[] methods = config.getGzipIncludedMethods().split(",");
    for (int i = 0; i < methods.length; i++) {
      methods[i] = methods[i].trim();
    }
    gzip.setIncludedMethods(methods);

    if (log.isInfoEnabled()) {
      log.info(
          "Created GzipHandler with minSize={} and methods={}",
          config.getGzipMinSize(),
          config.getGzipIncludedMethods());
    }

    return gzip;
  }
}
