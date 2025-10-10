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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.common.Attributes;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.security.cert.X509Certificate;
import java.util.Collections;
import javax.security.auth.x500.X500Principal;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.util.SolrMetricTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CertAuthPluginTest extends SolrTestCaseJ4 {
  private CertAuthPlugin plugin;

  @BeforeClass
  public static void setupMockito() {
    SolrTestCaseJ4.assumeWorkingMockito();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    CoreContainer coreContainer =
        SolrTestCaseJ4.createCoreContainer(
            "collection1", "data", "solrconfig-basic.xml", "schema.xml");
    String registryName = "solr.core.collection1";
    plugin = new CertAuthPlugin();
    SolrMetricsContext solrMetricsContext =
        new SolrMetricsContext(coreContainer.getMetricManager(), registryName);
    plugin.initializeMetrics(solrMetricsContext, Attributes.empty());
    plugin.init(Collections.emptyMap());
  }

  @After
  public void tearDown() throws Exception {
    deleteCore();
    super.tearDown();
  }

  @Test
  public void testAuthenticateOk() throws Exception {
    X500Principal principal = new X500Principal("CN=NAME");
    X509Certificate certificate = mock(X509Certificate.class);
    HttpServletRequest request = mock(HttpServletRequest.class);

    when(certificate.getSubjectX500Principal()).thenReturn(principal);
    when(request.getAttribute(any())).thenReturn(new X509Certificate[] {certificate});

    FilterChain chain =
        (req, rsp) -> assertEquals(principal, ((HttpServletRequest) req).getUserPrincipal());
    assertTrue(plugin.doAuthenticate(request, null, chain));

    Labels labels =
        Labels.of(
            "otel_scope_name",
            "org.apache.solr",
            "category",
            "SECURITY",
            "plugin_name",
            "CertAuthPlugin");
    long missingCredentialsCount =
        getLongMetricValue("solr_authentication_num_authenticated", labels);
    assertEquals(1L, missingCredentialsCount);
  }

  @Test
  public void testAuthenticateMissing() throws Exception {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getAttribute(any())).thenReturn(null);

    HttpServletResponse response = mock(HttpServletResponse.class);

    assertFalse(plugin.doAuthenticate(request, response, null));
    verify(response).sendError(eq(401), anyString());

    Labels labels =
        Labels.of(
            "otel_scope_name",
            "org.apache.solr",
            "category",
            "SECURITY",
            "type",
            "missing_credentials",
            "plugin_name",
            "CertAuthPlugin");
    long missingCredentialsCount = getLongMetricValue("solr_authentication_failures", labels);
    assertEquals(1L, missingCredentialsCount);
  }

  private long getLongMetricValue(String metricName, Labels labels) {
    SolrCore core = h.getCore();
    CounterSnapshot.CounterDataPointSnapshot metric =
        SolrMetricTestUtils.getCounterDatapoint(core, metricName, labels);
    return (metric != null) ? (long) metric.getValue() : 0L;
  }
}
