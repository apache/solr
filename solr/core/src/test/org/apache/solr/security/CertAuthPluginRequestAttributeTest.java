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


import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import io.opentelemetry.api.common.Attributes;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.util.SolrMetricTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import javax.security.auth.x500.X500Principal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class CertAuthPluginRequestAttributeTest extends SolrTestCaseJ4 {
  private CertAuthPlugin plugin;
  private SolrMetricsContext solrMetricsContext;
  @Parameterized.Parameter(0)
  public Map<String,Object> initMap;
  @Parameterized.Parameter(1)
  public String expectedAttributeLookup;

  @ParametersFactory()
  public static Collection<Object[]> data() {
    String defaultValue = "jakarta.servlet.request.X509Certificate";
    String requestAttributeParam = "requestAttribute";
    HashMap<Object,String> mapWithNullValue=new HashMap<>();
    mapWithNullValue.put(requestAttributeParam,null);
    return Arrays.asList(new Object[][] {
        {Map.of(), defaultValue},
        {mapWithNullValue, defaultValue},
        {Map.of(requestAttributeParam, ""), defaultValue},
        {
            Map.of(requestAttributeParam, "javax.servlet.request.X509Certificate"),
            "javax.servlet.request.X509Certificate"}
    });
  }

  public CertAuthPluginRequestAttributeTest(Map<String,Object> initMap, String expectedAttributeLookup) {
    this.initMap = initMap;
    this.expectedAttributeLookup = expectedAttributeLookup;
  }

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
    solrMetricsContext = new SolrMetricsContext(coreContainer.getMetricManager(), registryName);
    plugin.initializeMetrics(solrMetricsContext, Attributes.empty());
    plugin.init(initMap);

  }

  @Override
  @After
  public void tearDown() throws Exception {
    plugin.close();
    solrMetricsContext.close();
    deleteCore();
    super.tearDown();
  }

  @Test
  public void testAuthenticateOkAndSpyRequestedAttributes() throws Exception {
    X500Principal principal = new X500Principal("CN=NAME");
    final X509Certificate certificate = mock(X509Certificate.class);
    HttpServletRequest request = mock(HttpServletRequest.class);
    final ArrayList<String> requestedAttributes=new ArrayList<>();
    when(certificate.getSubjectX500Principal()).thenReturn(principal);
    when(request.getAttribute(any())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        requestedAttributes.add(invocation.getArgument(0));
        return new X509Certificate[] {certificate};
      }
    });


    FilterChain chain =
        (req, rsp) -> assertEquals(principal, ((HttpServletRequest) req).getUserPrincipal());
    assertTrue(plugin.doAuthenticate(request, null, chain));
    assertEquals(new String[]{expectedAttributeLookup},requestedAttributes.toArray());

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

  private long getLongMetricValue(String metricName, Labels labels) {
    SolrCore core = h.getCore();
    CounterSnapshot.CounterDataPointSnapshot metric =
        SolrMetricTestUtils.getCounterDatapoint(core, metricName, labels);
    return (metric != null) ? (long) metric.getValue() : 0L;
  }
}
