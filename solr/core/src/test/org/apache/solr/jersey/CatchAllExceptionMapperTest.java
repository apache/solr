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

package org.apache.solr.jersey;

import static org.apache.solr.jersey.CatchAllExceptionMapper.shouldHideStackTrace;
import static org.apache.solr.jersey.RequestContextKeys.CORE_CONTAINER;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.container.ContainerRequestContext;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for {@link CatchAllExceptionMapper#shouldHideStackTrace} */
public class CatchAllExceptionMapperTest extends SolrTestCaseJ4 {

  private SolrCore core;
  private CoreContainer coreContainer;
  private SolrQueryRequest request;
  private ContainerRequestContext ctx;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void setupMocks() {
    core = mock(SolrCore.class);
    coreContainer = mock(CoreContainer.class);
    request = mock(SolrQueryRequest.class);
    ctx = mock(ContainerRequestContext.class);
  }

  @Test
  public void testShouldHideStackTrace_viaCore() {
    when(request.getCore()).thenReturn(core);
    when(core.getCoreContainer()).thenReturn(coreContainer);

    when(coreContainer.hideStackTrace()).thenReturn(true);
    assertTrue(shouldHideStackTrace(request, ctx));

    when(coreContainer.hideStackTrace()).thenReturn(false);
    assertFalse(shouldHideStackTrace(request, ctx));
  }

  /**
   * SOLR-18066: When no SolrCore is associated with the request (e.g. cluster/collection-level
   * endpoints), the CoreContainer from the request context should be consulted instead.
   */
  @Test
  public void testShouldHideStackTrace_viaCoreContainer_whenCoreIsNull() {
    when(ctx.getProperty(CORE_CONTAINER)).thenReturn(coreContainer);

    when(coreContainer.hideStackTrace()).thenReturn(true);
    assertTrue(shouldHideStackTrace(request, ctx));

    when(coreContainer.hideStackTrace()).thenReturn(false);
    assertFalse(shouldHideStackTrace(request, ctx));
  }

  @Test
  public void testShouldHideStackTrace_returnsFalse_whenNeitherCoreNorContainerAvailable() {
    assertFalse(shouldHideStackTrace(request, ctx));
  }

  @Test
  public void testShouldHideStackTrace_returnsFalse_whenRequestIsNull() {
    assertFalse(shouldHideStackTrace(null, ctx));
  }
}
