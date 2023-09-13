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
package org.apache.solr.handler.admin;

import java.util.Locale;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.admin.CoreAdminHandler.CallInfo;
import org.apache.solr.handler.admin.CoreAdminHandler.CoreAdminOp;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

public class CoreAdminHandlerActionTest extends SolrTestCaseJ4 {

  private static final String SOLR_XML =
      "<solr><coreAdminHandlerActions>"
          + "<str name='action1'>"
          + CoreAdminHandlerTestAction1.class.getName()
          + "</str>"
          + "<str name='action2'>"
          + CoreAdminHandlerTestAction2.class.getName()
          + "</str>"
          + "</coreAdminHandlerActions></solr>";

  private static CoreAdminHandler admin = null;

  @BeforeClass
  public static void beforeClass() throws Exception {

    setupNoCoreTest(createTempDir(), SOLR_XML);
    admin = h.getCoreContainer().getMultiCoreHandler();
  }

  @Test
  public void testRegisteredAction() throws Exception {

    testAction(
        "action1",
        CoreAdminHandlerTestAction1.PROPERTY_NAME,
        CoreAdminHandlerTestAction1.PROPERTY_VALUE);
    testAction(
        "action2",
        CoreAdminHandlerTestAction2.PROPERTY_NAME,
        CoreAdminHandlerTestAction2.PROPERTY_VALUE);
  }

  @Test
  public void testUnregisteredAction() throws Exception {

    SolrQueryResponse response = new SolrQueryResponse();

    assertThrows(
        "Attempt to execute unregistered action should throw SolrException",
        SolrException.class,
        () -> admin.handleRequestBody(req(CoreAdminParams.ACTION, "action3"), response));
  }

  @SuppressWarnings("unchecked")
  private void testAction(String action, String propertyName, String propertyValue)
      throws Exception {

    SolrQueryResponse response = new SolrQueryResponse();

    admin.handleRequestBody(req(CoreAdminParams.ACTION, action), response);

    Map<String, Object> actionResponse = ((NamedList<Object>) response.getResponse()).asMap();

    assertTrue(
        String.format(Locale.ROOT, "Action response should contain %s property", propertyName),
        actionResponse.containsKey(propertyName));
    assertEquals(
        String.format(
            Locale.ROOT,
            "Action response should contain %s value for %s property",
            propertyValue,
            propertyName),
        propertyValue,
        actionResponse.get(propertyName));
  }

  public static class CoreAdminHandlerTestAction1 implements CoreAdminOp {

    public static final String PROPERTY_NAME = "property" + random().nextInt();
    public static final String PROPERTY_VALUE = "value" + random().nextInt();

    @Override
    public void execute(CallInfo it) throws Exception {

      NamedList<Object> details = new SimpleOrderedMap<>();

      details.add(PROPERTY_NAME, PROPERTY_VALUE);

      it.rsp.addResponse(details);
    }
  }

  public static class CoreAdminHandlerTestAction2 implements CoreAdminOp {

    public static final String PROPERTY_NAME = "property" + random().nextInt();
    public static final String PROPERTY_VALUE = "value" + random().nextInt();

    @Override
    public void execute(CallInfo it) throws Exception {

      NamedList<Object> details = new SimpleOrderedMap<>();

      details.add(PROPERTY_NAME, PROPERTY_VALUE);

      it.rsp.addResponse(details);
    }
  }
}
