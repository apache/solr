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

  private static CoreAdminHandler admin = null;

  @BeforeClass
  public static void beforeClass() throws Exception {

    setupNoCoreTest(createTempDir(), null);

    admin = new CoreAdminHandler(h.getCoreContainer());

    admin.registerCustomActions(
        Map.of(
            "action1",
            new CoreAdminHandlerTestAction1(),
            "action2",
            new CoreAdminHandlerTestAction2()));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAction1() throws Exception {

    SolrQueryResponse response = new SolrQueryResponse();

    admin.handleRequestBody(req(CoreAdminParams.ACTION, "action1"), response);

    Map<String, Object> actionResponse = ((NamedList<Object>) response.getResponse()).asMap();

    assertTrue(
        String.format(
            "Action response should contain %s property",
            CoreAdminHandlerTestAction1.PROPERTY_NAME),
        actionResponse.containsKey(CoreAdminHandlerTestAction1.PROPERTY_NAME));
    assertEquals(
        String.format(
            "Action response should contain %s value for %s property",
            CoreAdminHandlerTestAction1.PROPERTY_VALUE, CoreAdminHandlerTestAction1.PROPERTY_NAME),
        CoreAdminHandlerTestAction1.PROPERTY_VALUE,
        actionResponse.get(CoreAdminHandlerTestAction1.PROPERTY_NAME));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAction2() throws Exception {

    SolrQueryResponse response = new SolrQueryResponse();

    admin.handleRequestBody(req(CoreAdminParams.ACTION, "action2"), response);

    Map<String, Object> actionResponse = ((NamedList<Object>) response.getResponse()).asMap();

    assertTrue(
        String.format(
            "Action response should contain %s property",
            CoreAdminHandlerTestAction2.PROPERTY_NAME),
        actionResponse.containsKey(CoreAdminHandlerTestAction2.PROPERTY_NAME));
    assertEquals(
        String.format(
            "Action response should contain %s value for %s property",
            CoreAdminHandlerTestAction2.PROPERTY_VALUE, CoreAdminHandlerTestAction2.PROPERTY_NAME),
        CoreAdminHandlerTestAction2.PROPERTY_VALUE,
        actionResponse.get(CoreAdminHandlerTestAction2.PROPERTY_NAME));
  }

  @Test
  public void testUnregisteredAction() throws Exception {

    SolrQueryResponse response = new SolrQueryResponse();

    assertThrows(
        "Attempt to execute unregistered action should throw SolrException",
        SolrException.class,
        () -> admin.handleRequestBody(req(CoreAdminParams.ACTION, "action3"), response));
  }

  public static String randomString() {

    int leftBound = 97; // a
    int rightBound = 122; // z
    int length = random().nextInt(6) + 5;

    String randomString =
        random()
            .ints(leftBound, rightBound + 1)
            .limit(length)
            .collect(
                () -> new StringBuilder(),
                (item, value) -> item.appendCodePoint(value),
                (item1, item2) -> item1.append(item2))
            .toString();

    return randomString;
  }

  public static class CoreAdminHandlerTestAction1 implements CoreAdminOp {

    public static final String PROPERTY_NAME = randomString();
    public static final String PROPERTY_VALUE = randomString();

    @Override
    public void execute(CallInfo it) throws Exception {

      NamedList<Object> details = new SimpleOrderedMap<>();

      details.add(PROPERTY_NAME, PROPERTY_VALUE);

      it.rsp.addResponse(details);
    }
  }

  public static class CoreAdminHandlerTestAction2 implements CoreAdminOp {

    public static final String PROPERTY_NAME = randomString();
    public static final String PROPERTY_VALUE = randomString();

    @Override
    public void execute(CallInfo it) throws Exception {

      NamedList<Object> details = new SimpleOrderedMap<>();

      details.add(PROPERTY_NAME, PROPERTY_VALUE);

      it.rsp.addResponse(details);
    }
  }
}
