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

import java.util.Date;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test for {@link CoreInfoHandler} */
public class CoreInfoHandlerTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testCoreInfoHandlerResponse() throws Exception {
    CoreInfoHandler handler = new CoreInfoHandler();
    SolrQueryRequest req = req();
    SolrQueryResponse rsp = new SolrQueryResponse();

    handler.handleRequestBody(req, rsp);

    NamedList<Object> values = rsp.getValues();
    assertNotNull("Response should not be null", values);

    @SuppressWarnings("unchecked")
    SimpleOrderedMap<Object> core = (SimpleOrderedMap<Object>) values.get("core");
    assertNotNull("Core info should not be null", core);

    // Verify schema name is present
    String schema = (String) core.get("schema");
    assertNotNull("Schema should not be null", schema);
    assertEquals("test", schema);

    // Verify 'now' date is present and reasonable
    Date now = (Date) core.get("now");
    assertNotNull("Now date should not be null", now);
    Date currentTime = new Date();
    long responseTime = now.getTime();
    assertTrue(
        "Response time should be close to current time",
        Math.abs(currentTime.getTime() - responseTime) < 5000); // within 5 seconds

    // Verify start time is present
    Date start = (Date) core.get("start");
    assertNotNull("Start time should not be null", start);
    assertTrue("Start time should be before or equal to now", start.getTime() <= now.getTime());

    // Verify directory info
    @SuppressWarnings("unchecked")
    SimpleOrderedMap<Object> directory = (SimpleOrderedMap<Object>) core.get("directory");
    assertNotNull("Directory info should not be null", directory);

    // Check all expected directory fields are present
    assertNotNull("cwd should not be null", directory.get("cwd"));
    assertNotNull("instance should not be null", directory.get("instance"));
    assertNotNull("data should not be null", directory.get("data"));
    assertNotNull("dirimpl should not be null", directory.get("dirimpl"));
    assertNotNull("index should not be null", directory.get("index"));

    assertFalse("HTTP caching should be disabled", rsp.isHttpCaching());

    req.close();
  }

  @Test
  public void testCoreInfoMatchesActualCore() throws Exception {
    CoreInfoHandler handler = new CoreInfoHandler();

    try (SolrQueryRequest req = req()) {
      SolrCore core = req.getCore();
      assertNotNull("Core should not be null", core);

      SolrQueryResponse rsp = new SolrQueryResponse();
      handler.handleRequestBody(req, rsp);

      @SuppressWarnings("unchecked")
      SimpleOrderedMap<Object> coreInfo = (SimpleOrderedMap<Object>) rsp.getValues().get("core");

      // Verify the schema name matches what we expect
      String schemaName = (String) coreInfo.get("schema");
      assertEquals("Schema name should match", core.getLatestSchema().getSchemaName(), schemaName);

      // Verify start time matches core's start time
      Date startTime = (Date) coreInfo.get("start");
      assertEquals(
          "Start time should match core start time",
          core.getStartTimeStamp().getTime(),
          startTime.getTime());

      // Verify instance path is correctly reported
      @SuppressWarnings("unchecked")
      SimpleOrderedMap<Object> directory = (SimpleOrderedMap<Object>) coreInfo.get("directory");
      String instancePath = (String) directory.get("instance");
      assertEquals("Instance path should match", core.getInstancePath().toString(), instancePath);
    }
  }
}
