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
package org.apache.solr.update.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.LogListener;
import org.junit.BeforeClass;
import org.junit.Test;

public class LogUpdateProcessorFactoryTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  private static LogUpdateProcessorFactory factoryWithThreshold(int slowUpdateThresholdMillis) {
    final LogUpdateProcessorFactory factory = new LogUpdateProcessorFactory();
    final NamedList<Object> args = new NamedList<>();
    args.add("slowUpdateThresholdMillis", slowUpdateThresholdMillis);
    factory.init(args);
    return factory;
  }

  private static SolrQueryResponse responseWithToLog() {
    final SolrQueryResponse rsp = new SolrQueryResponse();
    rsp.addToLog("webapp", "/solr");
    rsp.addToLog("path", "/update");
    return rsp;
  }

  /**
   * When the INFO summary and the "slow" WARN are both logged, they must both contain the request's
   * {@code rsp.toLog} content (SOLR-16910); previously the INFO logging cleared it first, so the
   * WARN message came out in a different format without the request details.
   */
  @Test
  public void testInfoAndSlowWarnBothContainRequestToLog() throws Exception {
    final LogUpdateProcessorFactory factory = factoryWithThreshold(0); // everything is "slow"

    try (SolrQueryRequest req = req();
        LogListener info = LogListener.info(LogUpdateProcessorFactory.class);
        LogListener warn = LogListener.warn(LogUpdateProcessorFactory.class)) {
      final SolrQueryResponse rsp = responseWithToLog();

      final UpdateRequestProcessor processor = factory.getInstance(req, rsp, null);
      processor.finish();

      final String infoMsg = info.pollMessage();
      final String warnMsg = warn.pollMessage();
      assertNotNull("expected an INFO summary", infoMsg);
      assertNotNull("expected a slow-update WARN", warnMsg);
      assertTrue(infoMsg, infoMsg.contains("webapp=/solr"));
      assertTrue(infoMsg, infoMsg.contains("path=/update"));
      assertTrue(warnMsg, warnMsg.startsWith("slow: "));
      assertTrue(warnMsg, warnMsg.contains("webapp=/solr"));
      assertTrue(warnMsg, warnMsg.contains("path=/update"));
      assertTrue(
          "toLog should be cleared so SolrCore does not log it again", rsp.getToLog().size() == 0);
    }
  }

  /**
   * With INFO disabled, the "slow" WARN alone must still contain the request's {@code rsp.toLog}
   * content, and the toLog must still be cleared afterwards (SOLR-16910).
   */
  @Test
  @LogLevel("org.apache.solr.update.processor.LogUpdateProcessorFactory=WARN")
  public void testSlowWarnOnlyContainsRequestToLog() throws Exception {
    assertFalse(
        "this test requires INFO to be disabled for the factory logger",
        LogManager.getLogger(LogUpdateProcessorFactory.class).isInfoEnabled());
    final LogUpdateProcessorFactory factory = factoryWithThreshold(0); // everything is "slow"

    try (SolrQueryRequest req = req();
        LogListener warn = LogListener.warn(LogUpdateProcessorFactory.class)) {
      final SolrQueryResponse rsp = responseWithToLog();

      final UpdateRequestProcessor processor = factory.getInstance(req, rsp, null);
      processor.finish();

      final String warnMsg = warn.pollMessage();
      assertNotNull("expected a slow-update WARN", warnMsg);
      assertTrue(warnMsg, warnMsg.startsWith("slow: "));
      assertTrue(warnMsg, warnMsg.contains("webapp=/solr"));
      assertTrue(warnMsg, warnMsg.contains("path=/update"));
      assertTrue(
          "toLog should be cleared so SolrCore does not log it again", rsp.getToLog().size() == 0);
    }
  }

  /**
   * When neither the INFO summary nor the "slow" WARN is emitted, {@code rsp.toLog} must be left in
   * place so that SolrCore's request logger can still log the request (SOLR-16910).
   */
  @Test
  @LogLevel("org.apache.solr.update.processor.LogUpdateProcessorFactory=WARN")
  public void testToLogRetainedWhenNothingIsLogged() throws Exception {
    assertFalse(
        "this test requires INFO to be disabled for the factory logger",
        LogManager.getLogger(LogUpdateProcessorFactory.class).isInfoEnabled());
    // A threshold no request can reach: the "slow" WARN is never emitted either.
    final LogUpdateProcessorFactory factory = factoryWithThreshold(Integer.MAX_VALUE);

    try (SolrQueryRequest req = req();
        LogListener warn = LogListener.warn(LogUpdateProcessorFactory.class)) {
      final SolrQueryResponse rsp = responseWithToLog();

      final UpdateRequestProcessor processor = factory.getInstance(req, rsp, null);
      processor.finish();

      assertEquals("no slow-update WARN should be emitted", 0, warn.getCount());
      assertEquals(
          "toLog should be retained for SolrCore's request logger when nothing was logged",
          2,
          rsp.getToLog().size());
      assertEquals("/solr", rsp.getToLog().get("webapp"));
      assertEquals("/update", rsp.getToLog().get("path"));
    }
  }
}
