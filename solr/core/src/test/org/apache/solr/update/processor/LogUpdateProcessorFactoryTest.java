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
 */package org.apache.solr.update.processor;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.LogListener;
import org.junit.BeforeClass;
import org.junit.Test;

public class LogUpdateProcessorFactoryTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  /**
   * When the INFO summary and the "slow" WARN are both logged, they must both contain the request's
   * {@code rsp.toLog} content (SOLR-16910); previously the INFO logging cleared it first, so the
   * WARN message came out in a different format without the request details.
   */
  @Test
  public void testInfoAndSlowWarnBothContainRequestToLog() throws Exception {
    final LogUpdateProcessorFactory factory = new LogUpdateProcessorFactory();
    final NamedList<Object> args = new NamedList<>();
    args.add("slowUpdateThresholdMillis", 0); // everything is "slow"
    factory.init(args);

    try (SolrQueryRequest req = req();
        LogListener info = LogListener.info(LogUpdateProcessorFactory.class);
        LogListener warn = LogListener.warn(LogUpdateProcessorFactory.class)) {
      final SolrQueryResponse rsp = new SolrQueryResponse();
      rsp.addToLog("webapp", "/solr");
      rsp.addToLog("path", "/update");

      final UpdateRequestProcessor processor = factory.getInstance(req, rsp, null);
      processor.finish();

      final String infoMsg = info.pollMessage();
      final String warnMsg = warn.pollMessage();
      assertNotNull("expected an INFO summary", infoMsg);
      assertNotNull("expected a slow-update WARN", warnMsg);
      assertTrue(infoMsg, infoMsg.contains("webapp=/solr"));
      assertTrue(warnMsg, warnMsg.startsWith("slow: "));
      assertTrue(warnMsg, warnMsg.contains("webapp=/solr"));
      assertTrue(warnMsg, warnMsg.contains("path=/update"));
      assertTrue(
          "toLog should be cleared so SolrCore does not log it again", rsp.getToLog().isEmpty());
    }
  }
}