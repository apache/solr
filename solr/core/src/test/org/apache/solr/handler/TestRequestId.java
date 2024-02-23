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

package org.apache.solr.handler;

import static org.hamcrest.core.StringContains.containsString;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.LogListener;
import org.hamcrest.MatcherAssert;
import org.junit.BeforeClass;
import org.slf4j.MDC;

@SuppressForbidden(reason = "We need to use log4J2 classes directly to test MDC impacts")
public class TestRequestId extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  public void testRequestId() {

    try (LogListener reqLog = LogListener.info(SolrCore.class.getName() + ".Request")) {

      // Check that our MDC doesn't already have some sort of rid set in it
      assertNull(MDC.get(CommonParams.REQUEST_ID));

      // simple request that should successfully be logged ...
      assertQ("xxx", req("q", "*:*", CommonParams.REQUEST_ID, "xxx"), "//*[@numFound='0']");

      // Sanity check that the test framework didn't let our "request" MDC info "leak" out of
      // assertQ...
      assertNull(MDC.get(CommonParams.REQUEST_ID));

      {
        var reqEvent = reqLog.getQueue().poll();
        assertNotNull(reqEvent);
        assertEquals("xxx", reqEvent.getContextData().getValue("rid"));
        assertTrue(reqLog.getQueue().isEmpty());
      }

      // request that should cause some ERROR logging...
      // NOTE: we can't just listen for errors at the 'root' logger because assertQEx will 'mute'
      // them before we can intercept
      try (LogListener errLog = LogListener.error(RequestHandlerBase.class)) {
        assertQEx(
            "yyy",
            "bogus_yyy",
            req("q", "*:*", "sort", "bogus_yyy", CommonParams.REQUEST_ID, "yyy"),
            ErrorCode.BAD_REQUEST);

        // Sanity check that the test framework didn't let our "request" MDC info "leak" out of
        // assertQEx...
        assertNull(MDC.get(CommonParams.REQUEST_ID));

        {
          var reqEvent = reqLog.getQueue().poll();
          assertNotNull(reqEvent);
          assertEquals("yyy", reqEvent.getContextData().getValue("rid"));
          MatcherAssert.assertThat(
              reqEvent.getMessage().getFormattedMessage(),
              containsString("status=" + ErrorCode.BAD_REQUEST.code));
        }
        {
          var errEvent = errLog.getQueue().poll();
          assertNotNull(errEvent);
          assertEquals("yyy", errEvent.getContextData().getValue("rid"));
          assertNotNull(errEvent.getThrown());
        }
      }
    }
  }
}
