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

import java.lang.invoke.MethodHandles;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.Log4jListAppender;
import org.apache.solr.SolrTestCaseJ4;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LoggerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import org.junit.BeforeClass;

import static org.hamcrest.core.StringContains.containsString;

@SuppressForbidden(reason="We need to use log4J2 classes directly to check that the MDC is working")
public class TestRequestId extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml","schema.xml");
  }

  // NOTE: Explicitly configuring these so we know they have a LoggerConfig we can attach an appender to...
  @LogLevel("org.apache.solr.core.SolrCore.Request=INFO;org.apache.solr.handler.RequestHandlerBase=INFO")
  public void testRequestId() {
    
    final String reqLogName = SolrCore.class.getName() + ".Request";
    final String errLogName = RequestHandlerBase.class.getName();
      
    final Log4jListAppender reqLog = new Log4jListAppender("req-log");
    final Log4jListAppender errLog = new Log4jListAppender("err-log");
    try {
      LoggerContext.getContext(false).getConfiguration().getLoggerConfig(reqLogName).addAppender(reqLog, Level.INFO, null);
      LoggerContext.getContext(false).getConfiguration().getLoggerConfig(errLogName).addAppender(errLog, Level.ERROR, null);
      LoggerContext.getContext(false).updateLoggers();
      
      // Sanity check that the our MDC doesn't already have some sort of rid set in it
      assertNull(MDC.get(CommonParams.REQUEST_ID));

      // simple request that should successfully be logged ...
      assertQ("xxx", req("q", "*:*", CommonParams.REQUEST_ID, "xxx"), "//*[@numFound='0']");

      // Sanity check that the test framework didn't let our "request" MDC info "leak" out of assertQ..
      assertNull(MDC.get(CommonParams.REQUEST_ID));
      
      assertEquals(1, reqLog.getEvents().size());
      assertEquals("xxx", reqLog.getEvents().get(0).getContextData().getValue("rid"));

      assertEquals(0, errLog.getEvents().size());

      // reques that should cause some ERROR logging...
      // NOTE: we can't just listen for errors at the 'root' logger because assertQEx will 'mute' them before appenders get them
      assertQEx("yyy", "bogus_yyy", req("q", "*:*", "sort", "bogus_yyy", CommonParams.REQUEST_ID, "yyy"), ErrorCode.BAD_REQUEST);
                
      // Sanity check that the test framework didn't let our "request" MDC info "leak" out of assertQEx..
      assertNull(MDC.get(CommonParams.REQUEST_ID));
      
      assertEquals(2, reqLog.getEvents().size());
      assertEquals("yyy", reqLog.getEvents().get(1).getContextData().getValue("rid"));
      assertThat(reqLog.getEvents().get(1).getMessage().getFormattedMessage(), containsString("status="+ErrorCode.BAD_REQUEST.code));
      
      assertEquals(1, errLog.getEvents().size());
      assertEquals("yyy", errLog.getEvents().get(0).getContextData().getValue("rid"));
      assertNotNull(errLog.getEvents().get(0).getThrown());
      

    } finally {
      LoggerContext.getContext(false).getConfiguration().getLoggerConfig(reqLogName).removeAppender(reqLog.getName());
      LoggerContext.getContext(false).getConfiguration().getLoggerConfig(errLogName).removeAppender(errLog.getName());
      LoggerContext.getContext(false).updateLoggers();
    }
  }

}
