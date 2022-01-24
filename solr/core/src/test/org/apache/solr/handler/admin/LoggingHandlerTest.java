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


import java.util.ArrayList;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.util.LogLevel;
import org.junit.BeforeClass;
import org.junit.Test;


@SuppressForbidden(reason = "test uses log4j2 because it tests output at a specific level")
@LogLevel("org.apache.solr.bogus_logger_package.BogusLoggerClass=DEBUG")
public class LoggingHandlerTest extends SolrTestCaseJ4 {
  private final String PARENT_LOGGER_NAME = "org.apache.solr.bogus_logger_package";
  private final String CLASS_LOGGER_NAME = PARENT_LOGGER_NAME + ".BogusLoggerClass";
  
  // TODO: This only tests Log4j at the moment, as that's what's defined
  // through the CoreContainer.

  // TODO: Would be nice to throw an exception on trying to set a
  // log level that doesn't exist
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testLogLevelHandlerOutput() throws Exception {
    
    // sanity check our setup...
    assertNotNull(this.getClass().getAnnotation(LogLevel.class));
    final String annotationConfig = this.getClass().getAnnotation(LogLevel.class).value();
    assertTrue("WTF: " + annotationConfig, annotationConfig.startsWith( PARENT_LOGGER_NAME ));
    assertTrue("WTF: " + annotationConfig, annotationConfig.startsWith( CLASS_LOGGER_NAME ));
    assertTrue("WTF: " + annotationConfig, annotationConfig.endsWith( Level.DEBUG.toString() ));
    
    assertEquals(Level.DEBUG, LogManager.getLogger( CLASS_LOGGER_NAME ).getLevel());
    
    final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    final Configuration config = ctx.getConfiguration();

    assertEquals("Unexpected config for " + PARENT_LOGGER_NAME + " ... expected 'root' config",
                 config.getRootLogger(),
                 config.getLoggerConfig(PARENT_LOGGER_NAME));
    assertEquals(Level.DEBUG, config.getLoggerConfig(CLASS_LOGGER_NAME).getLevel());

    SolrClient client = new EmbeddedSolrServer(h.getCore());
    ModifiableSolrParams mparams = new ModifiableSolrParams();

    NamedList<Object> rsp = client.request(new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/info/logging", mparams));

    @SuppressWarnings({"unchecked"})
    ArrayList<NamedList<Object>> loggers = (ArrayList<NamedList<Object>>) rsp._get("loggers", null);

    // check log levels
    assertTrue(checkLoggerLevel(loggers, PARENT_LOGGER_NAME, ""));
    assertTrue(checkLoggerLevel(loggers, CLASS_LOGGER_NAME, "DEBUG"));

    // update parent logger level
    mparams.set("set", PARENT_LOGGER_NAME + ":TRACE");
    rsp = client.request(new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/info/logging", mparams));

    @SuppressWarnings({"unchecked"})
    ArrayList<NamedList<Object>> updatedLoggerLevel = (ArrayList<NamedList<Object>>) rsp._get("loggers", null);

    // check new parent logger level
    assertTrue(checkLoggerLevel(updatedLoggerLevel, PARENT_LOGGER_NAME, "TRACE"));

    assertEquals(Level.TRACE, config.getLoggerConfig(PARENT_LOGGER_NAME).getLevel());
    assertEquals(Level.DEBUG, config.getLoggerConfig(CLASS_LOGGER_NAME).getLevel());
    
    // NOTE: LoggingHandler doesn't actually "remove" the LoggerConfig, ...
    // evidently so people using they UI can see that it was explicitly turned "OFF" ?
    mparams.set("set", PARENT_LOGGER_NAME + ":null");
    rsp = client.request(new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/info/logging", mparams));

    @SuppressWarnings({"unchecked"})
    ArrayList<NamedList<Object>> removedLoggerLevel = (ArrayList<NamedList<Object>>) rsp._get("loggers", null);

    assertTrue(checkLoggerLevel(removedLoggerLevel, PARENT_LOGGER_NAME, "OFF"));

    assertEquals(Level.OFF, config.getLoggerConfig(PARENT_LOGGER_NAME).getLevel());
    assertEquals(Level.DEBUG, config.getLoggerConfig(CLASS_LOGGER_NAME).getLevel());

  }

  private boolean checkLoggerLevel(ArrayList<NamedList<Object>> properties, String logger, String level) {
    for (NamedList<Object> property : properties) {
      String loggerProperty = property._get("name", "").toString();
      String levelProperty  = property._get("level", "").toString();
      if (loggerProperty.equals(logger) && levelProperty.equals(level)) {
        return true;
      }
    }
    return false;
  }
}
