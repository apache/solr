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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
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

// regardless of what logging config is used to run test, we want a known-fixed logging level for
// this package
// to ensure our manipulations have expected effect
@LogLevel("org.apache.solr.bogus_logger_package=DEBUG")
@SuppressForbidden(reason = "test uses log4j2 because it tests output at a specific level")
public class LoggingHandlerTest extends SolrTestCaseJ4 {
  private final String PARENT_LOGGER_NAME = "org.apache.solr.bogus_logger_package";
  private final String A_LOGGER_NAME = PARENT_LOGGER_NAME + ".BogusClass_A";
  private final String B_LOGGER_NAME = PARENT_LOGGER_NAME + ".BogusClass_B";
  private final String BX_LOGGER_NAME = B_LOGGER_NAME + ".BogusNestedClass_X";

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

    // Direct access to the internal log4j configuration
    final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    assumeTrue("Test only works when log4j is in use", null != ctx);

    final Configuration config = ctx.getConfiguration();

    { // sanity check our setup...

      // did anybody break the anotations?
      assertNotNull(this.getClass().getAnnotation(LogLevel.class));
      final String annotationConfig = this.getClass().getAnnotation(LogLevel.class).value();
      assertTrue("WTF: " + annotationConfig, annotationConfig.startsWith(PARENT_LOGGER_NAME));
      assertTrue("WTF: " + annotationConfig, annotationConfig.endsWith(Level.DEBUG.toString()));

      // actual log4j configuration
      assertNotNull(
          "Parent logger should have explicit config", config.getLoggerConfig(PARENT_LOGGER_NAME));
      assertEquals(
          "Parent logger should have explicit DEBUG",
          Level.DEBUG,
          config.getLoggerConfig(PARENT_LOGGER_NAME).getExplicitLevel());
      for (String logger : Arrays.asList(A_LOGGER_NAME, B_LOGGER_NAME, BX_LOGGER_NAME)) {
        assertEquals(
            "Unexpected config for " + logger + " ... expected parent's config",
            config.getLoggerConfig(PARENT_LOGGER_NAME),
            config.getLoggerConfig(logger));
      }

      // Either explicit, or inherited, effictive logger values...
      for (String logger :
          Arrays.asList(PARENT_LOGGER_NAME, A_LOGGER_NAME, B_LOGGER_NAME, BX_LOGGER_NAME)) {
        assertEquals(Level.DEBUG, LogManager.getLogger(logger).getLevel());
      }
    }

    SolrClient client = new EmbeddedSolrServer(h.getCore());
    ModifiableSolrParams mparams = new ModifiableSolrParams();

    NamedList<Object> rsp =
        client.request(
            new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/info/logging", mparams));

    { // GET
      @SuppressWarnings({"unchecked"})
      List<Map<String, Object>> loggers = (List<Map<String, Object>>) rsp._get("loggers", null);

      // check expected log levels returned by handler
      assertLoggerLevel(loggers, PARENT_LOGGER_NAME, "DEBUG", true);
      for (String logger : Arrays.asList(A_LOGGER_NAME, B_LOGGER_NAME, BX_LOGGER_NAME)) {
        assertLoggerLevel(loggers, logger, "DEBUG", false);
      }
    }

    { // SET

      // update B's logger level
      mparams.set("set", B_LOGGER_NAME + ":TRACE");
      rsp =
          client.request(
              new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/info/logging", mparams));
      @SuppressWarnings({"unchecked"})
      List<Map<String, Object>> updatedLoggerLevel =
          (List<Map<String, Object>>) rsp._get("loggers", null);

      // check new log levels returned by handler
      assertLoggerLevel(updatedLoggerLevel, PARENT_LOGGER_NAME, "DEBUG", true);
      assertLoggerLevel(updatedLoggerLevel, A_LOGGER_NAME, "DEBUG", false);
      assertLoggerLevel(updatedLoggerLevel, B_LOGGER_NAME, "TRACE", true);
      assertLoggerLevel(updatedLoggerLevel, BX_LOGGER_NAME, "TRACE", false);

      // check directly with log4j what it's (updated) config has...
      assertEquals(Level.DEBUG, config.getLoggerConfig(PARENT_LOGGER_NAME).getExplicitLevel());
      assertEquals(Level.TRACE, config.getLoggerConfig(B_LOGGER_NAME).getExplicitLevel());
      assertEquals(
          "Unexpected config for BX ... expected B's config",
          config.getLoggerConfig(B_LOGGER_NAME),
          config.getLoggerConfig(BX_LOGGER_NAME));
      // ...and what it's effective values
      assertEquals(Level.DEBUG, LogManager.getLogger(PARENT_LOGGER_NAME).getLevel());
      assertEquals(Level.DEBUG, LogManager.getLogger(A_LOGGER_NAME).getLevel());
      assertEquals(Level.TRACE, LogManager.getLogger(B_LOGGER_NAME).getLevel());
      assertEquals(Level.TRACE, LogManager.getLogger(BX_LOGGER_NAME).getLevel());
    }

    { // UNSET
      final String unset = random().nextBoolean() ? "null" : "unset";
      mparams.set("set", B_LOGGER_NAME + ":" + unset);
      rsp =
          client.request(
              new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/info/logging", mparams));

      @SuppressWarnings({"unchecked"})
      List<Map<String, Object>> removedLoggerLevel =
          (List<Map<String, Object>>) rsp._get("loggers", null);

      // check new log levels returned by handler
      assertLoggerLevel(removedLoggerLevel, PARENT_LOGGER_NAME, "DEBUG", true);
      assertLoggerLevel(removedLoggerLevel, A_LOGGER_NAME, "DEBUG", false);
      assertLoggerLevel(removedLoggerLevel, B_LOGGER_NAME, "DEBUG", false);
      assertLoggerLevel(removedLoggerLevel, BX_LOGGER_NAME, "DEBUG", false);

      // check directly with log4j what it's (updated) config has...
      //
      // NOTE: LoggingHandler must not actually "remove" the LoggerConfig for B on 'unset'
      // (it might have already been defined in log4j's original config for some other reason,
      // w/o an explicit level -- Example: Filtering)
      //
      // The LoggerConfig must still exist, but with a 'null' (explicit) level
      // (so it's inheriting level from parent)
      assertEquals(Level.DEBUG, config.getLoggerConfig(PARENT_LOGGER_NAME).getLevel());
      assertEquals(null, config.getLoggerConfig(B_LOGGER_NAME).getExplicitLevel()); // explicit
      assertEquals(Level.DEBUG, config.getLoggerConfig(B_LOGGER_NAME).getLevel()); // inherited
      assertEquals(
          "Unexpected config for BX ... expected B's config",
          config.getLoggerConfig(B_LOGGER_NAME),
          config.getLoggerConfig(BX_LOGGER_NAME));
      // ...and what it's effective values
      assertEquals(Level.DEBUG, LogManager.getLogger(PARENT_LOGGER_NAME).getLevel());
      assertEquals(Level.DEBUG, LogManager.getLogger(A_LOGGER_NAME).getLevel());
      assertEquals(Level.DEBUG, LogManager.getLogger(B_LOGGER_NAME).getLevel());
      assertEquals(Level.DEBUG, LogManager.getLogger(BX_LOGGER_NAME).getLevel());
    }
  }

  private void assertLoggerLevel(
      List<Map<String, Object>> properties, String logger, String level, boolean isSet) {
    boolean foundLogger = false;
    for (Map<String, Object> property : properties) {
      final Object loggerProperty = property.get("name");
      assertNotNull("Found logger record with null 'name'", loggerProperty);
      if (logger.equals(loggerProperty)) {
        assertFalse("Duplicate Logger: " + logger, foundLogger);
        foundLogger = true;
        assertEquals("Level: " + logger, level, property.get("level"));
        assertEquals("isSet: " + logger, isSet, property.get("set"));
      }
    }
    assertTrue("Logger Missing: " + logger, foundLogger);
  }
}
