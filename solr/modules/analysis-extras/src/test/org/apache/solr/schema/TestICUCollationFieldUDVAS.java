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
package org.apache.solr.schema;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.lucene.util.Version;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.core.XmlConfigFile;
import org.junit.BeforeClass;

import java.io.StringWriter;
import java.util.function.BooleanSupplier;

/**
 * Tests warn/failure of {@link ICUCollationField} when schema explicitly sets
 * <code>useDocValuesAsStored="true"</code>
 */
@SuppressForbidden(reason = "test is specific to log4j2")
public class TestICUCollationFieldUDVAS extends SolrTestCaseJ4 {

  private static final String ICU_FIELD_UDVAS_PROPNAME = "tests.icu_collation_field.udvas";
  private static final String ICU_TYPE_UDVAS_PROPNAME = "tests.icu_collation_fieldType.udvas";
  private static final String TEST_LUCENE_MATCH_VERSION_PROPNAME = "tests.luceneMatchVersion";
  private static final Version WARN_CEILING = Version.LUCENE_8_12_0;
  private static String home;

  @BeforeClass
  public static void beforeClass() throws Exception {
    home = TestICUCollationFieldDocValues.setupSolrHome();
  }

  private enum Mode { OK, WARN, FAIL }

  @SuppressWarnings("fallthrough")
  public void testInitCore() throws Exception {
    final Mode mode = pickRandom(Mode.values());
    final BooleanSupplier messageLogged;
    final Runnable cleanup;
    Version useVersion = null;
    switch (mode) {
      case FAIL:
        useVersion = ICUCollationField.UDVAS_FORBIDDEN_AS_OF;
        messageLogged = null;
        cleanup = null;
        break;
      case WARN:
        useVersion = WARN_CEILING;
        // fallthrough
      case OK:
        // `OK` leaves `useVersion == null`
        final StringWriter writer = new StringWriter();
        final WriterAppender appender = WriterAppender.createAppender(
                PatternLayout
                        .newBuilder()
                        .withPattern("%-5p [%t]: %m%n")
                        .build(),
                null, writer, "ICUCollationUdvasTest", false, true);
        appender.start();
        final Logger logger = LogManager.getLogger(XmlConfigFile.class);
        final Level level = logger.getLevel();
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final LoggerConfig config = ctx.getConfiguration().getLoggerConfig(logger.getName());
        config.setLevel(Level.WARN);
        config.addAppender(appender, Level.WARN, null);
        ctx.updateLoggers();
        messageLogged = () -> writer.toString().contains(ICUCollationField.UDVAS_MESSAGE);
        cleanup = () -> {
          config.setLevel(level);
          config.removeAppender(appender.getName());
          ctx.updateLoggers();
        };
        break;
      default:
        throw new IllegalStateException();
    }

    final String restoreLuceneMatchVersion;
    if (mode == Mode.OK) {
      restoreLuceneMatchVersion = null;
    } else {
      System.setProperty(random().nextBoolean() ? ICU_TYPE_UDVAS_PROPNAME : ICU_FIELD_UDVAS_PROPNAME, "true");
      restoreLuceneMatchVersion = System.setProperty(TEST_LUCENE_MATCH_VERSION_PROPNAME, useVersion.toString());
    }
    try {
      initCore("solrconfig.xml", "schema.xml", home);
      switch (mode) {
        case FAIL:
          fail("expected failure for version " + useVersion);
          break;
        case WARN:
          assertTrue("expected warning message was not logged", messageLogged.getAsBoolean());
          break;
        case OK:
          assertFalse("unexpected warning message was logged", messageLogged.getAsBoolean());
          break;
      }
    } catch (SolrException ex) {
      assertSame("unexpected hard failure for " + useVersion + ": " + ex, mode, Mode.FAIL);
      assertTrue("unexpected failure message", getRootCause(ex).getMessage().contains(ICUCollationField.UDVAS_MESSAGE));
    } finally {
      switch (mode) {
        case WARN:
          restoreSysProps(restoreLuceneMatchVersion);
          cleanup.run();
          break;
        case FAIL:
          restoreSysProps(restoreLuceneMatchVersion);
          break;
        case OK:
          // we didn't modify any sysprops, but did verify that the warning message wasn't logged, so have
          // to cleanup appenders, etc.
          cleanup.run();
          break;
      }
    }
  }

  private static void restoreSysProps(String restoreLuceneMatchVersion) {
    System.clearProperty(ICU_FIELD_UDVAS_PROPNAME);
    System.clearProperty(ICU_TYPE_UDVAS_PROPNAME);
    System.setProperty(TEST_LUCENE_MATCH_VERSION_PROPNAME, restoreLuceneMatchVersion);
  }
}
