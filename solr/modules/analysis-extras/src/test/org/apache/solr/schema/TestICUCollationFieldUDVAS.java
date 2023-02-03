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

import org.apache.lucene.util.Version;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.XmlConfigFile;
import org.apache.solr.util.LogListener;
import org.junit.BeforeClass;

/**
 * Tests warn/failure of {@link ICUCollationField} when schema explicitly sets <code>
 * useDocValuesAsStored="true"</code>
 */
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

  private enum Mode {
    OK,
    WARN,
    FAIL
  }

  @SuppressWarnings("fallthrough")
  public void testInitCore() throws Exception {
    final Mode mode = pickRandom(Mode.values());
    Version useVersion;
    switch (mode) {
      case FAIL:
        useVersion = ICUCollationField.UDVAS_FORBIDDEN_AS_OF;
        break;
      case WARN:
        useVersion = WARN_CEILING;
        break;
      case OK:
        useVersion = null;
        break;
      default:
        throw new IllegalStateException();
    }

    final String restoreLuceneMatchVersion;
    if (mode == Mode.OK) {
      restoreLuceneMatchVersion = null;
    } else {
      System.setProperty(
          random().nextBoolean() ? ICU_TYPE_UDVAS_PROPNAME : ICU_FIELD_UDVAS_PROPNAME, "true");
      restoreLuceneMatchVersion =
          System.setProperty(TEST_LUCENE_MATCH_VERSION_PROPNAME, useVersion.toString());
    }
    try (LogListener warnLog =
        LogListener.warn(XmlConfigFile.class).substring(ICUCollationField.UDVAS_MESSAGE)) {
      initCore("solrconfig.xml", "schema.xml", home);
      switch (mode) {
        case FAIL:
          fail("expected failure for version " + useVersion);
          break;
        case WARN:
          assertEquals("expected warning message was not logged", 2, warnLog.getCount());
          // Clear out the warnLog
          warnLog.pollMessage();
          warnLog.pollMessage();
          break;
        case OK:
          assertEquals("unexpected warning message was logged", 0, warnLog.getCount());
          break;
      }
    } catch (SolrException ex) {
      assertSame("unexpected hard failure for " + useVersion + ": " + ex, mode, Mode.FAIL);
      assertTrue(
          "unexpected failure message",
          getRootCause(ex).getMessage().contains(ICUCollationField.UDVAS_MESSAGE));
    } finally {
      restoreSysProps(restoreLuceneMatchVersion);
    }
  }

  private static void restoreSysProps(String restoreLuceneMatchVersion) {
    if (restoreLuceneMatchVersion != null) {
      System.clearProperty(ICU_FIELD_UDVAS_PROPNAME);
      System.clearProperty(ICU_TYPE_UDVAS_PROPNAME);
      System.setProperty(TEST_LUCENE_MATCH_VERSION_PROPNAME, restoreLuceneMatchVersion);
    }
  }
}
