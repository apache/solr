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

package org.apache.solr;

import static com.carrotsearch.randomizedtesting.RandomizedTest.systemPropertyAsBoolean;
import static org.apache.solr.common.util.Utils.fromJSONString;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.rules.StatementAdapter;
import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressSysoutChecks;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.lucene.tests.util.VerifyTestClassNamingConvention;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.RevertDefaultThreadHandlerRule;
import org.apache.solr.util.StartupLoggingUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.ComparisonFailure;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * All Solr test cases should derive from this class eventually. This is originally a result of
 * async logging, see: SOLR-12055 and associated. To enable async logging, we must gracefully shut
 * down logging. Many Solr tests subclass LuceneTestCase.
 *
 * <p>Rather than add the cruft from SolrTestCaseJ4 to all the Solr tests that currently subclass
 * LuceneTestCase, we'll add the shutdown to this class and subclass it.
 *
 * <p>Other changes that should affect every Solr test case may go here if they don't require the
 * added capabilities in SolrTestCaseJ4.
 */

// ThreadLeakFilters are not additive. Any subclass that requires filters
// other than these must redefine these as well.
@ThreadLeakFilters(
    defaultFilters = true,
    filters = {SolrIgnoredThreadsFilter.class, QuickPatchThreadsFilter.class})
// The ThreadLeakLingering is set to 1s to allow ThreadPools to finish
// joining on termination. Ideally this should only be 10-100ms, but
// on slow machines it could take up to 1s. See discussion on SOLR-15660
// and SOLR-16187 regarding why this is necessary.
@ThreadLeakLingering(linger = 1000)
@SuppressSysoutChecks(bugUrl = "Solr dumps tons of logs to console.")
public class SolrTestCase extends LuceneTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Pattern NAMING_CONVENTION_TEST_SUFFIX =
      Pattern.compile("(.+\\.)([^.]+)(Test)");

  private static final Pattern NAMING_CONVENTION_TEST_PREFIX =
      Pattern.compile("(.+\\.)(Test)([^.]+)");

  @ClassRule
  public static TestRule solrClassRules =
      RuleChain.outerRule(new SystemPropertiesRestoreRule())
          .around(
              new VerifyTestClassNamingConvention(
                  "org.apache.solr.analytics", NAMING_CONVENTION_TEST_SUFFIX))
          .around(
              new VerifyTestClassNamingConvention(
                  "org.apache.solr.ltr", NAMING_CONVENTION_TEST_PREFIX))
          .around(new RevertDefaultThreadHandlerRule())
          .around(
              (base, description) ->
                  new StatementAdapter(base) {
                    @Override
                    protected void afterIfSuccessful() {
                      // if the tests passed, make sure everything was closed / released
                      String orr = ObjectReleaseTracker.clearObjectTrackerAndCheckEmpty();
                      assertNull(orr, orr);
                    }

                    @Override
                    protected void afterAlways(List<Throwable> errors) {
                      if (!errors.isEmpty()) {
                        ObjectReleaseTracker.tryClose();
                      }
                      StartupLoggingUtils.shutdown();
                    }
                  });

  /**
   * Sets the <code>solr.default.confdir</code> system property to the value of {@link
   * ExternalPaths#DEFAULT_CONFIGSET} if and only if the system property is not already set, and the
   * <code>DEFAULT_CONFIGSET</code> exists and is a readable directory.
   *
   * <p>Logs INFO/WARNing messages as appropriate based on these 2 conditions.
   *
   * @see SolrDispatchFilter#SOLR_DEFAULT_CONFDIR_ATTRIBUTE
   */
  @BeforeClass
  public static void beforeSolrTestCase() {
    final String existingValue =
        System.getProperty(SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE);
    if (null != existingValue) {
      log.info(
          "Test env includes configset dir system property '{}'='{}'",
          SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE,
          existingValue);
      return;
    }
    final File extPath = new File(ExternalPaths.DEFAULT_CONFIGSET);
    if (extPath.canRead(/* implies exists() */ ) && extPath.isDirectory()) {
      log.info(
          "Setting '{}' system property to test-framework derived value of '{}'",
          SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE,
          ExternalPaths.DEFAULT_CONFIGSET);
      assert null == existingValue;
      System.setProperty(
          SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE, ExternalPaths.DEFAULT_CONFIGSET);
    } else {
      log.warn(
          "System property '{}' is not already set, but test-framework derived value ('{}') either "
              + "does not exist or is not a readable directory, you may need to set the property yourself "
              + "for tests to run properly",
          SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE,
          ExternalPaths.DEFAULT_CONFIGSET);
    }

    // set solr.install.dir needed by some test configs outside of the test sandbox (!)
    if (ExternalPaths.SOURCE_HOME != null) {
      System.setProperty("solr.install.dir", ExternalPaths.SOURCE_HOME);
    }

    if (!TEST_NIGHTLY) {
      System.setProperty("zookeeper.nio.numSelectorThreads", "2");
      System.setProperty("zookeeper.nio.numWorkerThreads", "3");
      System.setProperty("zookeeper.commitProcessor.numWorkerThreads", "2");
      System.setProperty("zookeeper.forceSync", "no");
      System.setProperty("zookeeper.nio.shutdownTimeout", "100");
    }
  }

  /**
   * Special hook for sanity checking if any tests trigger failures when an Assumption failure
   * occures in a {@link BeforeClass} method
   *
   * @lucene.internal
   */
  @BeforeClass
  public static void checkSyspropForceBeforeClassAssumptionFailure() {
    // ant test -Dargs="-Dtests.force.assumption.failure.beforeclass=true"
    final String PROP = "tests.force.assumption.failure.beforeclass";
    assumeFalse(PROP + " == true", systemPropertyAsBoolean(PROP, false));
  }

  /**
   * Special hook for sanity checking if any tests trigger failures when an Assumption failure
   * occures in a {@link Before} method
   *
   * @lucene.internal
   */
  @Before
  public void checkSyspropForceBeforeAssumptionFailure() {
    // ant test -Dargs="-Dtests.force.assumption.failure.before=true"
    final String PROP = "tests.force.assumption.failure.before";
    assumeFalse(PROP + " == true", systemPropertyAsBoolean(PROP, false));
  }

  public static void assertJSONEquals(String expected, String actual) {
    Object json1 = fromJSONString(expected);
    Object json2 = fromJSONString(actual);
    if (Objects.equals(json2, json1)) return;
    throw new ComparisonFailure("", expected, actual);
  }
}
