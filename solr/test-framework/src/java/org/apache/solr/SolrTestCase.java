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

import com.carrotsearch.randomizedtesting.TraceFormatting;
import java.lang.invoke.MethodHandles;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.lucene.util.VerifyTestClassNamingConvention;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.RevertDefaultThreadHandlerRule;
import org.apache.solr.util.StartupLoggingUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;

import static com.carrotsearch.randomizedtesting.RandomizedTest.systemPropertyAsBoolean;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

/**
 * All Solr test cases should derive from this class eventually. This is originally a result of async logging, see:
 * SOLR-12055 and associated. To enable async logging, we must gracefully shut down logging. Many Solr tests subclass
 * LuceneTestCase.
 *
 * Rather than add the cruft from SolrTestCaseJ4 to all the Solr tests that currently subclass LuceneTestCase,
 * we'll add the shutdown to this class and subclass it.
 *
 * Other changes that should affect every Solr test case may go here if they don't require the added capabilities in
 * SolrTestCaseJ4.
 */

  // ThreadLeakFilters are not additive. Any subclass that requires filters
  // other than these must redefine these as well.
@ThreadLeakFilters(defaultFilters = true, filters = {
        SolrIgnoredThreadsFilter.class,
        QuickPatchThreadsFilter.class
})
@ThreadLeakLingering(linger = 10000)
public class SolrTestCase extends LuceneTestCase {

  /**
   * <b>DO NOT REMOVE THIS LOGGER</b>
   * <p>
   * For reasons that aren't 100% obvious, the existence of this logger is neccessary to ensure
   * that the logging framework is properly initialized (even if concrete subclasses do not 
   * themselves initialize any loggers) so that the async logger threads can be properly shutdown
   * on completion of the test suite
   * </p>
   * @see <a href="https://issues.apache.org/jira/browse/SOLR-14247">SOLR-14247</a>
   * @see #shutdownLogger
   */
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Pattern NAMING_CONVENTION_TEST_SUFFIX = Pattern.compile("(.+\\.)([^.]+)(Test)");

  private static final Pattern NAMING_CONVENTION_TEST_PREFIX = Pattern.compile("(.+\\.)(Test)([^.]+)");

  private static final List<String> DEFAULT_STACK_FILTERS = Arrays.asList(new String [] {
      "org.junit.",
      "junit.framework.",
      "sun.",
      "java.lang.reflect.",
      "com.carrotsearch.randomizedtesting.",
  });

  @ClassRule
  public static TestRule solrClassRules = 
    RuleChain.outerRule(new SystemPropertiesRestoreRule())
             .around(
                 new VerifyTestClassNamingConvention(
                     "org.apache.solr.analytics", NAMING_CONVENTION_TEST_SUFFIX))
             .around(
                 new VerifyTestClassNamingConvention(
                     "org.apache.solr.ltr", NAMING_CONVENTION_TEST_PREFIX))
             .around(new RevertDefaultThreadHandlerRule());

  /**
   * Sets the <code>solr.default.confdir</code> system property to the value of 
   * {@link ExternalPaths#DEFAULT_CONFIGSET} if and only if the system property is not already set, 
   * and the <code>DEFAULT_CONFIGSET</code> exists and is a readable directory.
   * <p>
   * Logs INFO/WARNing messages as appropriate based on these 2 conditions.
   * </p>
   * @see SolrDispatchFilter#SOLR_DEFAULT_CONFDIR_ATTRIBUTE
   */
  @BeforeClass
  public static void setDefaultConfigDirSysPropIfNotSet() {
    final String existingValue = System.getProperty(SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE);
    if (null != existingValue) {
      log.info("Test env includes configset dir system property '{}'='{}'", SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE, existingValue);
      return;
    }
    final File extPath = new File(ExternalPaths.DEFAULT_CONFIGSET);
    if (extPath.canRead(/* implies exists() */) && extPath.isDirectory()) {
      log.info("Setting '{}' system property to test-framework derived value of '{}'",
               SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE, ExternalPaths.DEFAULT_CONFIGSET);
      assert null == existingValue;
      System.setProperty(SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE, ExternalPaths.DEFAULT_CONFIGSET);
    } else {
      log.warn("System property '{}' is not already set, but test-framework derived value ('{}') either " +
               "does not exist or is not a readable directory, you may need to set the property yourself " +
               "for tests to run properly",
               SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE, ExternalPaths.DEFAULT_CONFIGSET);
    }
  }
  
  /** 
   * Special hook for sanity checking if any tests trigger failures when an
   * Assumption failure occures in a {@link BeforeClass} method
   * @lucene.internal
   */
  @BeforeClass
  public static void checkSyspropForceBeforeClassAssumptionFailure() {
    // ant test -Dargs="-Dtests.force.assumption.failure.beforeclass=true"
    final String PROP = "tests.force.assumption.failure.beforeclass";
    assumeFalse(PROP + " == true",
                systemPropertyAsBoolean(PROP, false));
  }
  
  /** 
   * Special hook for sanity checking if any tests trigger failures when an
   * Assumption failure occures in a {@link Before} method
   * @lucene.internal
   */
  @Before
  public void checkSyspropForceBeforeAssumptionFailure() {
    // ant test -Dargs="-Dtests.force.assumption.failure.before=true"
    final String PROP = "tests.force.assumption.failure.before";
    assumeFalse(PROP + " == true",
                systemPropertyAsBoolean(PROP, false));
  }
  
  @AfterClass
  public static void shutdownLogger() throws Exception {
    try {
      if (suiteFailureMarker.wasSuccessful()) {
        // if the tests passed, make sure everything was closed / released
        String orr = clearObjectTrackerAndCheckEmpty(0, false);
        assertNull(orr, orr);
      } else {
        ObjectReleaseTracker.tryClose();
      }
    } finally {
      ObjectReleaseTracker.clear();
      StartupLoggingUtils.shutdown();
    }
  }


  /**
   * @return null if ok else error message
   */
  public static String clearObjectTrackerAndCheckEmpty(int waitSeconds) {
    return clearObjectTrackerAndCheckEmpty(waitSeconds, false);
  }

  /**
   * @return null if ok else error message
   */
  public static String clearObjectTrackerAndCheckEmpty(int waitSeconds, boolean tryClose) {
    int retries = 0;
    String result;
    do {
      result = ObjectReleaseTracker.checkEmpty();
      if (result == null)
        break;
      try {
        if (retries % 10 == 0) {
          log.info("Waiting for all tracked resources to be released");
          if (retries > 10) {
            TraceFormatting tf = new TraceFormatting(DEFAULT_STACK_FILTERS);
            Map<Thread,StackTraceElement[]> stacksMap = Thread.getAllStackTraces();
            Set<Entry<Thread,StackTraceElement[]>> entries = stacksMap.entrySet();
            for (Entry<Thread,StackTraceElement[]> entry : entries) {
              String stack = tf.formatStackTrace(entry.getValue());
              System.err.println(entry.getKey().getName() + ":\n" + stack);
            }
          }
        }
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) { break; }
    }
    while (retries++ < waitSeconds);


    log.info("------------------------------------------------------- Done waiting for tracked resources to be released");

    ObjectReleaseTracker.clear();

    return result;
  }

  public static void interruptThreadsOnTearDown() {

    log.info("Checking leaked threads after test");

    ThreadGroup tg = Thread.currentThread().getThreadGroup();

    Set<Map.Entry<Thread, StackTraceElement[]>> threadSet = Thread.getAllStackTraces().entrySet();
    if (log.isInfoEnabled()) {
      log.info("thread count={}", threadSet.size());
    }
    Collection<Thread> waitThreads = new ArrayList<>();
    for (Map.Entry<Thread, StackTraceElement[]> threadEntry : threadSet) {
      Thread thread = threadEntry.getKey();
      ThreadGroup threadGroup = thread.getThreadGroup();
      if (threadGroup != null
          && !(thread.getName().startsWith("SUITE")
          && thread.getName().charAt(thread.getName().length() - 1) == ']')
          && !"main".equals(thread.getName())) {
        if (log.isTraceEnabled()) {
          log.trace("thread is {} state={}", thread.getName(), thread.getState());
        }
        if (threadGroup.getName().equals(tg.getName()) && interrupt(thread)) {
          waitThreads.add(thread);
        }
      }

      while (true) {
        boolean cont =
            threadGroup != null && threadGroup.getParent() != null && !(
                thread.getName().startsWith("SUITE")
                    && thread.getName().charAt(thread.getName().length() - 1) == ']')
                && !"main".equals(thread.getName());
        if (!cont) break;
        threadGroup = threadGroup.getParent();

        if (threadGroup.getName().equals(tg.getName())) {
          if (log.isTraceEnabled()) {
            log.trace("thread is {} state={}", thread.getName(), thread.getState());
          }
          if (interrupt(thread)) {
            waitThreads.add(thread);
          }
        }
      }
    }

    for (Thread thread : waitThreads) {
      int cnt = 0;
      do {
        if (log.isDebugEnabled()) {
          log.debug("waiting on {} {}", thread.getName(), thread.getState());
        }
        thread.interrupt();
        try {
          thread.join(5);
        } catch (InterruptedException e) {
          // ignore
        }
      } while (cnt++ < 20);
    }

    waitThreads.clear();
  }

  private static boolean interrupt(Thread thread) {

    if (thread.getName().startsWith("Reference Handler")
        || thread.getName().startsWith("Signal Dispatcher")
        || thread.getName().startsWith("Monitor")
        || thread.getName().startsWith("YJPAgent-RequestListener") || thread.getName().startsWith("TimeLimitedCollector")) {
      return false;
    }

    if (thread.getName().startsWith("ForkJoinPool.")
        || thread.getName().startsWith("Log4j2-")) {
      return false;
    }


    if (thread.getName().startsWith("SessionTracker")) {
      thread.interrupt();
      return false;
    }

    // pool is forkjoin
    if (thread.getName().contains("pool-")) {
      thread.interrupt();
      return false;
    }

    Thread.State state = thread.getState();

    if (state == Thread.State.TERMINATED) {
      return false;
    }
    if (log.isDebugEnabled()) {
      log.debug("Interrupt on {} state={}", thread.getName(), thread.getState());
    }
    thread.interrupt();
    return true;
  }
}
