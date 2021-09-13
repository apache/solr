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

package org.apache.solr.cloud.hadoop;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.io.nativeio.NativeIO;

import org.apache.hadoop.util.DiskChecker;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.common.util.SuppressForbidden;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopTestUtil {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static final String SOLR_HACK_FOR_CLASS_VERIFICATION_FIELD = "SOLR_HACK_FOR_CLASS_VERIFICATION";


  public static void checkAssumptions() {
    ensureHadoopHomeNotSet();
    checkHadoopWindows();
    checkOverriddenHadoopClasses(getModifiedHadoopClasses());
    checkFastDateFormat();
    checkGeneratedIdMatches();
  }

  /**
   * If Hadoop home is set via environment variable HADOOP_HOME or Java system property
   * hadoop.home.dir, the behavior of test is undefined. Ensure that these are not set
   * before starting. It is not possible to easily unset environment variables so better
   * to bail out early instead of trying to test.
   */
  protected static void ensureHadoopHomeNotSet() {
    if (System.getenv("HADOOP_HOME") != null) {
      LuceneTestCase.fail("Ensure that HADOOP_HOME environment variable is not set.");
    }
    if (System.getProperty("hadoop.home.dir") != null) {
      LuceneTestCase.fail("Ensure that \"hadoop.home.dir\" Java property is not set.");
    }
  }

  /**
   * Hadoop integration tests fail on Windows without Hadoop NativeIO
   */
  protected static void checkHadoopWindows() {
    LuceneTestCase.assumeTrue("Hadoop does not work on Windows without Hadoop NativeIO",
        !Constants.WINDOWS || NativeIO.isAvailable());
  }


  protected static List<Class<?>> getModifiedHadoopClasses() {
    List<Class<?>> modifiedHadoopClasses = new ArrayList<>(Arrays.asList(
        DiskChecker.class,
        FileUtil.class,
        HardLink.class,
        HttpServer2.class,
        RawLocalFileSystem.class));

    return modifiedHadoopClasses;
  }

  /**
   * Ensure that the tests are picking up the modified Hadoop classes
   */
  protected static void checkOverriddenHadoopClasses(List<Class<?>> modifiedHadoopClasses) {
    for (Class<?> clazz : modifiedHadoopClasses) {
      try {
        LuceneTestCase.assertNotNull("Field on " + clazz.getCanonicalName() + " should not have been null",
            clazz.getField(SOLR_HACK_FOR_CLASS_VERIFICATION_FIELD));
      } catch (NoSuchFieldException e) {
        LuceneTestCase.fail("Expected to load Solr modified Hadoop class " + clazz.getCanonicalName() +
            " , but it was not found.");
      }
    }
  }

  /**
   * Checks that commons-lang3 FastDateFormat works with configured locale
   */
  @SuppressForbidden(reason="Call FastDateFormat.format same way Hadoop calls it")
  protected static void checkFastDateFormat() {
    try {
      FastDateFormat.getInstance().format(System.currentTimeMillis());
    } catch (ArrayIndexOutOfBoundsException e) {
      LuceneTestCase.assumeNoException("commons-lang3 FastDateFormat doesn't work with " +
          Locale.getDefault().toLanguageTag(), e);
    }
  }

  /**
   * Hadoop fails to generate locale agnostic ids - Checks that generated string matches
   */
  protected static void checkGeneratedIdMatches() {
    // This is basically how Namenode generates fsimage ids and checks that the fsimage filename matches
    LuceneTestCase.assumeTrue("Check that generated id matches regex",
        Pattern.matches("(\\d+)", String.format(Locale.getDefault(),"%019d", 0)));
  }
}
