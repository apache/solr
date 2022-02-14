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

package org.apache.solr.security.hadoop;

import java.lang.invoke.MethodHandles;
import org.apache.hadoop.io.nativeio.NativeIO;

import org.apache.lucene.util.Constants;
import org.apache.solr.SolrTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopTestUtil {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static void checkAssumptions() {
    ensureHadoopHomeNotSet();
    checkHadoopWindows();
  }

  /**
   * If Hadoop home is set via environment variable HADOOP_HOME or Java system property
   * hadoop.home.dir, the behavior of test is undefined. Ensure that these are not set
   * before starting. It is not possible to easily unset environment variables so better
   * to bail out early instead of trying to test.
   */
  protected static void ensureHadoopHomeNotSet() {
    if (System.getenv("HADOOP_HOME") != null) {
      SolrTestCase.fail("Ensure that HADOOP_HOME environment variable is not set.");
    }
    if (System.getProperty("hadoop.home.dir") != null) {
      SolrTestCase.fail("Ensure that \"hadoop.home.dir\" Java property is not set.");
    }
  }

  /**
   * Hadoop integration tests fail on Windows without Hadoop NativeIO
   */
  protected static void checkHadoopWindows() {
    SolrTestCase.assumeTrue("Hadoop does not work on Windows without Hadoop NativeIO",
        !Constants.WINDOWS || NativeIO.isAvailable());
  }
}
