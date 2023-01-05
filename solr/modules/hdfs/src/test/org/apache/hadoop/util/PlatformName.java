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
package org.apache.hadoop.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.solr.client.solrj.util.Constants;

/**
 * A helper class for getting build-info of the java-vm.
 *
 */
@InterfaceAudience.LimitedPrivate({"HBase"})
@InterfaceStability.Unstable
public class PlatformName {
  /**
   * The complete platform 'name' to identify the platform as
   * per the java-vm.
   */
  public static final String PLATFORM_NAME =
      (System.getProperty("os.name").startsWith("Windows")
          ? System.getenv("os") : System.getProperty("os.name"))
          + "-" + System.getProperty("os.arch")
          + "-" + System.getProperty("sun.arch.data.model");

  /**
   * The java vendor name used in this platform.
   */
  public static final String JAVA_VENDOR_NAME = System.getProperty("java.vendor");

  /**
   * A public static variable to indicate the current java vendor is
   * IBM java or not.
   */
  public static final boolean IBM_JAVA = Constants.IS_IBM_JAVA;

  public static void main(String[] args) {
    System.out.println(PLATFORM_NAME);
  }
}
