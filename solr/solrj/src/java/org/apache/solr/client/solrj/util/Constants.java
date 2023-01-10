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

package org.apache.solr.client.solrj.util;

import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Clone of org.apache.lucene.util.Constants, so SolrJ can use it
public class Constants {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String JVM_SPEC_VERSION = System.getProperty("java.specification.version");

  public static final boolean JRE_IS_MINIMUM_JAVA9 = true;
  public static final boolean JRE_IS_MINIMUM_JAVA11 = true;
  // Future, enable if needed...
  // public static final boolean JRE_IS_MINIMUM_JAVA17 = Runtime.version().feature() >= 17;

  public static final boolean IS_IBM_JAVA = isIBMJava();

  private static boolean isIBMJava() {
    try {
      Class.forName(
          "com.ibm.security.auth.module.Krb5LoginModule", false, Constants.class.getClassLoader());
      return true;
    } catch (SecurityException se) {
      log.warn(
          "Unable to determine if IBM Java due to SecurityException. Assuming not IBM Java.", se);
      return false;
    } catch (ClassNotFoundException ignore) {
      return false;
    }
  }
}
