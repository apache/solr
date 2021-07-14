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
package org.apache.solr.common.cloud;

import java.util.List;
import java.util.Map;

import org.apache.zookeeper.data.ACL;

public class EnvAllAndReadonlyDigestZkACLProvider extends SecurityAwareZkACLProvider {

  public static final String ZK_READ_ACL_USERNAME = "ZK_READ_ACL_USERNAME";
  public static final String ZK_READ_ACL_PASSWORD = "ZK_READ_ACL_PASSWORD";

  private static Map<String, String> envVars = null;

  static String getEnvVar(String name) {
    return envVars != null ? envVars.get(name) : System.getenv(name);
  }

  // for testing only!
  static void setEnvVars(Map<String, String> env) {
    envVars = env;
  }

  /**
   * @return Set of ACLs to return for non-security related znodes
   */
  @Override
  protected List<ACL> createNonSecurityACLsToAdd() {
    return createACLsToAdd(true);
  }

  /**
   * @return Set of ACLs to return security-related znodes
   */
  @Override
  protected List<ACL> createSecurityACLsToAdd() {
    return createACLsToAdd(false);
  }

  protected List<ACL> createACLsToAdd(boolean includeReadOnly) {
    String digestAllUsername = getEnvVar(EnvSingleSetCredentialsDigestZkCredentialsProvider.ZK_ALL_ACL_USERNAME);
    String digestAllPassword = getEnvVar(EnvSingleSetCredentialsDigestZkCredentialsProvider.ZK_ALL_ACL_PASSWORD);
    String digestReadonlyUsername = getEnvVar(ZK_READ_ACL_USERNAME);
    String digestReadonlyPassword = getEnvVar(ZK_READ_ACL_PASSWORD);

    return createACLsToAdd(includeReadOnly,
        digestAllUsername, digestAllPassword,
        digestReadonlyUsername, digestReadonlyPassword);
  }
}

