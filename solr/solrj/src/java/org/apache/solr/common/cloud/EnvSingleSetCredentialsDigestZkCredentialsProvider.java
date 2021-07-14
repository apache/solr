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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.StringUtils;

public class EnvSingleSetCredentialsDigestZkCredentialsProvider extends DefaultZkCredentialsProvider {

  public static final String ZK_ALL_ACL_USERNAME = "ZK_ALL_ACL_USERNAME";
  public static final String ZK_ALL_ACL_PASSWORD = "ZK_ALL_ACL_PASSWORD";

  private static Map<String, String> envVars = null;

  static String getEnvVar(String name) {
    return envVars != null ? envVars.get(name) : System.getenv(name);
  }

  // for testing only!
  static void setEnvVars(Map<String, String> env) {
    envVars = env;
  }

  @Override
  protected Collection<ZkCredentials> createCredentials() {
    List<ZkCredentials> result = new ArrayList<>();
    String digestUsername = getEnvVar(ZK_ALL_ACL_USERNAME);
    String digestPassword = getEnvVar(ZK_ALL_ACL_PASSWORD);

    if (!StringUtils.isEmpty(digestUsername) && !StringUtils.isEmpty(digestPassword)) {
      result.add(new ZkCredentials("digest",
          (digestUsername + ":" + digestPassword).getBytes(StandardCharsets.UTF_8)));
    }
    return result;
  }
}

