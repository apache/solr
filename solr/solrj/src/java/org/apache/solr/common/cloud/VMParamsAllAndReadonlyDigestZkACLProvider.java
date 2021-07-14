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

import org.apache.zookeeper.data.ACL;

public class VMParamsAllAndReadonlyDigestZkACLProvider extends SecurityAwareZkACLProvider {

  public static final String DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME = "zkDigestReadonlyUsername";
  public static final String DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME = "zkDigestReadonlyPassword";
  
  final String zkDigestAllUsernameVMParamName;
  final String zkDigestAllPasswordVMParamName;
  final String zkDigestReadonlyUsernameVMParamName;
  final String zkDigestReadonlyPasswordVMParamName;
  
  public VMParamsAllAndReadonlyDigestZkACLProvider() {
    this(
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME, 
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME,
        DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME,
        DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME
        );
  }
  
  public VMParamsAllAndReadonlyDigestZkACLProvider(String zkDigestAllUsernameVMParamName, String zkDigestAllPasswordVMParamName,
      String zkDigestReadonlyUsernameVMParamName, String zkDigestReadonlyPasswordVMParamName) {
    this.zkDigestAllUsernameVMParamName = zkDigestAllUsernameVMParamName;
    this.zkDigestAllPasswordVMParamName = zkDigestAllPasswordVMParamName;
    this.zkDigestReadonlyUsernameVMParamName = zkDigestReadonlyUsernameVMParamName;
    this.zkDigestReadonlyPasswordVMParamName = zkDigestReadonlyPasswordVMParamName;
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
    String digestAllUsername = System.getProperty(zkDigestAllUsernameVMParamName);
    String digestAllPassword = System.getProperty(zkDigestAllPasswordVMParamName);
    String digestReadonlyUsername = System.getProperty(zkDigestReadonlyUsernameVMParamName);
    String digestReadonlyPassword = System.getProperty(zkDigestReadonlyPasswordVMParamName);

    return createACLsToAdd(includeReadOnly,
        digestAllUsername, digestAllPassword,
        digestReadonlyUsername, digestReadonlyPassword);
  }
}

