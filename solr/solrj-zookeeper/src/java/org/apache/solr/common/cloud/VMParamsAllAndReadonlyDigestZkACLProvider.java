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

/**
 * Deprecated in favor of a combination of {@link DigestZkACLProvider} and {@link
 * VMParamsZkCredentialsInjector}.
 *
 * <pre>
 * Current implementation delegates to {@link DigestZkACLProvider} with an injected {@link VMParamsZkCredentialsInjector}
 * </pre>
 */
@Deprecated
public class VMParamsAllAndReadonlyDigestZkACLProvider extends SecurityAwareZkACLProvider
    implements ZkACLProvider {

  @Deprecated
  public static final String DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME =
      VMParamsZkCredentialsInjector.DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME;

  @Deprecated
  public static final String DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME =
      VMParamsZkCredentialsInjector.DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME;

  @Deprecated
  public static final String DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME =
      VMParamsZkCredentialsInjector.DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME;

  @Deprecated
  public static final String DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME =
      VMParamsZkCredentialsInjector.DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME;

  @Deprecated
  public static final String DEFAULT_DIGEST_FILE_VM_PARAM_NAME =
      VMParamsZkCredentialsInjector.DEFAULT_DIGEST_FILE_VM_PARAM_NAME;

  private DigestZkACLProvider digestZkACLProvider;

  public VMParamsAllAndReadonlyDigestZkACLProvider() {
    this(new VMParamsZkCredentialsInjector());
  }

  public VMParamsAllAndReadonlyDigestZkACLProvider(ZkCredentialsInjector zkCredentialsInjector) {
    this.zkCredentialsInjector = zkCredentialsInjector;
    this.digestZkACLProvider = new DigestZkACLProvider(zkCredentialsInjector);
  }

  public VMParamsAllAndReadonlyDigestZkACLProvider(
      String zkDigestAllUsernameVMParamName,
      String zkDigestAllPasswordVMParamName,
      String zkDigestReadonlyUsernameVMParamName,
      String zkDigestReadonlyPasswordVMParamName) {
    this(
        new VMParamsZkCredentialsInjector(
            zkDigestAllUsernameVMParamName,
            zkDigestAllPasswordVMParamName,
            zkDigestReadonlyUsernameVMParamName,
            zkDigestReadonlyPasswordVMParamName));
  }

  @Override
  public void setZkCredentialsInjector(ZkCredentialsInjector zkCredentialsInjector) {
    this.zkCredentialsInjector =
        zkCredentialsInjector != null && !zkCredentialsInjector.getZkCredentials().isEmpty()
            ? zkCredentialsInjector
            : new VMParamsZkCredentialsInjector();
    this.digestZkACLProvider = new DigestZkACLProvider(this.zkCredentialsInjector);
  }

  @Override
  protected List<ACL> createNonSecurityACLsToAdd() {
    return digestZkACLProvider.createNonSecurityACLsToAdd();
  }

  @Override
  protected List<ACL> createSecurityACLsToAdd() {
    return digestZkACLProvider.createSecurityACLsToAdd();
  }
}
