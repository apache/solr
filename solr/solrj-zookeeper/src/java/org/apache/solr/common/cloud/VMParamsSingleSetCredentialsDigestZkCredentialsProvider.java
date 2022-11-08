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

import java.util.Collection;

/**
 * Deprecated in favor of a combination of {@link DigestZkCredentialsProvider} and {@link
 * VMParamsZkCredentialsInjector}.
 *
 * <pre>
 * Current implementation delegates to {@link DigestZkCredentialsProvider} with an injected {@link VMParamsZkCredentialsInjector}
 * </pre>
 */
@Deprecated
public class VMParamsSingleSetCredentialsDigestZkCredentialsProvider
    extends DefaultZkCredentialsProvider {

  public static final String DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME =
      VMParamsZkCredentialsInjector.DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME;
  public static final String DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME =
      VMParamsZkCredentialsInjector.DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME;

  private DigestZkCredentialsProvider digestZkCredentialsProvider;

  public VMParamsSingleSetCredentialsDigestZkCredentialsProvider() {
    this(DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME, DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME);
  }

  public VMParamsSingleSetCredentialsDigestZkCredentialsProvider(
      ZkCredentialsInjector zkCredentialsInjector) {
    this.zkCredentialsInjector = zkCredentialsInjector;
    this.digestZkCredentialsProvider = new DigestZkCredentialsProvider(zkCredentialsInjector);
  }

  public VMParamsSingleSetCredentialsDigestZkCredentialsProvider(
      String zkDigestUsernameVMParamName, String zkDigestPasswordVMParamName) {
    this(
        new VMParamsZkCredentialsInjector(
            zkDigestUsernameVMParamName, zkDigestPasswordVMParamName, null, null));
  }

  @Override
  public void setZkCredentialsInjector(ZkCredentialsInjector zkCredentialsInjector) {
    this.zkCredentialsInjector =
        zkCredentialsInjector != null && !zkCredentialsInjector.getZkCredentials().isEmpty()
            ? zkCredentialsInjector
            : new VMParamsZkCredentialsInjector();
    this.digestZkCredentialsProvider = new DigestZkCredentialsProvider(this.zkCredentialsInjector);
  }

  @Override
  protected Collection<ZkCredentials> createCredentials() {
    return digestZkCredentialsProvider.createCredentials();
  }
}
