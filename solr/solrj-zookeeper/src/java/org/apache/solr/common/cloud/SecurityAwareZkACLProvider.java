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

import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.ACL;

/**
 * {@link ZkACLProvider} capable of returning a different set of {@link ACL}s for security-related
 * znodes (default: subtree under /security and security.json) vs non-security-related znodes.
 */
public abstract class SecurityAwareZkACLProvider implements ZkACLProvider {
  public static final String SECURITY_ZNODE_PATH = "/security";

<<<<<<< HEAD:solr/solrj/src/java/org/apache/solr/common/cloud/SecurityAwareZkACLProvider.java
  private volatile List<ACL> nonSecurityACLsToAdd;
  private volatile List<ACL> securityACLsToAdd;

  private final String securityConfPath;
  private final String securityZNodePath;
  private final String securityZNodePathDir;

  protected ZkCredentialsInjector zkCredentialsInjector;

  public SecurityAwareZkACLProvider() {
    this(new DefaultZkCredentialsInjector());
  }

  public SecurityAwareZkACLProvider(String chroot) {
    this.securityConfPath = ZKPaths.makePath(chroot, ZkStateReader.SOLR_SECURITY_CONF_PATH);
    this.securityZNodePath = ZKPaths.makePath(chroot, SECURITY_ZNODE_PATH);
    this.securityZNodePathDir = securityZNodePath + "/";
  }

  public SecurityAwareZkACLProvider withChroot(String chroot) {
    return new SecurityAwareZkACLProvider(chroot) {
      @Override
      protected List<ACL> createNonSecurityACLsToAdd() {
        return SecurityAwareZkACLProvider.this.createNonSecurityACLsToAdd();
      }

      @Override
      protected List<ACL> createSecurityACLsToAdd() {
        return SecurityAwareZkACLProvider.this.createSecurityACLsToAdd();
      }
    };
  }

  public SecurityAwareZkACLProvider(ZkCredentialsInjector zkCredentialsInjector) {
    this.zkCredentialsInjector = zkCredentialsInjector;
  }

  @Override
  public void setZkCredentialsInjector(ZkCredentialsInjector zkCredentialsInjector) {
    this.zkCredentialsInjector = zkCredentialsInjector;
  }

  @Override
  public final List<ACL> getACLsToAdd(String zNodePath) {
    if (isSecurityZNodePath(zNodePath)) {
      return getSecurityACLsToAdd();
    } else {
      return getNonSecurityACLsToAdd();
    }
  }

  @Override
  public final List<ACL> getDefaultAcl() {
    return getNonSecurityACLsToAdd();
  }

  protected boolean isSecurityZNodePath(String zNodePath) {
    return zNodePath != null
        && (zNodePath.equals(securityConfPath)
            || zNodePath.equals(securityZNodePath)
            || zNodePath.startsWith(securityZNodePathDir));
  }

  /**
   * @return Set of ACLs to return for non-security related znodes
   */
  protected abstract List<ACL> createNonSecurityACLsToAdd();

  /**
   * @return Set of ACLs to return security-related znodes
   */
  protected abstract List<ACL> createSecurityACLsToAdd();

  private List<ACL> getNonSecurityACLsToAdd() {
    if (nonSecurityACLsToAdd == null) {
      synchronized (this) {
        if (nonSecurityACLsToAdd == null) nonSecurityACLsToAdd = createNonSecurityACLsToAdd();
      }
    }
    return nonSecurityACLsToAdd;
  }

  private List<ACL> getSecurityACLsToAdd() {
    if (securityACLsToAdd == null) {
      synchronized (this) {
        if (securityACLsToAdd == null) securityACLsToAdd = createSecurityACLsToAdd();
      }
    }
    return securityACLsToAdd;
  }
}
