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

import static org.apache.zookeeper.server.auth.DigestAuthenticationProvider.generateDigest;

import java.lang.invoke.MethodHandles;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkCredentialsInjector.ZkCredential;
import org.apache.solr.common.util.StrUtils;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that expects a {@link ZkCredentialsInjector} to create Zookeeper ACLs using digest scheme
 */
public class DigestZkACLProvider extends SecurityAwareZkACLProvider {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** Called by reflective instantiation */
  public DigestZkACLProvider() {}

  public DigestZkACLProvider(ZkCredentialsInjector zkCredentialsInjector) {
    super(zkCredentialsInjector);
  }

  /**
   * @return Set of ACLs to return for non-security related znodes
   */
  @Override
  protected List<ACL> createNonSecurityACLsToAdd() {
    return createACLsToAdd(true);
  }

  /**
   * Provider ACL Credential
   *
   * @return Set of ACLs to return security-related znodes
   */
  @Override
  protected List<ACL> createSecurityACLsToAdd() {
    return createACLsToAdd(false);
  }

  protected List<ACL> createACLsToAdd(boolean includeReadOnly) {
    List<ACL> result = new ArrayList<>();
    List<ZkCredential> zkCredentials = zkCredentialsInjector.getZkCredentials();
    log.debug("createACLsToAdd using ZkCredentials: {}", zkCredentials);
    for (ZkCredential zkCredential : zkCredentials) {
      if (StrUtils.isNullOrEmpty(zkCredential.getUsername())
          || StrUtils.isNullOrEmpty(zkCredential.getPassword())) {
        continue;
      }
      Id id = createACLId(zkCredential.getUsername(), zkCredential.getPassword());
      int perms;
      if (zkCredential.isAll()) {
        // Not to have to provide too much credentials and ACL information to the process it is
        // assumed that you want "ALL"-acls added to the user you are using to connect to ZK
        perms = ZooDefs.Perms.ALL;
      } else if (includeReadOnly && zkCredential.isReadonly()) {
        // Besides, that support for adding additional "READONLY"-acls for another user
        perms = ZooDefs.Perms.READ;
      } else {
        // ignore unsupported perms (neither All nor READONLY)
        continue;
      }
      result.add(new ACL(perms, id));
    }
    if (result.isEmpty()) {
      result = ZooDefs.Ids.OPEN_ACL_UNSAFE;
    }
    return result;
  }

  protected Id createACLId(String username, String password) {
    try {
      return new Id("digest", generateDigest(username + ":" + password));
    } catch (NoSuchAlgorithmException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "JVM mis-configured: missing SHA-1 algorithm", e);
    }
  }
}
