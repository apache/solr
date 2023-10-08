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

import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.solr.common.util.StrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that expects a {@link ZkCredentialsInjector} to create Zookeeper credentials using Digest
 * scheme
 */
public class DigestZkCredentialsProvider extends DefaultZkCredentialsProvider {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** Called by reflective instantiation */
  public DigestZkCredentialsProvider() {}

  public DigestZkCredentialsProvider(ZkCredentialsInjector zkCredentialsInjector) {
    super(zkCredentialsInjector);
  }

  @Override
  protected Collection<ZkCredentials> createCredentials() {
    List<ZkCredentials> result = new ArrayList<>(1);
    List<ZkCredentialsInjector.ZkCredential> zkCredentials =
        zkCredentialsInjector.getZkCredentials();
    log.debug("createCredentials using zkCredentials: {}", zkCredentials);
    for (ZkCredentialsInjector.ZkCredential zkCredential : zkCredentials) {
      if (zkCredential.isAll()) {
        // this is the "user" with all perms that SolrZooKeeper uses to connect to zookeeper
        if (StrUtils.isNotNullOrEmpty(zkCredential.getUsername())
            && StrUtils.isNotNullOrEmpty(zkCredential.getPassword())) {
          result.add(
              new ZkCredentials(
                  "digest",
                  (zkCredential.getUsername() + ":" + zkCredential.getPassword())
                      .getBytes(StandardCharsets.UTF_8)));
          break; // single credentials set
        }
      }
    }
    return result;
  }
}
