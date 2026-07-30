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
package org.apache.solr.core;

import org.apache.solr.client.solrj.impl.SolrZkClientTimeout;
import org.apache.solr.common.SolrException;

public class CloudConfig {

  private final String zkHost;

  private final int zkClientTimeout;

  private final int hostPort;

  private final String hostName;

  private final int leaderVoteWait;

  private final int leaderConflictResolveWait;

  private final String zkCredentialsProviderClass;

  private final String zkACLProviderClass;

  private final String zkCredentialsInjectorClass;

  private final int createCollectionWaitTimeTillActive;

  private final boolean createCollectionCheckLeaderActive;

  private final String pkiHandlerPrivateKeyPath;

  private final String pkiHandlerPublicKeyPath;

  private final int minStateByteLenForCompression;

  private final String stateCompressorClass;

  CloudConfig(
      String zkHost,
      int zkClientTimeout,
      int hostPort,
      String hostName,
      int leaderVoteWait,
      int leaderConflictResolveWait,
      String zkCredentialsProviderClass,
      String zkACLProviderClass,
      String zkCredentialsInjectorClass,
      int createCollectionWaitTimeTillActive,
      boolean createCollectionCheckLeaderActive,
      String pkiHandlerPrivateKeyPath,
      String pkiHandlerPublicKeyPath,
      int minStateByteLenForCompression,
      String stateCompressorClass) {
    this.zkHost = zkHost;
    this.zkClientTimeout = zkClientTimeout;
    this.hostPort = hostPort;
    this.hostName = hostName;
    this.leaderVoteWait = leaderVoteWait;
    this.leaderConflictResolveWait = leaderConflictResolveWait;
    this.zkCredentialsProviderClass = zkCredentialsProviderClass;
    this.zkACLProviderClass = zkACLProviderClass;
    this.zkCredentialsInjectorClass = zkCredentialsInjectorClass;
    this.createCollectionWaitTimeTillActive = createCollectionWaitTimeTillActive;
    this.createCollectionCheckLeaderActive = createCollectionCheckLeaderActive;
    this.pkiHandlerPrivateKeyPath = pkiHandlerPrivateKeyPath;
    this.pkiHandlerPublicKeyPath = pkiHandlerPublicKeyPath;
    this.minStateByteLenForCompression = minStateByteLenForCompression;
    this.stateCompressorClass = stateCompressorClass;

    if (this.hostPort == -1)
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "'hostPort' must be configured to run SolrCloud");
  }

  public String getZkHost() {
    return zkHost;
  }

  public int getZkClientTimeout() {
    return zkClientTimeout;
  }

  public int getSolrHostPort() {
    return hostPort;
  }

  public String getHost() {
    return hostName;
  }

  public String getZkCredentialsProviderClass() {
    return zkCredentialsProviderClass;
  }

  public String getZkACLProviderClass() {
    return zkACLProviderClass;
  }

  public String getZkCredentialsInjectorClass() {
    return zkCredentialsInjectorClass;
  }

  public int getLeaderVoteWait() {
    return leaderVoteWait;
  }

  public int getLeaderConflictResolveWait() {
    return leaderConflictResolveWait;
  }

  public int getCreateCollectionWaitTimeTillActive() {
    return createCollectionWaitTimeTillActive;
  }

  public boolean isCreateCollectionCheckLeaderActive() {
    return createCollectionCheckLeaderActive;
  }

  public String getPkiHandlerPrivateKeyPath() {
    return pkiHandlerPrivateKeyPath;
  }

  public String getPkiHandlerPublicKeyPath() {
    return pkiHandlerPublicKeyPath;
  }

  public int getMinStateByteLenForCompression() {
    return minStateByteLenForCompression;
  }

  public String getStateCompressorClass() {
    return stateCompressorClass;
  }

  public static class CloudConfigBuilder {

    private static final int DEFAULT_LEADER_VOTE_WAIT = 180000; // 3 minutes
    private static final int DEFAULT_LEADER_CONFLICT_RESOLVE_WAIT = 180000;
    private static final int DEFAULT_CREATE_COLLECTION_ACTIVE_WAIT = 45; // 45 seconds
    private static final boolean DEFAULT_CREATE_COLLECTION_CHECK_LEADER_ACTIVE = false;
    private static final int DEFAULT_MINIMUM_STATE_SIZE_FOR_COMPRESSION =
        -1; // By default compression for state is disabled

    private String zkHost;
    private int zkClientTimeout = SolrZkClientTimeout.DEFAULT_ZK_CLIENT_TIMEOUT;
    private final int hostPort;
    private final String hostName;
    private int leaderVoteWait = DEFAULT_LEADER_VOTE_WAIT;
    private int leaderConflictResolveWait = DEFAULT_LEADER_CONFLICT_RESOLVE_WAIT;
    private String zkCredentialsProviderClass;
    private String zkACLProviderClass;
    private String zkCredentialsInjectorClass;
    private int createCollectionWaitTimeTillActive = DEFAULT_CREATE_COLLECTION_ACTIVE_WAIT;
    private boolean createCollectionCheckLeaderActive =
        DEFAULT_CREATE_COLLECTION_CHECK_LEADER_ACTIVE;
    private String pkiHandlerPrivateKeyPath;
    private String pkiHandlerPublicKeyPath;
    private int minStateByteLenForCompression = DEFAULT_MINIMUM_STATE_SIZE_FOR_COMPRESSION;

    private String stateCompressorClass;

    public CloudConfigBuilder(String hostName, int hostPort) {
      this.hostName = hostName;
      this.hostPort = hostPort;
    }

    public CloudConfigBuilder setZkHost(String zkHost) {
      this.zkHost = zkHost;
      return this;
    }

    public CloudConfigBuilder setZkClientTimeout(int zkClientTimeout) {
      this.zkClientTimeout = zkClientTimeout;
      return this;
    }

    public CloudConfigBuilder setLeaderVoteWait(int leaderVoteWait) {
      this.leaderVoteWait = leaderVoteWait;
      return this;
    }

    public CloudConfigBuilder setLeaderConflictResolveWait(int leaderConflictResolveWait) {
      this.leaderConflictResolveWait = leaderConflictResolveWait;
      return this;
    }

    public CloudConfigBuilder setZkCredentialsProviderClass(String zkCredentialsProviderClass) {
      this.zkCredentialsProviderClass =
          zkCredentialsProviderClass != null ? zkCredentialsProviderClass.trim() : null;
      return this;
    }

    public CloudConfigBuilder setZkACLProviderClass(String zkACLProviderClass) {
      this.zkACLProviderClass = zkACLProviderClass != null ? zkACLProviderClass.trim() : null;
      return this;
    }

    public CloudConfigBuilder setZkCredentialsInjectorClass(String zkCredentialsInjectorClass) {
      this.zkCredentialsInjectorClass =
          zkCredentialsInjectorClass != null ? zkCredentialsInjectorClass.trim() : null;
      return this;
    }

    public CloudConfigBuilder setCreateCollectionWaitTimeTillActive(
        int createCollectionWaitTimeTillActive) {
      this.createCollectionWaitTimeTillActive = createCollectionWaitTimeTillActive;
      return this;
    }

    public CloudConfigBuilder setCreateCollectionCheckLeaderActive(
        boolean createCollectionCheckLeaderActive) {
      this.createCollectionCheckLeaderActive = createCollectionCheckLeaderActive;
      return this;
    }

    public CloudConfigBuilder setPkiHandlerPrivateKeyPath(String pkiHandlerPrivateKeyPath) {
      this.pkiHandlerPrivateKeyPath = pkiHandlerPrivateKeyPath;
      return this;
    }

    public CloudConfigBuilder setPkiHandlerPublicKeyPath(String pkiHandlerPublicKeyPath) {
      this.pkiHandlerPublicKeyPath = pkiHandlerPublicKeyPath;
      return this;
    }

    public CloudConfigBuilder setMinStateByteLenForCompression(int minStateByteLenForCompression) {
      this.minStateByteLenForCompression = minStateByteLenForCompression;
      return this;
    }

    public CloudConfigBuilder setStateCompressorClass(String stateCompressorClass) {
      this.stateCompressorClass = stateCompressorClass;
      return this;
    }

    public CloudConfig build() {
      return new CloudConfig(
          zkHost,
          zkClientTimeout,
          hostPort,
          hostName,
          leaderVoteWait,
          leaderConflictResolveWait,
          zkCredentialsProviderClass,
          zkACLProviderClass,
          zkCredentialsInjectorClass,
          createCollectionWaitTimeTillActive,
          createCollectionCheckLeaderActive,
          pkiHandlerPrivateKeyPath,
          pkiHandlerPublicKeyPath,
          minStateByteLenForCompression,
          stateCompressorClass);
    }
  }
}
