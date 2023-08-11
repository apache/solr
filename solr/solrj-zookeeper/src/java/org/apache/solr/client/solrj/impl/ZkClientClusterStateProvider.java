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

package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.solr.client.solrj.impl.SolrZkClientTimeout.SolrZkClientTimeoutAware;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.PerReplicaStatesOps;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.noggit.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Retrieves cluster state from Zookeeper */
public class ZkClientClusterStateProvider
    implements ClusterStateProvider, SolrZkClientTimeoutAware {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  volatile ZkStateReader zkStateReader;
  private boolean closeZkStateReader = true;
  private final String zkHost;
  private int zkConnectTimeout = SolrZkClientTimeout.DEFAULT_ZK_CONNECT_TIMEOUT;
  private int zkClientTimeout = SolrZkClientTimeout.DEFAULT_ZK_CLIENT_TIMEOUT;

  private volatile boolean isClosed = false;

  /** Extracts this from the client, or throws an exception if of the wrong type. */
  public static ZkClientClusterStateProvider from(CloudSolrClient client) {
    if (client.getClusterStateProvider() instanceof ZkClientClusterStateProvider) {
      return (ZkClientClusterStateProvider) client.getClusterStateProvider();
    }
    throw new IllegalArgumentException("This client does not use ZK");
  }

  public ZkClientClusterStateProvider(ZkStateReader zkStateReader) {
    this.zkStateReader = zkStateReader;
    this.closeZkStateReader = false;
    this.zkHost = null;
  }

  public ZkClientClusterStateProvider(Collection<String> zkHosts, String chroot) {
    zkHost = buildZkHostString(zkHosts, chroot);
  }

  public ZkClientClusterStateProvider(String zkHost) {
    this.zkHost = zkHost;
  }

  /**
   * Create a ClusterState from Json. This method supports legacy configName location
   *
   * @param bytes a byte array of a Json representation of a mapping from collection name to the
   *     Json representation of a {@link DocCollection} as written by {@link
   *     ClusterState#write(JSONWriter)}. It can represent one or more collections.
   * @param liveNodes list of live nodes
   * @param coll collection name
   * @param zkClient ZK client
   * @return the ClusterState
   */
  @SuppressWarnings({"unchecked"})
  @Deprecated
  public static ClusterState createFromJsonSupportingLegacyConfigName(
      int version, byte[] bytes, Set<String> liveNodes, String coll, SolrZkClient zkClient) {
    if (bytes == null || bytes.length == 0) {
      return new ClusterState(liveNodes, Collections.emptyMap());
    }
    Map<String, Object> stateMap = (Map<String, Object>) Utils.fromJSON(bytes);
    Map<String, Object> props = (Map<String, Object>) stateMap.get(coll);
    if (props != null) {
      if (!props.containsKey(ZkStateReader.CONFIGNAME_PROP)) {
        try {
          // read configName from collections/collection node
          String path = ZkStateReader.COLLECTIONS_ZKNODE + "/" + coll;
          byte[] data = zkClient.getData(path, null, null, true);
          if (data != null && data.length > 0) {
            ZkNodeProps configProp = ZkNodeProps.load(data);
            String configName = configProp.getStr(ZkStateReader.CONFIGNAME_PROP);
            if (configName != null) {
              props.put(ZkStateReader.CONFIGNAME_PROP, configName);
              stateMap.put(coll, props);
            } else {
              log.warn("configName is null, not found on {}", path);
            }
          }
        } catch (KeeperException | InterruptedException e) {
          // do nothing
        }
      }
    }
    return ClusterState.createFromCollectionMap(
        version,
        stateMap,
        liveNodes,
        PerReplicaStatesOps.getZkClientPrsSupplier(
            zkClient, DocCollection.getCollectionPath(coll)));
  }

  @Override
  public ClusterState.CollectionRef getState(String collection) {
    ClusterState clusterState = getZkStateReader().getClusterState();
    if (clusterState != null) {
      return clusterState.getCollectionRef(collection);
    } else {
      return null;
    }
  }

  @Override
  public Set<String> getLiveNodes() {
    ClusterState clusterState = getZkStateReader().getClusterState();
    if (clusterState != null) {
      return clusterState.getLiveNodes();
    } else {
      return Collections.emptySet();
    }
  }

  @Override
  public List<String> resolveAlias(String alias) {
    return getZkStateReader().getAliases().resolveAliases(alias); // if not an alias, returns itself
  }

  @Override
  public Map<String, String> getAliasProperties(String alias) {
    return getZkStateReader().getAliases().getCollectionAliasProperties(alias);
  }

  @Override
  public String resolveSimpleAlias(String alias) throws IllegalArgumentException {
    return getZkStateReader().getAliases().resolveSimpleAlias(alias);
  }

  @Override
  public Object getClusterProperty(String propertyName) {
    Map<String, Object> props = getZkStateReader().getClusterProperties();
    return props.get(propertyName);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getClusterProperty(String propertyName, T def) {
    Map<String, Object> props = getZkStateReader().getClusterProperties();
    if (props.containsKey(propertyName)) {
      return (T) props.get(propertyName);
    }
    return def;
  }

  @Override
  public ClusterState getClusterState() {
    return getZkStateReader().getClusterState();
  }

  @Override
  public Map<String, Object> getClusterProperties() {
    return getZkStateReader().getClusterProperties();
  }

  @Override
  public String getPolicyNameByCollection(String coll) {
    ClusterState.CollectionRef state = getState(coll);
    return state == null || state.get() == null
        ? null
        : (String) state.get().getProperties().get("policy");
  }

  @Override
  public void connect() {
    // Esentially a No-Op, but force a check that we're not closed and the ZkStateReader is
    // available...
    final ZkStateReader ignored = getZkStateReader();
  }

  public ZkStateReader getZkStateReader() {
    if (isClosed) { // quick check...
      throw new AlreadyClosedException();
    }
    if (zkStateReader == null) {
      synchronized (this) {
        if (isClosed) { // while we were waiting for sync lock another thread may have closed
          throw new AlreadyClosedException();
        }
        if (zkStateReader == null) {
          ZkStateReader zk = null;
          try {
            zk = new ZkStateReader(zkHost, zkClientTimeout, zkConnectTimeout);
            zk.createClusterStateWatchersAndUpdate();
            log.info("Cluster at {} ready", zkHost);
            zkStateReader = zk;
          } catch (InterruptedException e) {
            zk.close();
            Thread.currentThread().interrupt();
            throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
          } catch (KeeperException e) {
            zk.close();
            throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
          } catch (Exception e) {
            if (zk != null) zk.close();
            // do not wrap because clients may be relying on the underlying exception being thrown
            throw e;
          }
        }
      }
    }
    return zkStateReader;
  }

  @Override
  public void close() throws IOException {
    synchronized (this) {
      if (false == isClosed && zkStateReader != null) {
        isClosed = true;

        // force zkStateReader to null first so that any parallel calls drop into the synch block
        // getZkStateReader() as soon as possible.
        final ZkStateReader zkToClose = zkStateReader;
        zkStateReader = null;
        if (closeZkStateReader) {
          zkToClose.close();
        }
      }
    }
  }

  static String buildZkHostString(Collection<String> zkHosts, String chroot) {
    if (zkHosts == null || zkHosts.isEmpty()) {
      throw new IllegalArgumentException(
          "Cannot create CloudSearchClient without valid ZooKeeper host; none specified!");
    }

    StringBuilder zkBuilder = new StringBuilder();
    int lastIndexValue = zkHosts.size() - 1;
    int i = 0;
    for (String zkHost : zkHosts) {
      zkBuilder.append(zkHost);
      if (i < lastIndexValue) {
        zkBuilder.append(",");
      }
      i++;
    }
    if (chroot != null) {
      if (chroot.startsWith("/")) {
        zkBuilder.append(chroot);
      } else {
        throw new IllegalArgumentException("The chroot must start with a forward slash.");
      }
    }

    /* Log the constructed connection string and then initialize. */
    final String zkHostString = zkBuilder.toString();
    log.debug("Final constructed zkHost string: {}", zkHostString);
    return zkHostString;
  }

  @Override
  public String getQuorumHosts() {
    return getZkStateReader().getZkClient().getZkServerAddress();
  }

  @Override
  public String toString() {
    return zkHost;
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  /**
   * @return the zkHost value used to connect to zookeeper.
   */
  public String getZkHost() {
    return zkHost;
  }

  public int getZkConnectTimeout() {
    return zkConnectTimeout;
  }

  /** Set the connect timeout to the zookeeper ensemble in ms */
  @Override
  public void setZkConnectTimeout(int zkConnectTimeout) {
    this.zkConnectTimeout = zkConnectTimeout;
  }

  public int getZkClientTimeout() {
    return zkClientTimeout;
  }

  /** Set the timeout to the zookeeper ensemble in ms */
  @Override
  public void setZkClientTimeout(int zkClientTimeout) {
    this.zkClientTimeout = zkClientTimeout;
  }
}
