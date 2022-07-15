package org.apache.solr.common.util;

import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkOperation;
import org.apache.zookeeper.KeeperException;

import java.util.Collections;
import java.util.Map;

public class ZkUtils {
    /**
     * Assumes data in ZooKeeper is a JSON string, deserializes it and returns as a Map
     *
     * @param zkClient the zookeeper client
     * @param path the path to the znode being read
     * @param retryOnConnLoss whether to retry the operation automatically on connection loss, see
     *     {@link org.apache.solr.common.cloud.ZkCmdExecutor#retryOperation(ZkOperation)}
     * @return a Map if the node exists and contains valid JSON or an empty map if znode does not
     *     exist or has a null data
     */
    @SuppressWarnings({"unchecked"})
    public static Map<String, Object> getJson(
            SolrZkClient zkClient, String path, boolean retryOnConnLoss)
        throws KeeperException, InterruptedException {
      try {
        byte[] bytes = zkClient.getData(path, null, null, retryOnConnLoss);
        if (bytes != null && bytes.length > 0) {
          return (Map<String, Object>) Utils.fromJSON(bytes);
        }
      } catch (KeeperException.NoNodeException e) {
        return Collections.emptyMap();
      }
      return Collections.emptyMap();
    }
}
