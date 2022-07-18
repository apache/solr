package org.apache.solr.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.VersionedData;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkOperation;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.ByteBufferInputStream;

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

  @SuppressWarnings({"unchecked"})
  public static Map<String, Object> getJson(DistribStateManager distribStateManager, String path)
      throws InterruptedException, IOException, KeeperException {
    VersionedData data;
    try {
      data = distribStateManager.getData(path);
    } catch (KeeperException.NoNodeException | NoSuchElementException e) {
      return Collections.emptyMap();
    }
    if (data == null || data.getData() == null || data.getData().length == 0)
      return Collections.emptyMap();
    return (Map<String, Object>) Utils.fromJSON(data.getData());
  }

  public static InputStream toJavabin(Object o) throws IOException {
    try (final JavaBinCodec jbc = new JavaBinCodec()) {
      BinaryRequestWriter.BAOS baos = new BinaryRequestWriter.BAOS();
      jbc.marshal(o, baos);
      return new ByteBufferInputStream(ByteBuffer.wrap(baos.getbuf(), 0, baos.size()));
    }
  }
}
