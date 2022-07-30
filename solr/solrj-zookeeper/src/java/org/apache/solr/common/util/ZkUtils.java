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

package org.apache.solr.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
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

  public static InputStream toJavabin(Object o) throws IOException {
    try (final JavaBinCodec jbc = new JavaBinCodec()) {
      BinaryRequestWriter.BAOS baos = new BinaryRequestWriter.BAOS();
      jbc.marshal(o, baos);
      return new ByteBufferInputStream(ByteBuffer.wrap(baos.getbuf(), 0, baos.size()));
    }
  }
}
