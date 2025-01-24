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

package org.apache.solr.handler.admin;

import static org.apache.solr.security.PermissionNameProvider.Name.SECURITY_READ_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.ZK_READ_PERM;

import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.api.endpoint.ZooKeeperReadApis;
import org.apache.solr.client.api.model.ZooKeeperListChildrenResponse;
import org.apache.solr.client.api.model.ZooKeeperStat;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.api.AdminAPIBase;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * v2 API definition exposing read-content in Zookeeper.
 *
 * <p>This is an expert feature that exposes the data inside the back end zookeeper.This API may
 * change or be removed in future versions. This is not a public API. The data that is returned is
 * not guaranteed to remain same across releases, as the data stored in Zookeeper may change from
 * time to time.
 *
 * @lucene.experimental
 */
public class ZookeeperRead extends AdminAPIBase implements ZooKeeperReadApis {

  private static final String EMPTY = "empty";

  @Inject
  public ZookeeperRead(CoreContainer coreContainer, SolrQueryRequest req, SolrQueryResponse rsp) {
    super(coreContainer, req, rsp);
  }

  /** Request contents of a znode, except security.json */
  @Override
  @PermissionName(ZK_READ_PERM)
  public StreamingOutput readNode(String zkPath) {
    zkPath = sanitizeZkPath(zkPath);
    return readNodeAndAddToResponse(zkPath);
  }

  /** Request contents of the security.json node */
  @Override
  @PermissionName(SECURITY_READ_PERM)
  public StreamingOutput readSecurityJsonNode() {
    return readNodeAndAddToResponse("/security.json");
  }

  /** List the children of a certain zookeeper znode */
  @Override
  @PermissionName(ZK_READ_PERM)
  public ZooKeeperListChildrenResponse listNodes(String zkPath, Boolean includeChildren)
      throws Exception {
    final ZooKeeperListChildrenResponse listResponse =
        instantiateJerseyResponse(ZooKeeperListChildrenResponse.class);

    zkPath = sanitizeZkPath(zkPath);
    try {
      Stat stat = coreContainer.getZkController().getZkClient().exists(zkPath, null, true);
      listResponse.stat = createAnnotatedStatFrom(stat);
      if (includeChildren != null && !includeChildren.booleanValue()) {
        return listResponse;
      }
      List<String> l =
          coreContainer.getZkController().getZkClient().getChildren(zkPath, null, false);
      String prefix = zkPath.endsWith("/") ? zkPath : zkPath + "/";

      Map<String, Stat> stats = new LinkedHashMap<>();
      for (String s : l) {
        try {
          stats.put(
              s, coreContainer.getZkController().getZkClient().exists(prefix + s, null, false));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      final Map<String, ZooKeeperStat> childStats = new HashMap<>();
      for (Map.Entry<String, Stat> e : stats.entrySet()) {
        childStats.put(e.getKey(), createAnnotatedStatFrom(e.getValue()));
      }
      listResponse.unknownFields.put(zkPath, childStats);

      return listResponse;
    } catch (KeeperException.NoNodeException e) {
      throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such node :" + zkPath);
    }
  }

  private String sanitizeZkPath(String zkPath) {
    if (zkPath == null || zkPath.isEmpty()) {
      return "/";
    } else if (zkPath.length() > 1 && zkPath.endsWith("/")) {
      return zkPath.substring(0, zkPath.length() - 1);
    }

    return zkPath;
  }

  /** Simple mime type guessing based on first character of the response */
  private String guessMime(byte firstByte) {
    switch (firstByte) {
      case '{':
        return CommonParams.JSON_MIME;
      case '<':
      case '?':
        return XMLResponseParser.XML_CONTENT_TYPE;
      default:
        return BinaryResponseParser.BINARY_CONTENT_TYPE;
    }
  }

  /** Reads content of a znode */
  private StreamingOutput readNodeAndAddToResponse(String zkPath) {
    byte[] d = readPathFromZookeeper(zkPath);
    if (d == null || d.length == 0) {
      d = new byte[0];
    }

    final var bytesToWrite = d;
    return new StreamingOutput() {
      @Override
      public void write(OutputStream output) throws IOException, WebApplicationException {
        output.write(bytesToWrite);
      }
    };
  }

  /** Reads a single node from zookeeper and return as byte array */
  private byte[] readPathFromZookeeper(String path) {
    byte[] d;
    try {
      d = coreContainer.getZkController().getZkClient().getData(path, null, null, false);
    } catch (KeeperException.NoNodeException e) {
      throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such node: " + path);
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unexpected error", e);
    }
    return d;
  }

  public static ZooKeeperStat createAnnotatedStatFrom(Stat stat) {
    final var annotatedStat = new ZooKeeperStat();
    annotatedStat.version = stat.getVersion();
    annotatedStat.aversion = stat.getAversion();
    annotatedStat.children = stat.getNumChildren();
    annotatedStat.ctime = stat.getCtime();
    annotatedStat.cversion = stat.getCversion();
    annotatedStat.czxid = stat.getCzxid();
    annotatedStat.ephemeralOwner = stat.getEphemeralOwner();
    annotatedStat.mtime = stat.getMtime();
    annotatedStat.mzxid = stat.getMzxid();
    annotatedStat.pzxid = stat.getPzxid();
    annotatedStat.dataLength = stat.getDataLength();

    return annotatedStat;
  }
}
