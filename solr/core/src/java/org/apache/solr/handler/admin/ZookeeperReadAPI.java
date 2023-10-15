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

import static org.apache.solr.response.RawResponseWriter.CONTENT;
import static org.apache.solr.security.PermissionNameProvider.Name.SECURITY_READ_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.ZK_READ_PERM;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.Parameter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.api.AdminAPIBase;
import org.apache.solr.jersey.ExperimentalResponse;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.RawResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * Exposes the content of the Zookeeper This is an expert feature that exposes the data inside the
 * back end zookeeper.This API may change or be removed in future versions. This is not a public
 * API. The data that is returned is not guaranteed to remain same across releases, as the data
 * stored in Zookeeper may change from time to time.
 *
 * @lucene.experimental
 */
@Path("/cluster/zookeeper/")
public class ZookeeperReadAPI extends AdminAPIBase {
  @Inject
  public ZookeeperReadAPI(
      CoreContainer coreContainer, SolrQueryRequest req, SolrQueryResponse rsp) {
    super(coreContainer, req, rsp);
  }

  /** Request contents of a znode, except security.json */
  @GET
  @Path("/data{zkPath:.+}")
  @Produces({RawResponseWriter.CONTENT_TYPE, MediaType.APPLICATION_JSON})
  @PermissionName(ZK_READ_PERM)
  public ZooKeeperFileResponse readNode(
      @Parameter(description = "The path of the node to read from ZooKeeper") @PathParam("zkPath")
          String zkPath) {
    zkPath = sanitizeZkPath(zkPath);
    return readNodeAndAddToResponse(zkPath);
  }

  /** Request contents of the security.json node */
  @GET
  @Path("/data/security.json")
  @Produces({RawResponseWriter.CONTENT_TYPE, MediaType.APPLICATION_JSON})
  @PermissionName(SECURITY_READ_PERM)
  public ZooKeeperFileResponse readSecurityJsonNode() {
    return readNodeAndAddToResponse("/security.json");
  }

  private String sanitizeZkPath(String zkPath) {
    if (zkPath == null || zkPath.isEmpty()) {
      return "/";
    } else if (zkPath.length() > 1 && zkPath.endsWith("/")) {
      return zkPath.substring(0, zkPath.length() - 1);
    }

    return zkPath;
  }

  /** List the children of a certain zookeeper znode */
  @GET
  @Path("/children{zkPath:.*}")
  @Produces({"application/json", "application/javabin"})
  @PermissionName(ZK_READ_PERM)
  public ListZkChildrenResponse listNodes(
      @Parameter(description = "The path of the ZooKeeper node to stat and list children of")
          @PathParam("zkPath")
          String zkPath,
      @Parameter(
              description =
                  "Controls whether stat information for child nodes is included in the response. 'true' by default.")
          @QueryParam("children")
          Boolean includeChildren)
      throws Exception {
    final ListZkChildrenResponse listResponse =
        instantiateJerseyResponse(ListZkChildrenResponse.class);

    zkPath = sanitizeZkPath(zkPath);
    try {
      Stat stat = coreContainer.getZkController().getZkClient().exists(zkPath, null, true);
      listResponse.stat = new AnnotatedStat(stat);
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

      final Map<String, AnnotatedStat> childStats = new HashMap<>();
      for (Map.Entry<String, Stat> e : stats.entrySet()) {
        childStats.put(e.getKey(), new AnnotatedStat(e.getValue()));
      }
      listResponse.unknownFields.put(zkPath, childStats);

      return listResponse;
    } catch (KeeperException.NoNodeException e) {
      throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such node :" + zkPath);
    }
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
  private ZooKeeperFileResponse readNodeAndAddToResponse(String zkPath) {
    final ZooKeeperFileResponse zkFileResponse =
        instantiateJerseyResponse(ZooKeeperFileResponse.class);

    byte[] d = readPathFromZookeeper(zkPath);
    if (d == null || d.length == 0) {
      zkFileResponse.zkData = EMPTY;
      return zkFileResponse;
    }

    zkFileResponse.output = new ContentStreamBase.ByteArrayStream(d, null, guessMime(d[0]));
    return zkFileResponse;
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

  public static class ListZkChildrenResponse extends ExperimentalResponse {
    @JsonProperty("stat")
    public AnnotatedStat stat;

    // TODO Currently the list response (when child information is fetched) consists primarily of an
    //  object with only one key - the name of the root node - with separate objects under there for
    //  each child.  The additional nesting under the root node doesn't serve much purpose afaict
    //  and should be removed.
    private Map<String, Map<String, AnnotatedStat>> unknownFields = new HashMap<>();

    @JsonAnyGetter
    public Map<String, Map<String, AnnotatedStat>> unknownProperties() {
      return unknownFields;
    }

    @JsonAnySetter
    public void setUnknownProperty(String field, Map<String, AnnotatedStat> value) {
      unknownFields.put(field, value);
    }
  }

  public static class AnnotatedStat implements JacksonReflectMapWriter {
    @JsonProperty("version")
    public int version;

    @JsonProperty("aversion")
    public int aversion;

    @JsonProperty("children")
    public int children;

    @JsonProperty("ctime")
    public long ctime;

    @JsonProperty("cversion")
    public int cversion;

    @JsonProperty("czxid")
    public long czxid;

    @JsonProperty("ephemeralOwner")
    public long ephemeralOwner;

    @JsonProperty("mtime")
    public long mtime;

    @JsonProperty("mzxid")
    public long mzxid;

    @JsonProperty("pzxid")
    public long pzxid;

    @JsonProperty("dataLength")
    public int dataLength;

    public AnnotatedStat(Stat stat) {
      this.version = stat.getVersion();
      this.aversion = stat.getAversion();
      this.children = stat.getNumChildren();
      this.ctime = stat.getCtime();
      this.cversion = stat.getCversion();
      this.czxid = stat.getCzxid();
      this.ephemeralOwner = stat.getEphemeralOwner();
      this.mtime = stat.getMtime();
      this.mzxid = stat.getMzxid();
      this.pzxid = stat.getPzxid();
      this.dataLength = stat.getDataLength();
    }

    public AnnotatedStat() {}
  }

  private static final String EMPTY = "empty";

  public static class ZooKeeperFileResponse extends SolrJerseyResponse {
    @JsonProperty(CONTENT) // A flag value that RawResponseWriter handles specially
    public ContentStream output;

    @JsonProperty("zkData")
    public String zkData;
  }
}
