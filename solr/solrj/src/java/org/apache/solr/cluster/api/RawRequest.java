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

package org.apache.solr.cluster.api;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.Replica;

/**
 * A plain interface that captures all inputs for a Solr request. As the name suggests, it lets
 * users interact with Solr using raw bytes and params instead of concrete objects.
 *
 * * <pre> {@code
 * public class C {
 *     //fields go here
 * }
 *  public class D {
 *       // class for payload
 *  }
 *
 * void serializeJackson(OutputStream os, Object o) {
 *    // code to serialize an object using jackson (or you may use your favorite library)
 *   }
 * <T> T deserializeJackson(InputStream in, Class<T> c) {
 *       //code to deserialize an object from jackson (or you may use your favorite library)
 *   }
 *  CloudHttp2SolrClient client = null;
 *  C c = client.<C>createRequest()
 *                  .withNode("nodeName")
 *                  .withPath("/admin/metrics")
 *                  .withParser(in -> deserializeJackson(in, C.class))
 *                  .withParams(p -> p.
 *                          .add(CommonParams.OMIT_HEADER, CommonParams.TRUE)
 *                          .add("key", "solr.jvm:system.properties:java.version"))
 *                          .add("wt", "json"))
 *             .GET();
 *
 *  client.<C>createRequest()
 *             .withCollection("coll-name")
 *             .withReplica(r ->
 *                     r.shardKey("id1234")
 *                     .onlyLeader())
 *             .withPath("/update/json")
 *             .withParser(in -> deserializeJackson(in, C.class))
 *             .withPayload(os -> serializeJackson(os, new D());)
 *             .withParams(p-> p.add(CommonParams.OMIT_HEADER, CommonParams.TRUE))
 *             .POST();
 *
 *
 *  }</pre>
 *
 * @param <T> The concrete return type object
 */
public interface RawRequest<T> {

  /** Use /solr or /api end points */
  RawRequest<T> withApiType(ApiType apiType);

  /**
   * Make a request to a specific Solr node
   *
   * @param node node name
   */
  RawRequest<T> withNode(String node);

  RawRequest<T> withReplica(Consumer<ReplicaLocator> replicaLocator);
  /**
   * Make a request to a specific collection
   *
   * @param collection collection name
   */
  RawRequest<T> withCollection(String collection);

  /**
   * The path to which the request needs to be made eg: /update , /admin/metrics etc. The actual
   * path depends on whether a collection is specified or not
   *
   * @param path The path
   */
  RawRequest<T> withPath(String path);

  /** The request parameters */
  RawRequest<T> withParams(Consumer<Params> params);

  /** If there is a payload, write it directly to the server */
  RawRequest<T> withPayload(Consumer<OutputStream> os);

  /**
   * Parse and deserialize a concrete object . If this is not supplied, the response is just eaten
   * up and thrown away
   *
   * @param parser This impl should consume an input stream and return an object
   */
  RawRequest<T> withParser(Function<InputStream, T> parser);

  /**
   * do an HTTP GET operation
   *
   * @return the parsed object as returned by the parser. A null is returned if there is no parser
   *     set.
   */
  T GET();

  /**
   * do an HTTP POST operation
   *
   * @return the parsed object as returned by the parser. A null is returned if there is no parser
   *     set.
   */
  T POST();

  /**
   * do an HTTP DELETE operation
   *
   * @return the parsed object as returned by the parser. A null is returned if there is no parser
   *     set.
   */
  T DELETE();

  /**
   * do an HTTP PUT operation
   *
   * @return the parsed object as returned by the parser. A null is returned if there is no parser
   *     set.
   */
  T PUT();

  interface Params {
    Params add(String key, String val);

    Params add(MapWriter mw);
  }

  interface ReplicaLocator {
    /**
     * We know the exact replica of the collection to which the request needs to be sent
     * @param replicaName the name of the replica
     */
    ReplicaLocator replicaName(String replicaName);

    /**
     * We do not know the name of the shard. Let the system hash the key and find out the shard
     * @param key the routing key. usually the id of the doc
     */
    ReplicaLocator shardKey(String key) ;
    /**
     * The shard to which the replica is to be sent
     * @param shardName name of the shard
     */
    ReplicaLocator shardName(String shardName);

    /**
     * Always chose the leader
     */
    ReplicaLocator onlyLeader() ;

    /**
     * Send to a replica that is not a leader
     */
    ReplicaLocator onlyFollower();


    /**
     * Send this request to a specific replica type
     * @param type
     */
    ReplicaLocator replicaType(Replica.Type type) ;
  }
}
