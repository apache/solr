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
package org.apache.solr.handler.component;

import static org.apache.solr.common.params.CommonParams.DISTRIB;
import static org.apache.solr.common.params.CommonParams.INDENT;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;

/**
 * Executes, tracks, and awaits all shard-requests made in the course of a distributed request.
 *
 * <p>New ShardHandler instances are created for each individual distributed request, and should not
 * be assumed to be thread-safe.
 */
public abstract class ShardHandler {

  /**
   * Bootstraps any data structures needed by the ShardHandler to execute or track outgoing
   * requests.
   *
   * @param rb provides access to request and response state.
   */
  public abstract void prepDistributed(ResponseBuilder rb);

  /**
   * Sends a request (represented by <code>sreq</code>) to the specified shard.
   *
   * <p>The outgoing request may be sent asynchronously. Callers must invoke {@link
   * #takeCompletedIncludingErrors()} or {@link #takeCompletedOrError()} to inspect the success or
   * failure of requests.
   *
   * @param sreq metadata about the series of sub-requests that the outgoing request belongs to and
   *     should be tracked with.
   * @param shard URLs for replicas of the receiving shard, delimited by '|' (e.g.
   *     "http://solr1:8983/solr/foo1|http://solr2:7574/solr/foo2")
   * @param params query-parameters set on the outgoing request
   */
  public abstract void submit(ShardRequest sreq, String shard, ModifiableSolrParams params);

  /**
   * returns a ShardResponse of the last response correlated with a ShardRequest. This won't return
   * early if it runs into an error.
   */
  public abstract ShardResponse takeCompletedIncludingErrors();

  // TODO - Shouldn't this method be taking in a ShardRequest?  Does ShardHandler not really
  // distinguish between different ShardRequest objects as it seems to advertise? What's going on
  // here?
  /**
   * returns a ShardResponse of the last response correlated with a ShardRequest, or immediately
   * returns a ShardResponse if there was an error detected
   */
  public abstract ShardResponse takeCompletedOrError();

  /** Cancels all uncompleted requests managed by this instance */
  public abstract void cancelAll();

  public abstract ShardHandlerFactory getShardHandlerFactory();

  public static void setShardAttributesToParams(ModifiableSolrParams params, int purpose) {
    params.remove(ShardParams.SHARDS); // not a top-level request
    params.set(DISTRIB, Boolean.FALSE.toString()); // not a top-level request
    params.remove(INDENT);
    params.remove(CommonParams.HEADER_ECHO_PARAMS);
    params.set(ShardParams.IS_SHARD, true); // a sub (shard) request
    params.set(ShardParams.SHARDS_PURPOSE, purpose);
    params.set(CommonParams.OMIT_HEADER, false);
  }
}
