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

public abstract class ShardHandler {
  public abstract void prepDistributed(ResponseBuilder rb);

  public abstract void submit(ShardRequest sreq, String shard, ModifiableSolrParams params);

  public abstract ShardResponse takeCompletedIncludingErrors();

  public abstract ShardResponse takeCompletedIncludingErrorsWithTimeout(
      long maxAllowedTimeInMillis);

  public abstract ShardResponse takeCompletedOrError();

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
