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

import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.apache.solr.common.util.NamedList;

/**
 * A container for recording shard to shard info mappings.
 *
 * @see org.apache.solr.common.params.ShardParams#SHARDS_INFO
 */
public abstract class ShardsInfoContainer
    implements BiConsumer<String, NamedList<Object>>, Supplier<Object> {

  /**
   * Records detailed match info for a specific shard.
   *
   * @param shardInfoName the shard
   * @param shardInfoValue detailed match info for the shard
   */
  @Override
  public abstract void accept(String shardInfoName, NamedList<Object> shardInfoValue);

  /**
   * Returns a representation of all recorded mappings.
   *
   * @return mappings representation
   */
  @Override
  public abstract Object get();
}
