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

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;

/**
 * A container for recording shard to shard info mappings in a {@link NamedList} container.
 *
 * @see org.apache.solr.common.params.ShardParams#SHARDS_INFO
 */
public class DefaultShardsInfoContainer extends ShardsInfoContainer {

  private final NamedList<Object> container = new SimpleOrderedMap<>();

  @Override
  public void accept(String shardInfoName, NamedList<Object> shardInfoValue) {
    container.add(shardInfoName, shardInfoValue);
  }

  @Override
  public Object get() {
    return container;
  }
}
