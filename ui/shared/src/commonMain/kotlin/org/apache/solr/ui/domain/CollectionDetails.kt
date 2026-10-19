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
 */package org.apache.solr.ui.domain

/**
 * Details of a collection as shown in the collection details card.
 *
 * @property name The name and unique identifier of the collection.
 * @property configName The configset the collection uses.
 * @property shardCount The number of shards.
 * @property replicationFactor The number of replicas per shard.
 * @property routerName The name of the document router.
 * @property maxShardsPerNode The maximum number of shards of this collection per node.
 * @property autoAddReplicas Whether replicas are added automatically when a node is lost.
 */
data class CollectionDetails(
    val name: String,
    val configName: String,
    val shardCount: Int,
    val replicationFactor: Int,
    val routerName: String,
    val maxShardsPerNode: Int,
    val autoAddReplicas: Boolean,
)
