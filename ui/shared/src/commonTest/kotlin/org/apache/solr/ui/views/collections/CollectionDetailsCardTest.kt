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
 */package org.apache.solr.ui.views.collections

import androidx.compose.ui.test.ExperimentalTestApi
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.v2.runComposeUiTest
import org.apache.solr.ui.domain.CollectionDetails
import org.apache.solr.ui.shared.generated.resources.Res
import org.apache.solr.ui.shared.generated.resources.collection_details
import org.apache.solr.ui.shared.generated.resources.label_shard_count
import org.apache.solr.ui.shared.generated.resources.state_enabled
import org.jetbrains.compose.resources.getString
import kotlin.test.Test


class CollectionDetailsCardTest {

    private val details = CollectionDetails(
        name = "films",
        configName = "_default",
        shardCount = 2,
        replicationFactor = 3,
        routerName = "compositeId",
        maxShardsPerNode = 5,
        autoAddReplicas = false,
    )

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testShowsCollectionDetails() = runComposeUiTest {
        getString(Res.string.collection_details)
        getString(Res.string.label_shard_count)
        setContent { CollectionDetailsCard(details) }
        awaitIdle()

        onNodeWithText("Collection Details").assertExists()
        onNodeWithText("films").assertExists()
        onNodeWithText("_default").assertExists()
        onNodeWithText("compositeId").assertExists()
        onNodeWithText("Shards").assertExists()
        onNodeWithText("2").assertExists()
        onNodeWithText("3").assertExists()
        onNodeWithText("5").assertExists()
    }

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testShowsAutoAddReplicasState() = runComposeUiTest {
        getString(Res.string.state_enabled)
        setContent { CollectionDetailsCard(details.copy(autoAddReplicas = true)) }
        awaitIdle()

        onNodeWithText("Enabled").assertExists()
    }
}
