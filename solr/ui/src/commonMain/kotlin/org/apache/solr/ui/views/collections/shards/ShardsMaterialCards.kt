package org.apache.solr.ui.views.collections.shards

import androidx.compose.foundation.LocalScrollbarStyle
import androidx.compose.foundation.VerticalScrollbar
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.rememberScrollbarAdapter
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import kotlin.collections.component1
import kotlin.collections.component2
import org.apache.solr.ui.components.collections.data.Shard
import org.apache.solr.ui.views.components.SolrCard

@Composable
fun ShardsMaterialCards(
    shards: Map<String, Shard>,
    modifier: Modifier = Modifier,
    resetKey: Any? = null,
    liveNodes: List<String> = emptyList(),
    onFetchLiveNodes: () -> Unit = {},
    onAddReplica: (shardName: String, node: String?, type: String) -> Unit = { _, _, _ -> },
    onDeleteReplica: (shardName: String, replica: String) -> Unit = { _, _ -> },
) = SolrCard(modifier) {
    Text(
        "Shards",
        style = MaterialTheme.typography.headlineSmall,
        color = MaterialTheme.colorScheme.onSurfaceVariant,
    )
    Spacer(Modifier.height(8.dp))

    val shardEntries = remember(shards) {
        shards.entries.sortedBy { entry ->
            entry.key.replace("shard", "").toIntOrNull() ?: Int.MAX_VALUE
        }
    }

    var expanded by remember(resetKey) { mutableStateOf<String?>(null) }

    // optional guard if a shard disappears
    LaunchedEffect(shards, expanded) {
        if (expanded != null && expanded !in shards.keys) expanded = null
    }
    val listState = rememberLazyListState()
    Column(Modifier.fillMaxSize()) {
        Box(Modifier.weight(1f)) {
            LazyColumn(
                state = listState,
                modifier = Modifier.fillMaxSize().padding(end = 12.dp),
                verticalArrangement = Arrangement.spacedBy(12.dp),
                contentPadding = PaddingValues(end = 8.dp, bottom = 12.dp), // room beside scrollbar
            ) {
                items(shardEntries, key = { it.key }) { (shardName, shard) ->
                    ShardCard(
                        shardName = shardName,
                        shard = shard,
                        expanded = expanded == shardName,
                        onToggle = { expanded = if (expanded == shardName) null else shardName },
                        liveNodes = liveNodes,
                        onFetchLiveNodes = onFetchLiveNodes,
                        onAddReplica = onAddReplica,
                        onDeleteReplica = onDeleteReplica,
                    )
                }
            }

            // Desktop scrollbar
            CompositionLocalProvider(
                LocalScrollbarStyle provides LocalScrollbarStyle.current.copy(
                    thickness = 8.dp,
                    minimalHeight = 28.dp,
                ),
            ) {
                VerticalScrollbar(
                    adapter = rememberScrollbarAdapter(listState),
                    modifier = Modifier
                        .align(Alignment.CenterEnd)
                        .fillMaxHeight()
                        .padding(vertical = 12.dp),
                )
            }
        }
    }
}
