package org.apache.solr.composeui.components.environment.store

import com.arkivanov.mvikotlin.core.store.Store
import org.apache.solr.composeui.components.environment.data.JavaProperty
import org.apache.solr.composeui.components.environment.data.JvmData
import org.apache.solr.composeui.components.environment.data.SecurityConfig
import org.apache.solr.composeui.components.environment.data.SystemInformation
import org.apache.solr.composeui.components.environment.data.SystemMode
import org.apache.solr.composeui.components.environment.data.Versions
import org.apache.solr.composeui.components.environment.store.EnvironmentStore.Intent
import org.apache.solr.composeui.components.environment.store.EnvironmentStore.State

internal interface EnvironmentStore : Store<Intent, State, Nothing> {

    sealed interface Intent {

        data object FetchSystemData: Intent
    }

    data class State(
        val mode: SystemMode = SystemMode.Unknown,
        val zkHost: String = "",
        val solrHome: String = "",
        val coreRoot: String = "",
        val lucene: Versions = Versions(),
        val jvm: JvmData = JvmData(),
        val security: SecurityConfig = SecurityConfig(),
        val system: SystemInformation = SystemInformation(),
        val node: String = "",
        val javaProperties: List<JavaProperty> = emptyList(),
    )
}
