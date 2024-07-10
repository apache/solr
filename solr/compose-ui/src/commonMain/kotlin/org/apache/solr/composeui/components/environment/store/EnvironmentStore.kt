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

/**
 * State store interface of the environment.
 *
 * Implementations of this state store manage detailed information of the environment.
 */
internal interface EnvironmentStore : Store<Intent, State, Nothing> {

    /**
     * Intent for interacting with the environment store.
     */
    sealed interface Intent {

        /**
         * Intent for requesting system data.
         */
        data object FetchSystemData: Intent
    }

    /**
     * State class that holds the data of the [EnvironmentStore].
     */
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
