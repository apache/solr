package org.apache.solr.composeui.components.environment.store

import com.arkivanov.mvikotlin.core.store.Reducer
import com.arkivanov.mvikotlin.core.store.SimpleBootstrapper
import com.arkivanov.mvikotlin.core.store.Store
import com.arkivanov.mvikotlin.core.store.StoreFactory
import com.arkivanov.mvikotlin.extensions.coroutines.CoroutineExecutor
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.solr.composeui.components.environment.data.JavaProperty
import org.apache.solr.composeui.components.environment.data.SystemData
import org.apache.solr.composeui.components.environment.store.EnvironmentStore.Intent
import org.apache.solr.composeui.components.environment.store.EnvironmentStore.State

internal class EnvironmentStoreProvider(
    private val storeFactory: StoreFactory,
    private val client: Client,
    private val ioContext: CoroutineContext,
) {

    fun provide(): EnvironmentStore = object :
        EnvironmentStore,
        Store<Intent, State, Nothing> by storeFactory.create(
            name = "EnvironmentStore",
            initialState = State(),
            bootstrapper = SimpleBootstrapper(Action.FetchInitialSystemData),
            executorFactory = ::ExecutorImpl,
            reducer = ReducerImpl,
        ) {}

    private sealed interface Action {

        data object FetchInitialSystemData: Action
    }

    private sealed interface Message {

        data class SystemDataUpdated(val data: SystemData) : Message

        data class JavaPropertiesUpdated(val properties: List<JavaProperty>) : Message
    }

    private inner class ExecutorImpl : CoroutineExecutor<Intent, Action, State, Message, Nothing>() {

        override fun executeAction(action: Action) = when(action) {
            Action.FetchInitialSystemData -> fetchSystemData()
        }

        override fun executeIntent(intent: Intent) {
            when (intent) {
                Intent.FetchSystemData -> fetchSystemData()
            }
        }

        private fun fetchSystemData() {
            scope.launch { // TODO Add coroutine exception handler
                withContext(ioContext) {
                    client.getSystemData()
                }.onSuccess {
                    dispatch(Message.SystemDataUpdated(it))
                }

                withContext(ioContext) {
                    client.getJavaProperties()
                }.onSuccess {
                    dispatch(Message.JavaPropertiesUpdated(it))
                }
                // TODO Add error handling
            }
        }
    }

    private object ReducerImpl : Reducer<State, Message> {
        override fun State.reduce(msg: Message): State = when (msg) {
            is Message.SystemDataUpdated -> copy(
                mode = msg.data.mode,
                zkHost = msg.data.zkHost,
                solrHome = msg.data.solrHome,
                coreRoot = msg.data.coreRoot,
                lucene = msg.data.lucene,
                jvm = msg.data.jvm,
                security = msg.data.security,
                system = msg.data.system,
                node = msg.data.node,
            )
            is Message.JavaPropertiesUpdated -> copy(
                javaProperties = msg.properties,
            )
        }
    }

    interface Client {

        /**
         * Fetches a set of system data.
         *
         * @return Result with the system data fetched.
         */
        suspend fun getSystemData(): Result<SystemData>

        /**
         * Fetches the configured java properties.
         *
         * @return Result with a list of [JavaProperty]s.
         */
        suspend fun getJavaProperties(): Result<List<JavaProperty>>
    }
}
