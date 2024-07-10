package org.apache.solr.composeui.utils

import com.arkivanov.essenty.lifecycle.Lifecycle
import com.arkivanov.essenty.lifecycle.LifecycleOwner
import com.arkivanov.essenty.lifecycle.doOnDestroy
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.stateIn
import kotlin.coroutines.CoroutineContext

/**
 * Function for creating a coroutine scope for a [lifecycle] that cancels automatically
 * when [lifecycle] is destroyed.
 *
 * @param context The [CoroutineContext] to use for the scope.
 * @param lifecycle The lifecycle this coroutine should be aware of.
 *
 * @return Returns a lifecycle-aware coroutine scope.
 */
fun CoroutineScope(context: CoroutineContext, lifecycle: Lifecycle): CoroutineScope {
    val scope = CoroutineScope(context)
    lifecycle.doOnDestroy(scope::cancel)
    return scope
}

/**
 * Function for creating a coroutine scope from a lifecycle that cancels automatically
 * when lifecycle is destroyed.
 *
 * @param context The [CoroutineContext] to use for the scope.
 *
 * @return Returns a lifecycle-aware coroutine scope.
 */
fun LifecycleOwner.coroutineScope(context: CoroutineContext): CoroutineScope =
    CoroutineScope(context, lifecycle)


/**
 * StateFlow mapping function that maps one stateflow to another stateflow applying
 * [mapper] on the state. [coroutineScope] used for the mapping work.
 *
 * @param coroutineScope The scope in which the mapping should be executed
 * @param mapper The mapper function to use on [T].
 * @param T The state to map
 * @param M the mapped / resulting state
 */
fun <T, M> StateFlow<T>.map(
    coroutineScope : CoroutineScope,
    mapper : (value : T) -> M
) : StateFlow<M> = map { mapper(it) }.stateIn(
    coroutineScope,
    SharingStarted.Eagerly,
    mapper(value)
)
