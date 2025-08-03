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

package org.apache.solr.ui.utils

import com.arkivanov.essenty.lifecycle.Lifecycle
import com.arkivanov.essenty.lifecycle.LifecycleOwner
import com.arkivanov.essenty.lifecycle.doOnDestroy
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.stateIn

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
fun LifecycleOwner.coroutineScope(context: CoroutineContext): CoroutineScope = CoroutineScope(context, lifecycle)

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
    coroutineScope: CoroutineScope,
    mapper: (value: T) -> M,
): StateFlow<M> = map { mapper(it) }.stateIn(
    coroutineScope,
    SharingStarted.Eagerly,
    mapper(value),
)
