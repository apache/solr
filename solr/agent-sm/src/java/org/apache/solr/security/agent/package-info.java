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

/**
 * Solr Security Agent — Java Security Manager replacement via ByteBuddy instrumentation.
 *
 * <p>Intercepts file access, outbound network connections, {@code System.exit()}, {@code
 * Runtime.halt()}, and process spawning at the bytecode level, enforcing a policy loaded from
 * JDK-style {@code .policy} files. Entry point: {@link
 * org.apache.solr.security.agent.SolrAgentEntryPoint}. Policy: {@link
 * org.apache.solr.security.agent.AgentPolicy} (loaded by {@link
 * org.apache.solr.security.agent.PolicyLoader}).
 *
 * <p><b>Advice visibility rule:</b> Every static helper method called directly from an
 * {@code @Advice.OnMethodEnter} body must be {@code public}. ByteBuddy inlines the advice bytecode
 * into the target JDK method (e.g. {@code java.nio.file.Files}, {@code
 * sun.nio.ch.SocketChannelImpl}), so the call site is in {@code java.base}. A package-private
 * method in this package is inaccessible from there and causes {@code IllegalAccessError} at
 * runtime.
 *
 * <p>Several files ({@link org.apache.solr.security.agent.FileInterceptor}, {@link
 * org.apache.solr.security.agent.SocketChannelInterceptor}, {@link
 * org.apache.solr.security.agent.SystemExitInterceptor}, {@link
 * org.apache.solr.security.agent.RuntimeHaltInterceptor}, {@link
 * org.apache.solr.security.agent.StackCallerClassChainExtractor}, {@link
 * org.apache.solr.security.agent.PolicyFileParser}, {@link
 * org.apache.solr.security.agent.PolicyTokenStream}, {@link
 * org.apache.solr.security.agent.PolicyPropertyExpander}) were derived from the OpenSearch project
 * (Apache License 2.0). See {@code NOTICE.txt} for attribution.
 */
package org.apache.solr.security.agent;
