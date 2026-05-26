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
 * <p>This package contains the Java agent entry point and runtime enforcement infrastructure that
 * replaces the removed Java Security Manager API. The agent intercepts file access, outbound
 * network connections, {@code System.exit()}, {@code Runtime.halt()}, and process spawning at the
 * bytecode level, enforcing a policy loaded from JDK-style {@code .policy} files.
 *
 * <h2>Key classes</h2>
 *
 * <ul>
 *   <li>{@link org.apache.solr.security.agent.SolrAgentEntryPoint} — {@code premain()}/{@code
 *       agentmain()} entry point; loads the policy and registers all ByteBuddy interceptors.
 *   <li>{@link org.apache.solr.security.agent.AgentPolicy} — immutable singleton holding the merged
 *       default + operator policy, enforcement mode, and trusted-host set.
 *   <li>{@link org.apache.solr.security.agent.PolicyLoader} — parses {@code .policy} files,
 *       performs variable substitution, and merges the default and operator extension files.
 *   <li>{@link org.apache.solr.security.agent.PolicyPropertyExpander} — expands {@code ${property}}
 *       placeholders; falls back to env vars using the standard {@code SOLR_FOO_BAR} convention
 *       (with custom overrides for non-standard mappings such as {@code SOLR_HOME}).
 *   <li>{@link org.apache.solr.security.agent.PolicyFileParser} — {@code StreamTokenizer}-based
 *       parser for JDK-style {@code .policy} files; handles comments and quoted strings natively.
 *   <li>{@link org.apache.solr.security.agent.StackCallerClassChainExtractor} — virtual-thread-safe
 *       call chain extraction via {@code StackWalker}.
 *   <li>{@link org.apache.solr.security.agent.SecurityViolationLogger} — structured SLF4J violation
 *       log emitter.
 *   <li>{@link org.apache.solr.security.agent.ViolationMetricsReporter} — per-type violation
 *       counters with deferred registration in {@code SolrMetricManager}.
 * </ul>
 *
 * <h2>OpenSearch-derived files</h2>
 *
 * <p>The following source files in this package were derived from the <a
 * href="https://github.com/opensearch-project/OpenSearch">OpenSearch project</a> (Apache License
 * 2.0) and modified to integrate with Solr's policy model:
 *
 * <ul>
 *   <li>{@link org.apache.solr.security.agent.FileInterceptor}
 *   <li>{@link org.apache.solr.security.agent.SocketChannelInterceptor}
 *   <li>{@link org.apache.solr.security.agent.SystemExitInterceptor}
 *   <li>{@link org.apache.solr.security.agent.RuntimeHaltInterceptor}
 *   <li>{@link org.apache.solr.security.agent.StackCallerClassChainExtractor}
 *   <li>{@link org.apache.solr.security.agent.PolicyFileParser}
 *   <li>{@link org.apache.solr.security.agent.PolicyTokenStream}
 *   <li>{@link org.apache.solr.security.agent.PolicyPropertyExpander}
 * </ul>
 *
 * <p>See {@code NOTICE.txt} for full attribution.
 */
package org.apache.solr.security.agent;
