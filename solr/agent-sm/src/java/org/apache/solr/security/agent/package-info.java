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
 * network connections, {@code System.exit()}, and process spawning at the bytecode level, enforcing
 * a policy loaded from JDK-style {@code .policy} files.
 *
 * <p>Key classes:
 *
 * <ul>
 *   <li>{@link org.apache.solr.security.agent.SolrAgentEntryPoint} — {@code premain()}/{@code
 *       agentmain()} entry point; registers all ByteBuddy interceptors.
 *   <li>{@link org.apache.solr.security.agent.PolicyLoader} — parses {@code .policy} files with
 *       Solr variable substitution.
 *   <li>{@link org.apache.solr.security.agent.SolrSecurityPolicy} — immutable singleton holding the
 *       merged default + operator policy.
 *   <li>{@link org.apache.solr.security.agent.StackInspector} — virtual-thread-safe call chain
 *       analysis via {@code StackWalker}.
 *   <li>{@link org.apache.solr.security.agent.SecurityViolationLogger} — structured SLF4J violation
 *       log emitter.
 *   <li>{@link org.apache.solr.security.agent.ViolationMetricsReporter} — per-type violation
 *       counters with deferred registration in {@code SolrMetricManager}.
 * </ul>
 */
package org.apache.solr.security.agent;
