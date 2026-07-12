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
 * Experimental join between two indexes through an auxiliary index that persists per (from-segment,
 * to-segment) doc id mappings, so query-time joining reduces to bitset translation. {@link
 * org.apache.solr.search.join.aijoin.AIJoinIndex} is the entry point: it owns the auxiliary index and
 * builds its pair columns lazily on first search, so queries created with {@link
 * org.apache.solr.search.join.aijoin.AIJoinIndex#newJoinQuery} run against a bare to-side searcher
 * with no explicit build step.
 */
package org.apache.solr.search.join.aijoin;
