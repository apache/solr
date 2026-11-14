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
 * CATS, the cross-platform harness for testing command line programs.
 *
 * <p>This package and its subpackages are generic. Keep them that way: nothing in them may name
 * Solr, its scripts, its ports or its files, and they depend on nothing beyond the JRE, JUnit and
 * slf4j. Whatever is specific to the program under test belongs in a layer on top, which for Solr
 * is {@code org.apache.solr.shell}.
 */
package org.apache.solr.cats;
