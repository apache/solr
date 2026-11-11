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

package org.apache.solr.shell;

/**
 * Which command-line implementation {@code bin/solr} dispatches to, selected by {@code
 * SOLR_PICOCLI} / {@code solr.picocli}.
 *
 * <p>The two mostly agree, but not on every word of their usage and error text, so a test asserting
 * on wording may hold for only one of them. {@link EnabledOnCli} skips such a test on the other;
 * {@code CommandResult.assertContainsForCli} asserts the right wording for whichever is running.
 *
 * @see org.apache.solr.cli.SolrCLI#main
 */
public enum Cli {

  /** The commons-cli implementation. Being replaced, and due to go in Solr 11. */
  COMMONS_CLI,

  /** The picocli implementation. */
  PICOCLI
}
