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
package org.apache.solr.handler.component;

import java.nio.file.Path;
import java.util.Random;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrClientTestRule;

/** Creates the combined-query test core on a Solr started by a test rule. */
final class CombinedQueryTestCore {

  /** The test configset; a Solr home outside it must allow reading from it. */
  static final Path CONF =
      ExternalPaths.SOURCE_HOME.resolve("core/src/test-files/solr/collection1/conf");

  private CombinedQueryTestCore() {}

  /**
   * Creates the default collection from {@code solrconfig-combined-query.xml} and {@code
   * schema-vector-catchall.xml}, the uniqueKey read from stored fields or from docValues at random.
   * The schema's numeric types are those {@code SolrTestCaseJ4} chose for the test class.
   */
  static void create(SolrClientTestRule rule, Random random) throws Exception {
    boolean idFromDocValues = random.nextBoolean();
    System.setProperty("solr.tests.id.stored", Boolean.toString(!idFromDocValues));
    System.setProperty("solr.tests.id.docValues", Boolean.toString(idFromDocValues));
    rule.newCollection()
        .withConfigSet(CONF)
        .withConfigFile("solrconfig-combined-query.xml")
        .withSchemaFile("schema-vector-catchall.xml")
        .create();
  }
}
