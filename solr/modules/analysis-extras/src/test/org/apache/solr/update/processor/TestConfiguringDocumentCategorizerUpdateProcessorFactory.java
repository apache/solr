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

package org.apache.solr.update.processor;

import java.nio.file.Path;
import org.apache.commons.io.file.PathUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for {@link DocumentCategorizerUpdateProcessorFactory}
 *
 * <p>Focused on verifying how DocumentCategorizerUpdateProcessorFactory is set up to run.
 *
 * <p>See test_opennlp.bats for a complete system test of DocumentCategorizerUpdateProcessorFactory.
 */
public class TestConfiguringDocumentCategorizerUpdateProcessorFactory
    extends UpdateProcessorTestBase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    Path testHome = createTempDir();
    PathUtils.copyDirectory(getFile("analysis-extras/solr"), testHome);
    initCore("solrconfig-document-categorizer.xml", "schema-document-categorizer.xml", testHome);
  }

  @Test
  public void testBasicConfiguration() {
    DocumentCategorizerUpdateProcessorFactory factory =
        new DocumentCategorizerUpdateProcessorFactory();
    NamedList<Object> args = new NamedList<>();

    // Add source and dest
    args.add("source", "text_field");
    args.add("dest", "category_field");

    // Add required model files
    args.add("modelFile", "doccat-model.bin");
    args.add("vocabFile", "doccat-vocab.txt");

    // Should initialize without errors
    factory.init(args);
  }

  @Test(expected = SolrException.class)
  public void testMissingModelFile() {
    DocumentCategorizerUpdateProcessorFactory factory =
        new DocumentCategorizerUpdateProcessorFactory();
    NamedList<Object> args = new NamedList<>();

    // Add source and dest but miss modelFile
    args.add("source", "text_field");
    args.add("dest", "category_field");
    args.add("vocabFile", "doccat-vocab.txt");

    factory.init(args);
  }

  @Test(expected = SolrException.class)
  public void testMissingVocabFile() {
    DocumentCategorizerUpdateProcessorFactory factory =
        new DocumentCategorizerUpdateProcessorFactory();
    NamedList<Object> args = new NamedList<>();

    // Add source and dest but miss vocabFile
    args.add("source", "text_field");
    args.add("dest", "category_field");
    args.add("modelFile", "doccat-model.bin");

    factory.init(args);
  }

  @Test(expected = SolrException.class)
  public void testMissingSourceAndDest() {
    DocumentCategorizerUpdateProcessorFactory factory =
        new DocumentCategorizerUpdateProcessorFactory();
    NamedList<Object> args = new NamedList<>();

    // Add only model files but missing source/dest
    args.add("modelFile", "doccat-model.bin");
    args.add("vocabFile", "doccat-vocab.txt");

    factory.init(args);
  }

  @Test
  public void testRegexConfiguration() {
    DocumentCategorizerUpdateProcessorFactory factory =
        new DocumentCategorizerUpdateProcessorFactory();
    NamedList<Object> args = new NamedList<>();

    // Add source parameter
    NamedList<Object> sourceArgs = new NamedList<>();
    sourceArgs.add("fieldRegex", "text_.*");
    args.add("source", sourceArgs);

    // Create pattern and replacement as part of a dest structure
    NamedList<Object> destArgs = new NamedList<>();
    destArgs.add("pattern", "text_(.*)");
    destArgs.add("replacement", "category_$1");
    args.add("dest", destArgs);

    // Add required model files
    args.add("modelFile", "doccat-model.bin");
    args.add("vocabFile", "doccat-vocab.txt");

    // Should initialize without errors
    factory.init(args);
  }

  @Test
  public void testSourceSelector() {
    DocumentCategorizerUpdateProcessorFactory factory =
        new DocumentCategorizerUpdateProcessorFactory();
    NamedList<Object> args = new NamedList<>();

    // Create a complex source selector
    NamedList<Object> sourceArgs = new NamedList<>();
    sourceArgs.add("fieldRegex", "text_.*");
    NamedList<Object> excludeArgs = new NamedList<>();
    excludeArgs.add("fieldRegex", "text_ignore_.*");
    sourceArgs.add("exclude", excludeArgs);

    args.add("source", sourceArgs);
    args.add("dest", "category_field");
    args.add("modelFile", "doccat-model.bin");
    args.add("vocabFile", "doccat-vocab.txt");

    // Should initialize without errors
    factory.init(args);
  }

  @Test
  public void testDestWithPatternReplacement() {
    DocumentCategorizerUpdateProcessorFactory factory =
        new DocumentCategorizerUpdateProcessorFactory();
    NamedList<Object> args = new NamedList<>();

    // Simple source
    args.add("source", "text_field");

    // Dest with pattern and replacement
    NamedList<Object> destArgs = new NamedList<>();
    destArgs.add("pattern", "text_(.*)");
    destArgs.add("replacement", "category_$1");
    args.add("dest", destArgs);

    // Add required model files
    args.add("modelFile", "doccat-model.bin");
    args.add("vocabFile", "doccat-vocab.txt");

    // Should initialize without errors
    factory.init(args);
  }
}
