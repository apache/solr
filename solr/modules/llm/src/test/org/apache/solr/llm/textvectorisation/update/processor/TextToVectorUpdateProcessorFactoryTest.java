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
package org.apache.solr.llm.textvectorisation.update.processor;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.llm.TestLlmBase;
import org.apache.solr.request.SolrQueryRequestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TextToVectorUpdateProcessorFactoryTest extends TestLlmBase {

  @BeforeClass
  public static void init() throws Exception {
    setupTest("solrconfig-llm.xml", "schema.xml", false, false);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    afterTest();
  }

  @Test
  public void init_fullArgs_shouldInitAllParams() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "_text_");
    args.add("outputField", "vector");
    args.add("model", "model1");

    TextToVectorUpdateProcessorFactory factoryToTest = new TextToVectorUpdateProcessorFactory();
    factoryToTest.init(args);

    assertEquals("_text_", factoryToTest.getInputField());
    assertEquals("vector", factoryToTest.getOutputField());
    assertEquals("model1", factoryToTest.getModelName());
  }

  @Test
  public void init_nullInputField_shouldThrowExceptionWithDetailedMessage() {
    NamedList<String> args = new NamedList<>();
    args.add("outputField", "vector");
    args.add("model", "model1");

    TextToVectorUpdateProcessorFactory factoryToTest = new TextToVectorUpdateProcessorFactory();

    SolrException e = assertThrows(SolrException.class, () -> factoryToTest.init(args));
    assertEquals("Missing required parameter: inputField", e.getMessage());
  }

  @Test
  public void init_nullOutputField_shouldThrowExceptionWithDetailedMessage() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "_text_");
    args.add("model", "model1");

    TextToVectorUpdateProcessorFactory factoryToTest = new TextToVectorUpdateProcessorFactory();

    SolrException e = assertThrows(SolrException.class, () -> factoryToTest.init(args));
    assertEquals("Missing required parameter: outputField", e.getMessage());
  }

  @Test
  public void init_nullModel_shouldThrowExceptionWithDetailedMessage() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "_text_");
    args.add("outputField", "vector");

    TextToVectorUpdateProcessorFactory factoryToTest = new TextToVectorUpdateProcessorFactory();

    SolrException e = assertThrows(SolrException.class, () -> factoryToTest.init(args));
    assertEquals("Missing required parameter: model", e.getMessage());
  }

  /* Following test depends on a real solr schema and depends on BeforeClass-AfterClass methods */
  @Test
  public void init_notExistentOutputField_shouldThrowExceptionWithDetailedMessage() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "_text_");
    args.add("outputField", "notExistentOutput");
    args.add("model", "model1");

    TextToVectorUpdateProcessorFactory factoryToTest = new TextToVectorUpdateProcessorFactory();

    ModifiableSolrParams params = new ModifiableSolrParams();
    SolrCore collection1 = solrClientTestRule.getCoreContainer().getCore("collection1");
    SolrQueryRequestBase req = new SolrQueryRequestBase(collection1, params) {};
    factoryToTest.init(args);
    SolrException e =
        assertThrows(SolrException.class, () -> factoryToTest.getInstance(req, null, null));
    assertEquals("undefined field: \"notExistentOutput\"", e.getMessage());
    collection1.close();
  }

  /* Following test depends on a real solr schema and depends on BeforeClass-AfterClass methods */
  @Test
  public void init_notDenseVectorOutputField_shouldThrowExceptionWithDetailedMessage() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "_text_");
    args.add("outputField", "_text_");
    args.add("model", "model1");

    TextToVectorUpdateProcessorFactory factoryToTest = new TextToVectorUpdateProcessorFactory();

    ModifiableSolrParams params = new ModifiableSolrParams();
    SolrCore collection1 = solrClientTestRule.getCoreContainer().getCore("collection1");
    SolrQueryRequestBase req = new SolrQueryRequestBase(collection1, params) {};
    factoryToTest.init(args);
    SolrException e =
        assertThrows(SolrException.class, () -> factoryToTest.getInstance(req, null, null));
    assertEquals(
        "only DenseVectorField is compatible with Vector Query Parsers: _text_", e.getMessage());
    collection1.close();
  }

  /* Following test depends on a real solr schema and depends on BeforeClass-AfterClass methods */
  @Test
  public void init_notExistentInputField_shouldThrowExceptionWithDetailedMessage() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "notExistentInput");
    args.add("outputField", "vector");
    args.add("model", "model1");

    TextToVectorUpdateProcessorFactory factoryToTest = new TextToVectorUpdateProcessorFactory();

    ModifiableSolrParams params = new ModifiableSolrParams();
    SolrCore collection1 = solrClientTestRule.getCoreContainer().getCore("collection1");
    SolrQueryRequestBase req = new SolrQueryRequestBase(collection1, params) {};
    factoryToTest.init(args);
    SolrException e =
        assertThrows(SolrException.class, () -> factoryToTest.getInstance(req, null, null));
    assertEquals("undefined field: \"notExistentInput\"", e.getMessage());
    collection1.close();
  }
}
