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
import org.apache.solr.llm.textvectorisation.model.SolrTextToVectorModel;
import org.apache.solr.llm.textvectorisation.store.rest.ManagedTextToVectorModelStore;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
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

  SolrCore collection1;

  @Before
  public void setup() {
    collection1 = solrClientTestRule.getCoreContainer().getCore("collection1");
  }

  @After
  public void after() {
    collection1.close();
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
    SolrQueryRequestBase req = new SolrQueryRequestBase(collection1, params) {};
    factoryToTest.init(args);
    SolrException e =
        assertThrows(SolrException.class, () -> factoryToTest.getInstance(req, null, null));
    assertEquals("undefined field: \"notExistentOutput\"", e.getMessage());
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
    SolrQueryRequestBase req = new SolrQueryRequestBase(collection1, params) {};
    factoryToTest.init(args);
    SolrException e =
        assertThrows(SolrException.class, () -> factoryToTest.getInstance(req, null, null));
    assertEquals(
        "only DenseVectorField is compatible with Vector Query Parsers: _text_", e.getMessage());
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
    SolrQueryRequestBase req = new SolrQueryRequestBase(collection1, params) {};
    factoryToTest.init(args);
    SolrException e =
        assertThrows(SolrException.class, () -> factoryToTest.getInstance(req, null, null));
    assertEquals("undefined field: \"notExistentInput\"", e.getMessage());
  }

  /* Following test depends on a real solr schema and depends on BeforeClass-AfterClass methods */
  @Test
  public void init_notExistentDynamicInputField_shouldNotThrowException() {
    String inputFieldName = "text_s";
    String outputFieldName = "vector";

    UpdateRequestProcessor instance =
        createUpdateProcessor(inputFieldName, outputFieldName, collection1, "model1");
    assertNotNull(instance);
  }

  /* Following test depends on a real solr schema and depends on BeforeClass-AfterClass methods */
  @Test
  public void init_notExistingDynamicOutputField_shouldNotThrowException() {
    String inputFieldName = "_text_";
    String outputFieldName = "vec_vector1024";

    UpdateRequestProcessor instance =
        createUpdateProcessor(inputFieldName, outputFieldName, collection1, "model2");
    assertNotNull(instance);
  }

  private UpdateRequestProcessor createUpdateProcessor(
      String inputFieldName, String outputFieldName, SolrCore collection1, String modelName) {
    NamedList<String> args = new NamedList<>();

    ManagedTextToVectorModelStore.getManagedModelStore(collection1)
        .addModel(new SolrTextToVectorModel(modelName, null, null));
    args.add("inputField", inputFieldName);
    args.add("outputField", outputFieldName);
    args.add("model", modelName);

    TextToVectorUpdateProcessorFactory factoryToTest = new TextToVectorUpdateProcessorFactory();
    ModifiableSolrParams params = new ModifiableSolrParams();
    factoryToTest.init(args);

    SolrQueryRequestBase req = new SolrQueryRequestBase(collection1, params) {};

    return factoryToTest.getInstance(req, null, null);
  }
}
