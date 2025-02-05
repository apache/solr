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
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.llm.TestLlmBase;
import org.apache.solr.request.SolrQueryRequestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;


public class TextToVectorUpdateProcessorFactoryTest extends TestLlmBase {
  private TextToVectorUpdateProcessorFactory factoryToTest =
      new TextToVectorUpdateProcessorFactory();
  private NamedList<String> args = new NamedList<>();
  
  @BeforeClass
  public static void initArgs() throws Exception {
    setupTest("solrconfig-llm.xml", "schema.xml", false, false);
  }

  @AfterClass
  public static void after() throws Exception {
    afterTest();
  }

  @Test
  public void init_fullArgs_shouldInitAllParams() {
    args.add("inputField", "_text_");
    args.add("outputField", "vector");
    args.add("model", "model1");
    factoryToTest.init(args);

    assertEquals("_text_", factoryToTest.getInputField());
    assertEquals("vector", factoryToTest.getOutputField());
    assertEquals("model1", factoryToTest.getModelName());
  }

  @Test
  public void init_nullInputField_shouldThrowExceptionWithDetailedMessage() {
    args.add("outputField", "vector");
    args.add("model", "model1");
    
    SolrException e = assertThrows(SolrException.class, () -> factoryToTest.init(args));
    assertEquals("Missing required parameter: inputField", e.getMessage());
  }

  @Test
  public void init_notExistentInputField_shouldThrowExceptionWithDetailedMessage() throws Exception {
    args.add("inputField", "notExistentInput");
    args.add("outputField", "vector");
    args.add("model", "model1");

    Map<String, String[]> params = new HashMap<>();
    MultiMapSolrParams mmparams = new MultiMapSolrParams(params);
    SolrQueryRequestBase req = new SolrQueryRequestBase(solrClientTestRule.getCoreContainer().getCore("collection1"), (SolrParams) mmparams) {};
    factoryToTest.init(args);
    SolrException e = assertThrows(SolrException.class, () -> factoryToTest.getInstance(req,null,null));
    assertEquals("undefined field: \"notExistentInput\"", e.getMessage());
  }

  @Test
  public void init_nullOutputField_shouldThrowExceptionWithDetailedMessage() {
    args.add("inputField", "_text_");
    args.add("model", "model1");
    
    SolrException e = assertThrows(SolrException.class, () -> factoryToTest.init(args));
    assertEquals("Missing required parameter: outputField", e.getMessage());
  }

  @Test
  public void init_notExistentOutputField_shouldThrowExceptionWithDetailedMessage() throws Exception {
    args.add("inputField", "_text_");
    args.add("outputField", "notExistentOutput");
    args.add("model", "model1");
    
    Map<String, String[]> params = new HashMap<>();
    MultiMapSolrParams mmparams = new MultiMapSolrParams(params);
    SolrQueryRequestBase req = new SolrQueryRequestBase(solrClientTestRule.getCoreContainer().getCore("collection1"), (SolrParams) mmparams) {};
    factoryToTest.init(args);
    SolrException e = assertThrows(SolrException.class, () -> factoryToTest.getInstance(req,null,null));
    assertEquals("undefined field: \"notExistentOutput\"", e.getMessage());
  }
  
  @Test
  public void init_notDenseVectorOutputField_shouldThrowExceptionWithDetailedMessage() throws Exception {
    args.add("inputField", "_text_");
    args.add("outputField", "_text_");
    args.add("model", "model1");
    
    Map<String, String[]> params = new HashMap<>();
    MultiMapSolrParams mmparams = new MultiMapSolrParams(params);
    SolrQueryRequestBase req = new SolrQueryRequestBase(solrClientTestRule.getCoreContainer().getCore("collection1"), (SolrParams) mmparams) {};
    factoryToTest.init(args);
    SolrException e = assertThrows(SolrException.class, () -> factoryToTest.getInstance(req,null,null));
    assertEquals("only DenseVectorField is compatible with Vector Query Parsers: _text_", e.getMessage());
  }

  @Test
  public void init_nullModel_shouldThrowExceptionWithDetailedMessage() {
    args.add("inputField", "_text_");
    args.add("outputField", "vector");
    
    SolrException e = assertThrows(SolrException.class, () -> factoryToTest.init(args));
    assertEquals("Missing required parameter: model", e.getMessage());
  }
  
}
