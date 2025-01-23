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
package org.apache.solr.llm.texttovector.update.processor;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.junit.Before;
import org.junit.Test;


public class TextToVectorUpdateProcessorFactoryTest extends SolrTestCaseJ4 {
  private TextToVectorUpdateProcessorFactory factoryToTest =
      new TextToVectorUpdateProcessorFactory();
  private NamedList<String> args = new NamedList<>();

  @Before
  public void initArgs() {
    args.add("inputField", "inputField1");
    args.add("outputField", "outputField1");
    args.add("model", "model1");
  }

  @Test
  public void init_fullArgs_shouldInitFullClassificationParams() {
    factoryToTest.init(args);

    assertEquals("inputField1", factoryToTest.getInputField());
    assertEquals("outputField1", factoryToTest.getOutputField());
    assertEquals("model1", factoryToTest.getEmbeddingModelName());
  }

  @Test
  public void init_nullyInputFields_shouldThrowExceptionWithDetailedMessage() {
    args.removeAll("inputField");
    SolrException e = assertThrows(SolrException.class, () -> factoryToTest.init(args));
    assertEquals("Text to Vector UpdateProcessor 'inputField' can not be null", e.getMessage());
  }

  @Test
  public void init_nullOutputField_shouldThrowExceptionWithDetailedMessage() {
    args.removeAll("outputField");
    SolrException e = assertThrows(SolrException.class, () -> factoryToTest.init(args));
    assertEquals("Text to Vector UpdateProcessor 'outputField' can not be null", e.getMessage());
  }

  @Test
  public void init_nullModel_shouldThrowExceptionWithDetailedMessage() {
    args.removeAll("model");
    SolrException e = assertThrows(SolrException.class, () -> factoryToTest.init(args));
    assertEquals("Text to Vector UpdateProcessor 'model' can not be null", e.getMessage());
  }
  
}
