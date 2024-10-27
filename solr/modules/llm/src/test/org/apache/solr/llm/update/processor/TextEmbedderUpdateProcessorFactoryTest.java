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
package org.apache.solr.llm.update.processor;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.llm.TestLlmBase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/** Tests for {@link TextEmbedderUpdateProcessorFactory} */
public class TextEmbedderUpdateProcessorFactoryTest extends TestLlmBase {
  @BeforeClass
  public static void init() throws Exception {
    setuptest(true);
    loadModels("dummy-model.json");
  }
  
  private TextEmbedderUpdateProcessorFactory cFactoryToTest =
      new TextEmbedderUpdateProcessorFactory();
  private NamedList<String> args = new NamedList<>();

  @Before
  public void initArgs() {
    args.add("inputField", "text");
    args.add("outputField", "vector");
    args.add("model", "dummy-1");
  }

  @Test
  public void init_fullArgs_shouldInitFullParams() {
    cFactoryToTest.init(args);
    assertEquals(cFactoryToTest.getInputField(), "text");
  }

  @Test
  public void init_emptyInputFields_shouldThrowExceptionWithDetailedMessage() {
    args.removeAll("inputFields");
    SolrException e = assertThrows(SolrException.class, () -> cFactoryToTest.init(args));
    assertEquals("Classification UpdateProcessor 'inputFields' can not be null", e.getMessage());
  }
  
}
