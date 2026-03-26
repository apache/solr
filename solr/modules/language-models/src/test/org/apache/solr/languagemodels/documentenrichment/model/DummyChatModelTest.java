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
package org.apache.solr.languagemodels.documentenrichment.model;

import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.request.ChatRequest;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

public class DummyChatModelTest extends SolrTestCase {

  @Test
  public void constructAndChat() throws Exception {
    assertEquals(
        "hello world",
        new DummyChatModel("hello world")
            .chat(ChatRequest.builder().messages(UserMessage.from("any input")).build())
            .aiMessage()
            .text());
    assertEquals(
        "fixed response",
        new DummyChatModel("fixed response")
            .chat(ChatRequest.builder().messages(UserMessage.from("another input")).build())
            .aiMessage()
            .text());
    assertEquals(
        "dummy response",
        DummyChatModel.builder()
            .build()
            .chat(ChatRequest.builder().messages(UserMessage.from("default")).build())
            .aiMessage()
            .text());
  }
}