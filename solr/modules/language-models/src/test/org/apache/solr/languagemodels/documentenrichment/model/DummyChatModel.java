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

import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;

/**
 * A deterministic {@link ChatModel} for testing. It returns a fixed response string regardless of
 * the input, allowing tests to assert exact enriched-field values without real API calls.
 *
 * <p>The builder also exposes {@code unsupported} and {@code ambiguous} setter methods to exercise
 * the reflection-based parameter handling in {@link
 * org.apache.solr.languagemodels.documentenrichment.model.SolrChatModel#getInstance}.
 */
public class DummyChatModel implements ChatModel {

  private final String response;

  public DummyChatModel(String response) {
    this.response = response;
  }

  @Override
  public ChatResponse chat(ChatRequest chatRequest) {
    return ChatResponse.builder().aiMessage(AiMessage.from(response)).build();
  }

  public static DummyChatModelBuilder builder() {
    return new DummyChatModelBuilder();
  }

  public static class DummyChatModelBuilder {
    private String response = "dummy response";
    private int intValue;

    public DummyChatModelBuilder() {}

    public DummyChatModelBuilder response(String response) {
      this.response = response;
      return this;
    }

    /** Intentionally has no String overload so the reflection code raises a BAD_REQUEST error. */
    public DummyChatModelBuilder unsupported(Integer input) {
      return this;
    }

    /** Two overloads make this param "ambiguous": the reflection code should default to String. */
    public DummyChatModelBuilder ambiguous(int input) {
      this.intValue = input;
      return this;
    }

    public DummyChatModelBuilder ambiguous(String input) {
      this.intValue = Integer.valueOf(input);
      return this;
    }

    public DummyChatModel build() {
      return new DummyChatModel(this.response);
    }
  }
}
