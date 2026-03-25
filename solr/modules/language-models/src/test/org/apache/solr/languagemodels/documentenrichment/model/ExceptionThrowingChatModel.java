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

import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;

/**
 * A {@link ChatModel} that always throws a {@link RuntimeException}. Used to verify that {@link
 * org.apache.solr.languagemodels.documentenrichment.update.processor.DocumentEnrichmentUpdateProcessor}
 * handles chat-model failures gracefully (logs the error and continues indexing without the
 * enriched field).
 */
public class ExceptionThrowingChatModel implements ChatModel {

  @Override
  public ChatResponse chat(ChatRequest chatRequest) {
    throw new RuntimeException("Failed to enrich");
  }

  public static ExceptionThrowingChatModelBuilder builder() {
    return new ExceptionThrowingChatModelBuilder();
  }

  public static class ExceptionThrowingChatModelBuilder {

    public ExceptionThrowingChatModelBuilder() {}

    public ExceptionThrowingChatModel build() {
      return new ExceptionThrowingChatModel();
    }
  }
}
