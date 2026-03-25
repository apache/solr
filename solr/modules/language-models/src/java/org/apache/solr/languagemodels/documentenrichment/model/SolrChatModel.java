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
import dev.langchain4j.model.chat.ChatModel;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.languagemodels.documentenrichment.store.ChatModelException;
import org.apache.solr.languagemodels.documentenrichment.store.rest.ManagedChatModelStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This object wraps a {@link ChatModel} to produce the content of new fields from another.
 * It's meant to be used as a managed resource with the {@link
 * ManagedChatModelStore}
 */
public class SolrChatModel implements Accountable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final long BASE_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(SolrChatModel.class);
  // timeout is type Duration
  private static final String TIMEOUT_PARAM = "timeout";

  // the following are Integer type
  private static final String MAX_RETRIES_PARAM = "maxRetries";
  private static final String THINKING_BUDGET_TOKENS ="thinkingBudgetTokens";
  private static final String RANDOM_SEED = "randomSeed";

  private final String name;
  private final Map<String, Object> params;
  private final ChatModel chatModel;
  private final int hashCode;

  public static SolrChatModel getInstance(
      SolrResourceLoader solrResourceLoader,
      String className,
      String name,
      Map<String, Object> params)
      throws ChatModelException {
    try {
      /*
       * The idea here is to build a {@link dev.langchain4j.model.chat.ChatModel} using inversion
       * of control.
       * Each model has its own list of parameters we don't know beforehand, but each {@link dev.langchain4j.model.chat.ChatModel} class
       * has its own builder that uses setters with the same name of the parameter in input.
       * */
      ChatModel textToTextModel;
      Class<?> modelClass = solrResourceLoader.findClass(className, ChatModel.class);
      var builder = modelClass.getMethod("builder").invoke(null);
      if (params != null) {
        /*
         * This block of code has the responsibility of instantiate a {@link
         * dev.langchain4j.model.chat.ChatModel} using the params provided.classes have
         * params of The specific implementation of {@link
         * dev.langchain4j.model.chat.ChatModel} is not known beforehand. So we benefit of
         * the design choice in langchain4j that each subclass implementing {@link
         * dev.langchain4j.model.chat.ChatModel} uses setters with the same name of the
         * param.
         */
        for (String paramName : params.keySet()) {
          /*
           * When a param is not primitive, we need to instantiate the object explicitly and then call the
           * setter method.
           * N.B. when adding support to new models, pay attention to all the parameters they
           * support, some of them may require to be handled in here as separate switch cases
           */
          switch (paramName) {
            case TIMEOUT_PARAM -> builder
                  .getClass()
                  .getMethod(paramName, Duration.class)
                  .invoke(builder, Duration.ofSeconds((Long) params.get(paramName)));

            case MAX_RETRIES_PARAM, THINKING_BUDGET_TOKENS, RANDOM_SEED -> builder
                .getClass()
                .getMethod(paramName, Integer.class)
                .invoke(builder, ((Long) params.get(paramName)).intValue());

              /*
               * For primitive params if there's only one setter available, we call it.
               * If there's choice we default to the string one
               */
            default -> {
              ArrayList<Method> paramNameMatches = new ArrayList<>();
              for (var method : builder.getClass().getMethods()) {
                if (paramName.equals(method.getName()) && method.getParameterCount() == 1) {
                  paramNameMatches.add(method);
                }
              }
              if (paramNameMatches.size() == 1) {
                paramNameMatches.getFirst().invoke(builder, params.get(paramName));
              } else {
                try {
                  builder
                      .getClass()
                      .getMethod(paramName, String.class)
                      .invoke(builder, params.get(paramName).toString());
                } catch (NoSuchMethodException e) {
                  log.error("Parameter {} not supported by model {}", paramName, className);
                  throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e.getMessage(), e);
                }
              }
            }
          }
        }
      }
      textToTextModel = (ChatModel) builder.getClass().getMethod("build").invoke(builder);
      return new SolrChatModel(name, textToTextModel, params);
    } catch (final Exception e) {
      throw new ChatModelException("Model loading failed for " + className, e);
    }
  }

  public SolrChatModel(
      String name, ChatModel chatModel, Map<String, Object> params) {
    this.name = name;
    this.chatModel = chatModel;
    this.params = params;
    this.hashCode = calculateHashCode();
  }

  public String chat(String text){
    ChatRequest chatRequest = ChatRequest.builder()
        //.responseFormat(responseFormat) // used for structured outputs
        .messages(UserMessage.from(text))
        .build();
    ChatResponse chatResponse = chatModel.chat(chatRequest);
    return chatResponse.aiMessage().text(); // To change in case of structured output support
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(name=" + getName() + ")";
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES
        + RamUsageEstimator.sizeOfObject(name)
        + RamUsageEstimator.sizeOfObject(chatModel);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  private int calculateHashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + Objects.hashCode(name);
    result = (prime * result) + Objects.hashCode(chatModel);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof SolrChatModel)) return false;
    final SolrChatModel other = (SolrChatModel) obj;
    return Objects.equals(chatModel, other.chatModel) && Objects.equals(name, other.name);
  }

  public String getName() {
    return name;
  }

  public String getChatModelClassName() {
    return chatModel.getClass().getName();
  }

  public Map<String, Object> getParams() {
    return params;
  }
}
