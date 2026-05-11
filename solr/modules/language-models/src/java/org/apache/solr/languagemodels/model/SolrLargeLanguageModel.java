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
package org.apache.solr.languagemodels.model;

import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.request.ResponseFormat;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.languagemodels.LanguageModelException;
import org.apache.solr.languagemodels.store.rest.ManagedLargeLanguageModelStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This object wraps a {@link dev.langchain4j.model.chat.ChatModel} to some content given a prompt
 * and a {@link ResponseFormat}. It's meant to be used as a managed resource with the {@link
 * ManagedLargeLanguageModelStore}
 */
public class SolrLargeLanguageModel extends SolrLanguageModel implements Accountable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final long BASE_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(SolrLargeLanguageModel.class);

  private static final String THINKING_BUDGET_TOKENS = "thinkingBudgetTokens";
  private static final String RANDOM_SEED = "randomSeed";

  private final ChatModel chatModel;
  private final int hashCode;

  public static SolrLargeLanguageModel getInstance(
      SolrResourceLoader solrResourceLoader,
      String className,
      String name,
      Map<String, Object> params)
      throws LanguageModelException {
    try {
      /*
       * The idea here is to build a {@link dev.langchain4j.model.chat.ChatModel} using inversion
       * of control.
       * Each model has its own list of parameters we don't know beforehand, but each {@link dev.langchain4j.model.chat.ChatModel} class
       * has its own builder that uses setters with the same name of the parameter in input.
       * */
      ChatModel chatModel;
      Class<?> modelClass = solrResourceLoader.findClass(className, ChatModel.class);
      var builder = modelClass.getMethod("builder").invoke(null);
      if (params != null) {
        /*
         * This block of code has the responsibility of instantiate a {@link
         * dev.langchain4j.model.chat.ChatModel} using the params provided. Classes have
         * params of the specific implementation of {@link
         * dev.langchain4j.model.chat.ChatModel}, which is not known beforehand. So we benefit of
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

      // Always enforce strict schema adherence where supported. For Anthropic and Google it's
      // enabled by default
      if (!"dev.langchain4j.model.anthropic.AnthropicChatModel".equals(className)
          && !"dev.langchain4j.model.googleai.GoogleAiGeminiChatModel".equals(className)) {
        try {
          builder.getClass().getMethod("strictJsonSchema", Boolean.class).invoke(builder, true);
        } catch (NoSuchMethodException ignored) {
          log.debug(
              "Model {} does not have strictJsonSchema param, structured output is not enforced",
              className);
        }
      }
      chatModel = (ChatModel) builder.getClass().getMethod("build").invoke(builder);
      return new SolrLargeLanguageModel(name, chatModel, params);
    } catch (final Exception e) {
      throw new LanguageModelException("Model loading failed for " + className, e);
    }
  }

  public SolrLargeLanguageModel(String name, ChatModel chatModel, Map<String, Object> params) {
    super(name, params);
    this.chatModel = chatModel;
    this.hashCode = calculateHashCode();
  }

  /**
   * Sends a structured chat request to the language model and returns the raw text response.
   *
   * @param prompt the user prompt to send to the language model
   * @param responseFormat the format specification that instructs the model to produce structured
   *     JSON output
   * @return the raw text response from the language model
   */
  public String generate(String prompt, ResponseFormat responseFormat) {
    ChatRequest chatRequest =
        ChatRequest.builder()
            .responseFormat(responseFormat)
            .messages(UserMessage.from(prompt))
            .build();
    return chatModel.chat(chatRequest).aiMessage().text();
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
    if (!(obj instanceof SolrLargeLanguageModel)) return false;
    final SolrLargeLanguageModel other = (SolrLargeLanguageModel) obj;
    return Objects.equals(chatModel, other.chatModel) && Objects.equals(name, other.name);
  }

  @Override
  public String getModelClassName() {
    return chatModel.getClass().getName();
  }
}
