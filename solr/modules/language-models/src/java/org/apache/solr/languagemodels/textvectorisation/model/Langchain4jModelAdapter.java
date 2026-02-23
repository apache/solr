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
package org.apache.solr.languagemodels.textvectorisation.model;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.model.embedding.EmbeddingModel;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.languagemodels.textvectorisation.store.TextToVectorModelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adapter that wraps a LangChain4j {@link EmbeddingModel} to implement the Solr-native {@link
 * TextToVectorModel} interface.
 *
 * <p>This provides backward compatibility for existing LangChain4j model configurations (OpenAI,
 * Cohere, HuggingFace, MistralAI).
 */
public class Langchain4jModelAdapter implements TextToVectorModel {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String TIMEOUT_PARAM = "timeout";
  private static final String MAX_SEGMENTS_PER_BATCH_PARAM = "maxSegmentsPerBatch";
  private static final String MAX_RETRIES_PARAM = "maxRetries";

  private final EmbeddingModel delegate;

  public Langchain4jModelAdapter(EmbeddingModel delegate) {
    this.delegate = delegate;
  }

  /**
   * Create a Langchain4jModelAdapter by instantiating the specified EmbeddingModel class using
   * reflection and the builder pattern.
   */
  public static Langchain4jModelAdapter create(
      SolrResourceLoader solrResourceLoader, String className, Map<String, Object> params)
      throws TextToVectorModelException {
    try {
      Class<?> modelClass = solrResourceLoader.findClass(className, EmbeddingModel.class);
      var builder = modelClass.getMethod("builder").invoke(null);

      if (params != null) {
        /*
         * This block of code has the responsibility of instantiate a {@link
         * dev.langchain4j.model.embedding.EmbeddingModel} using the params provided.classes have
         * params of The specific implementation of {@link
         * dev.langchain4j.model.embedding.EmbeddingModel} is not known beforehand. So we benefit of
         * the design choice in langchain4j that each subclass implementing {@link
         * dev.langchain4j.model.embedding.EmbeddingModel} uses setters with the same name of the
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
            case TIMEOUT_PARAM -> {
              Duration timeOut = Duration.ofSeconds((Long) params.get(paramName));
              builder.getClass().getMethod(paramName, Duration.class).invoke(builder, timeOut);
            }
            case MAX_SEGMENTS_PER_BATCH_PARAM, MAX_RETRIES_PARAM -> builder
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
                Method method = paramNameMatches.getFirst();
                Class<?> paramType = method.getParameterTypes()[0];
                Object convertedValue =
                    ModelConfigUtils.convertValue(params.get(paramName), paramType);
                method.invoke(builder, convertedValue);
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

      EmbeddingModel embeddingModel =
          (EmbeddingModel) builder.getClass().getMethod("build").invoke(builder);
      return new Langchain4jModelAdapter(embeddingModel);
    } catch (final Exception e) {
      throw new TextToVectorModelException("LangChain4j model loading failed for " + className, e);
    }
  }

  @Override
  public float[] vectorise(String text) {
    Embedding vector = delegate.embed(text).content();
    return vector.vector();
  }
}
