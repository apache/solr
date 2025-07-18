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
package org.apache.solr.llm.textvectorisation.model;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.model.embedding.EmbeddingModel;
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
import org.apache.solr.llm.textvectorisation.store.TextToVectorModelException;
import org.apache.solr.llm.textvectorisation.store.rest.ManagedTextToVectorModelStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This object wraps a {@link dev.langchain4j.model.embedding.EmbeddingModel} to encode text to
 * vector. It's meant to be used as a managed resource with the {@link
 * ManagedTextToVectorModelStore}
 */
public class SolrTextToVectorModel implements Accountable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final long BASE_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(SolrTextToVectorModel.class);
  private static final String TIMEOUT_PARAM = "timeout";
  private static final String MAX_SEGMENTS_PER_BATCH_PARAM = "maxSegmentsPerBatch";
  private static final String MAX_RETRIES_PARAM = "maxRetries";

  private final String name;
  private final Map<String, Object> params;
  private final EmbeddingModel textToVector;
  private final int hashCode;

  public static SolrTextToVectorModel getInstance(
      SolrResourceLoader solrResourceLoader,
      String className,
      String name,
      Map<String, Object> params)
      throws TextToVectorModelException {
    try {
      /*
       * The idea here is to build a {@link dev.langchain4j.model.embedding.EmbeddingModel} using inversion
       * of control.
       * Each model has its own list of parameters we don't know beforehand, but each {@link dev.langchain4j.model.embedding.EmbeddingModel} class
       * has its own builder that uses setters with the same name of the parameter in input.
       * */
      EmbeddingModel textToVector;
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
      textToVector = (EmbeddingModel) builder.getClass().getMethod("build").invoke(builder);
      return new SolrTextToVectorModel(name, textToVector, params);
    } catch (final Exception e) {
      throw new TextToVectorModelException("Model loading failed for " + className, e);
    }
  }

  public SolrTextToVectorModel(
      String name, EmbeddingModel textToVector, Map<String, Object> params) {
    this.name = name;
    this.textToVector = textToVector;
    this.params = params;
    this.hashCode = calculateHashCode();
  }

  public float[] vectorise(String text) {
    Embedding vector = textToVector.embed(text).content();
    return vector.vector();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(name=" + getName() + ")";
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES
        + RamUsageEstimator.sizeOfObject(name)
        + RamUsageEstimator.sizeOfObject(textToVector);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  private int calculateHashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + Objects.hashCode(name);
    result = (prime * result) + Objects.hashCode(textToVector);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof SolrTextToVectorModel)) return false;
    final SolrTextToVectorModel other = (SolrTextToVectorModel) obj;
    return Objects.equals(textToVector, other.textToVector) && Objects.equals(name, other.name);
  }

  public String getName() {
    return name;
  }

  public String getEmbeddingModelClassName() {
    return textToVector.getClass().getName();
  }

  public Map<String, Object> getParams() {
    return params;
  }
}
