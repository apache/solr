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

import dev.langchain4j.model.embedding.EmbeddingModel;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.languagemodels.textvectorisation.store.TextToVectorModelException;
import org.apache.solr.languagemodels.textvectorisation.store.rest.ManagedTextToVectorModelStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This object wraps a {@link TextToVectorModel} to encode text to vector. It's meant to be used as
 * a managed resource with the {@link ManagedTextToVectorModelStore}.
 *
 * <p>Supports two types of model implementations:
 *
 * <ul>
 *   <li>Solr-native {@link TextToVectorModel} implementations for custom integrations
 *   <li>LangChain4j {@link EmbeddingModel} implementations (wrapped via {@link
 *       Langchain4jModelAdapter})
 * </ul>
 */
public class SolrTextToVectorModel implements Accountable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final long BASE_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(SolrTextToVectorModel.class);

  private final String name;
  private final String className;
  private final Map<String, Object> params;
  private final TextToVectorModel textToVector;
  private final int hashCode;

  public static SolrTextToVectorModel getInstance(
      SolrResourceLoader solrResourceLoader,
      String className,
      String name,
      Map<String, Object> params)
      throws TextToVectorModelException {

    TextToVectorModel textToVector = createModel(solrResourceLoader, className, params);
    return new SolrTextToVectorModel(name, className, textToVector, params);
  }

  /**
   * Create a TextToVectorModel instance from the given class name. First tries to load as a
   * Solr-native TextToVectorModel, then falls back to LangChain4j EmbeddingModel wrapped in an
   * adapter.
   */
  private static TextToVectorModel createModel(
      SolrResourceLoader solrResourceLoader, String className, Map<String, Object> params)
      throws TextToVectorModelException {

    // First, try to load as a Solr-native TextToVectorModel
    try {
      Class<?> clazz = solrResourceLoader.findClass(className, Object.class);

      if (TextToVectorModel.class.isAssignableFrom(clazz)) {
        log.info("Loading Solr-native TextToVectorModel: {}", className);
        return createSolrNativeModel(solrResourceLoader, className, params);
      }

      if (EmbeddingModel.class.isAssignableFrom(clazz)) {
        log.info("Loading LangChain4j EmbeddingModel via adapter: {}", className);
        return Langchain4jModelAdapter.create(solrResourceLoader, className, params);
      }

      throw new TextToVectorModelException(
          "Class "
              + className
              + " must implement either "
              + TextToVectorModel.class.getName()
              + " or "
              + EmbeddingModel.class.getName());

    } catch (TextToVectorModelException e) {
      throw e;
    } catch (Exception e) {
      throw new TextToVectorModelException("Model loading failed for " + className, e);
    }
  }

  /**
   * Create a Solr-native TextToVectorModel using either: 1. Builder pattern (if static builder()
   * method exists) 2. No-arg constructor + init(params)
   */
  private static TextToVectorModel createSolrNativeModel(
      SolrResourceLoader solrResourceLoader, String className, Map<String, Object> params)
      throws TextToVectorModelException {
    try {
      Class<?> modelClass = solrResourceLoader.findClass(className, TextToVectorModel.class);

      TextToVectorModel model;

      // Try builder pattern first
      try {
        var builderMethod = modelClass.getMethod("builder");
        var builder = builderMethod.invoke(null);

        // Apply params to builder using reflection
        if (params != null) {
          for (Map.Entry<String, Object> entry : params.entrySet()) {
            String paramName = entry.getKey();
            Object paramValue = entry.getValue();
            try {
              // Find matching setter method on builder
              for (var method : builder.getClass().getMethods()) {
                if (paramName.equals(method.getName()) && method.getParameterCount() == 1) {
                  Class<?> paramType = method.getParameterTypes()[0];
                  Object convertedValue = ModelConfigUtils.convertValue(paramValue, paramType);
                  method.invoke(builder, convertedValue);
                  break;
                }
              }
            } catch (Exception e) {
              log.warn("Could not set parameter {} on builder for {}", paramName, className, e);
            }
          }
        }

        model = (TextToVectorModel) builder.getClass().getMethod("build").invoke(builder);
        log.debug("Created {} using builder pattern", className);

      } catch (NoSuchMethodException e) {
        // Fall back to no-arg constructor + init()
        model = (TextToVectorModel) modelClass.getDeclaredConstructor().newInstance();
        if (params != null) {
          model.init(params);
        }
        log.debug("Created {} using no-arg constructor + init()", className);
      }

      return model;

    } catch (Exception e) {
      throw new TextToVectorModelException("Failed to create Solr-native model: " + className, e);
    }
  }

  public SolrTextToVectorModel(
      String name, String className, TextToVectorModel textToVector, Map<String, Object> params) {
    this.name = name;
    this.className = className;
    this.textToVector = textToVector;
    this.params = params;
    this.hashCode = calculateHashCode();
  }

  public float[] vectorise(String text) {
    return textToVector.vectorise(text);
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
    return className;
  }

  public Map<String, Object> getParams() {
    return params;
  }
}
