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

import java.util.Map;

/**
 * Interface for text-to-vector embedding models.
 *
 * <p>Implement this interface to create custom embedding model integrations (e.g., Triton Inference
 * Server, TensorFlow Serving, or any custom HTTP endpoint).
 *
 * <p>Implementations must have either:
 *
 * <ul>
 *   <li>A public no-arg constructor, or
 *   <li>A static {@code builder()} method returning a builder with a {@code build()} method
 * </ul>
 *
 * <p>Example usage in model configuration:
 *
 * <pre>
 * {
 *   "class": "com.example.MyCustomEmbeddingModel",
 *   "name": "my-model",
 *   "params": {
 *     "endpoint": "http://my-server:8000/embed",
 *     "api_key": "my-api-key",
 *     "dimension": 384
 *   }
 * }
 * </pre>
 */
public interface TextToVectorModel {
  /**
   * Convert text to a vector embedding.
   *
   * @param text the input text to vectorise
   * @return the embedding vector as a float array
   */
  float[] vectorise(String text);

  /**
   * Initialize the model with configuration parameters. Called after construction with the params
   * from the model configuration.
   *
   * @param params the configuration parameters from the model JSON
   */
  default void init(Map<String, Object> params) {}
}
