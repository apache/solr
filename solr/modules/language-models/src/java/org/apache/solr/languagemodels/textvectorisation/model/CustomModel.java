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
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.solr.common.util.CollectionUtil;

public class CustomModel implements EmbeddingModel {

  private final String customUrl;
  private final Long customVersion;
  private final float[] defaultEmbedding;
  private final Map<String, float[]> customEmbeddings;

  private CustomModel(
      String customUrl,
      Long customVersion,
      float[] defaultEmbedding,
      Map<String, float[]> customEmbeddings) {
    this.customUrl = customUrl;
    this.customVersion = customVersion;
    this.defaultEmbedding = defaultEmbedding;
    this.customEmbeddings = customEmbeddings;
  }

  private Embedding embedding(String text) {
    float[] embedding = customEmbeddings.getOrDefault(text, defaultEmbedding);
    return new Embedding(embedding);
  }

  @Override
  public Response<Embedding> embed(String text) {
    return new Response<>(embedding(text));
  }

  @Override
  public Response<Embedding> embed(TextSegment textSegment) {
    return new Response<>(embedding(textSegment.text()));
  }

  @Override
  public Response<List<Embedding>> embedAll(List<TextSegment> textSegments) {
    List<Embedding> embeddings = new ArrayList<>(textSegments.size());
    for (TextSegment textSegment : textSegments) {
      embeddings.add(embedding(textSegment.text()));
    }
    return new Response<>(embeddings);
  }

  @Override
  public int dimension() {
    return this.defaultEmbedding.length;
  }

  public static CustomModelBuilder builder() {
    return new CustomModelBuilder();
  }

  public static class CustomModelBuilder {
    private String customUrl;
    private Long customVersion;
    private float[] defaultEmbedding;
    private Map<String, float[]> customEmbeddings;

    public CustomModelBuilder() {}

    public CustomModelBuilder customUrl(String customUrl) {
      this.customUrl = customUrl;
      return this;
    }

    public CustomModelBuilder customVersion(Number customVersion) {
      this.customVersion = customVersion.longValue();
      return this;
    }

    public CustomModelBuilder defaultEmbedding(ArrayList<Double> defaultEmbedding) {
      float[] converted = new float[defaultEmbedding.size()];
      for (int i = 0; i < defaultEmbedding.size(); i++) {
        converted[i] = defaultEmbedding.get(i).floatValue();
      }
      this.defaultEmbedding = converted;
      return this;
    }

    public CustomModelBuilder customEmbeddings(Map<String, ArrayList<Double>> customEmbeddings) {
      Map<String, float[]> converted = CollectionUtil.newHashMap(customEmbeddings.size());
      for (Map.Entry<String, ArrayList<Double>> entry : customEmbeddings.entrySet()) {
        ArrayList<Double> values = entry.getValue();
        float[] embedding = new float[values.size()];
        for (int i = 0; i < values.size(); i++) {
          embedding[i] = values.get(i).floatValue();
        }
        converted.put(entry.getKey(), embedding);
      }
      this.customEmbeddings = converted;
      return this;
    }

    public CustomModel build() {
      if (defaultEmbedding == null) {
        throw new IllegalArgumentException("defaultEmbedding must not be null");
      }
      if (customEmbeddings == null) {
        throw new IllegalArgumentException("customEmbeddings must not be null");
      }
      final int expectedDimension = defaultEmbedding.length;
      for (Map.Entry<String, float[]> entry : customEmbeddings.entrySet()) {
        final int actualDimension = entry.getValue().length;
        if (actualDimension != expectedDimension) {
          throw new IllegalArgumentException(
              "Custom embedding for key '"
                  + entry.getKey()
                  + "' has dimension "
                  + actualDimension
                  + ", expected "
                  + expectedDimension);
        }
      }
      return new CustomModel(customUrl, customVersion, defaultEmbedding, customEmbeddings);
    }
  }
}
