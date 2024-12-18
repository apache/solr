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
package org.apache.solr.llm.texttovector.model;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;
import java.util.ArrayList;
import java.util.List;

public class DummyEmbeddingModel implements EmbeddingModel {
  final float[] embedding;

  public DummyEmbeddingModel(float[] embedding) {
    this.embedding = embedding;
  }

  @Override
  public Response<Embedding> embed(String text) {
    Embedding dummy = new Embedding(embedding);
    return new Response<Embedding>(dummy);
  }

  @Override
  public Response<Embedding> embed(TextSegment textSegment) {
    Embedding dummy = new Embedding(embedding);
    return new Response<Embedding>(dummy);
  }

  @Override
  public Response<List<Embedding>> embedAll(List<TextSegment> textSegments) {
    return null;
  }

  @Override
  public int dimension() {
    return embedding.length;
  }

  public static DummyEmbeddingModelBuilder builder() {
    return new DummyEmbeddingModelBuilder();
  }

  public static class DummyEmbeddingModelBuilder {
    private float[] builderEmbeddings;
    private int intValue;

    public DummyEmbeddingModelBuilder() {}

    public DummyEmbeddingModelBuilder embedding(ArrayList<Double> embeddings) {
      this.builderEmbeddings = new float[embeddings.size()];
      for (int i = 0; i < embeddings.size(); i++) {
        this.builderEmbeddings[i] = embeddings.get(i).floatValue();
      }
      return this;
    }

    public DummyEmbeddingModelBuilder unsupported(Integer input) {
      return this;
    }

    public DummyEmbeddingModelBuilder ambiguous(int input) {
      this.intValue = input;
      return this;
    }

    public DummyEmbeddingModelBuilder ambiguous(String input) {
      this.intValue = Integer.valueOf(input);
      return this;
    }

    public DummyEmbeddingModel build() {
      return new DummyEmbeddingModel(this.builderEmbeddings);
    }
  }
}
