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
package org.apache.solr.bench;

import java.util.SplittableRandom;
import org.apache.solr.common.SolrInputDocument;

/**
 * Provides the definition for a randomly generated field in a {@link SolrInputDocument} created by
 * a {@link DocMaker}.
 */
public class FieldDef {
  public static final int DEFAULT_MAX_LENGTH = 64;

  private DocMaker.Content content;
  private int numTokens = 1;
  private int maxNumTokens = -1;
  private int maxCardinality = -1;
  private int maxLength = -1;
  private int length = -1;
  private long cardinalityStart;

  public int getNumTokens() {
    return numTokens;
  }

  public int getMaxNumTokens() {
    return maxNumTokens;
  }

  public int getMaxCardinality() {
    return maxCardinality;
  }

  public long getCardinalityStart() {
    return cardinalityStart;
  }

  public int getMaxLength() {
    return maxLength;
  }

  public int getLength() {
    return length;
  }

  public DocMaker.Content getContent() {
    return content;
  }

  public static final class FieldDefBuilder {

    private DocMaker.Content content;
    private int numTokens = 1;
    private int maxNumTokens = -1;
    private int maxCardinality = -1;
    private int maxLength = -1;
    private int length = -1;
    private long cardinalityStart;

    private FieldDefBuilder() {}

    public static FieldDefBuilder aFieldDef() {
      return new FieldDefBuilder();
    }

    public FieldDefBuilder withContent(DocMaker.Content content) {
      this.content = content;
      return this;
    }

    public FieldDefBuilder withTokenCount(int numTokens) {
      if (numTokens > 1 && content == DocMaker.Content.UNIQUE_INT) {
        throw new UnsupportedOperationException(
            "UNIQUE_INT content type cannot be used with token count > 1");
      }
      if (maxCardinality > 1) {
        throw new UnsupportedOperationException(
            "tokenCount cannot be used with maxCardinality > 0");
      }
      this.numTokens = numTokens;
      return this;
    }

    public FieldDefBuilder withMaxTokenCount(int maxNumTokens) {
      if (numTokens > 1 && content == DocMaker.Content.UNIQUE_INT) {
        throw new UnsupportedOperationException(
            "UNIQUE_INT content type cannot be used with token count > 1");
      }
      if (maxCardinality > 1) {
        throw new UnsupportedOperationException(
            "maxNumTokens cannot be used with maxCardinality > 0");
      }
      this.maxNumTokens = maxNumTokens;
      return this;
    }

    public FieldDefBuilder withMaxCardinality(int maxCardinality, SplittableRandom random) {
      if (numTokens > 1) {
        throw new UnsupportedOperationException(
            "maxCardinality cannot be used with token count > 1");
      }
      this.maxCardinality = maxCardinality;
      this.cardinalityStart = random.nextLong(0, Long.MAX_VALUE - maxCardinality);
      return this;
    }

    public FieldDefBuilder withMaxLength(int maxLength) {
      if (length > -1) {
        throw new UnsupportedOperationException("maxLength cannot be used with length");
      }
      this.maxLength = maxLength;
      return this;
    }

    public FieldDefBuilder withLength(int length) {
      if (maxLength > -1) {
        throw new UnsupportedOperationException("length cannot be used with maxLength");
      }
      this.length = length;
      return this;
    }

    public FieldDef build() {
      FieldDef fieldDef = new FieldDef();
      fieldDef.numTokens = this.numTokens;
      fieldDef.maxNumTokens = this.maxNumTokens;
      fieldDef.content = this.content;
      fieldDef.maxCardinality = this.maxCardinality;
      fieldDef.maxLength = this.maxLength == -1 ? DEFAULT_MAX_LENGTH : this.maxLength;
      fieldDef.length = this.length;
      fieldDef.cardinalityStart = this.cardinalityStart;
      return fieldDef;
    }
  }

  @Override
  public String toString() {
    return "FieldDef{"
        + "content="
        + content
        + ", numTokens="
        + numTokens
        + ", maxNumTokens="
        + maxNumTokens
        + ", maxCardinality="
        + maxCardinality
        + ", maxLength="
        + maxLength
        + ", length="
        + length
        + ", cardinalityStart="
        + cardinalityStart
        + '}';
  }
}
