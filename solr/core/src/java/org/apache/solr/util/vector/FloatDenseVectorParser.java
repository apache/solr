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
package org.apache.solr.util.vector;

public class FloatDenseVectorParser extends DenseVectorParser {
  private float[] vector;
  private int curPosition;

  public FloatDenseVectorParser(int dimension, Object inputValue, BuilderPhase builderPhase) {
    this.dimension = dimension;
    this.inputValue = inputValue;
    this.curPosition = 0;
    this.builderPhase = builderPhase;
  }

  @Override
  public float[] getFloatVector() {
    if (vector == null) {
      vector = new float[dimension];
      parseVector();
    }
    return vector;
  }

  @Override
  protected void addNumberElement(Number element) {
    vector[curPosition++] = element.floatValue();
  }

  @Override
  protected void addStringElement(String element) {
    vector[curPosition++] = Float.parseFloat(element);
  }

  @Override
  protected String errorMessage() {
    return "The expected format is:'[f1,f2..f3]' where each element f is a float";
  }
}
