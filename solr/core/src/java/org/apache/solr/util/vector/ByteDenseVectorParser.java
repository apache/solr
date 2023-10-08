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

public class ByteDenseVectorParser extends DenseVectorParser {
  private byte[] byteVector;
  private int curPosition;

  public ByteDenseVectorParser(int dimension, Object inputValue, BuilderPhase builderPhase) {
    this.dimension = dimension;
    this.inputValue = inputValue;
    this.builderPhase = builderPhase;
    this.curPosition = 0;
  }

  @Override
  public byte[] getByteVector() {
    if (byteVector == null) {
      byteVector = new byte[dimension];
      parseVector();
    }
    return byteVector;
  }

  @Override
  protected void addNumberElement(Number element) {
    byteVector[curPosition++] = element.byteValue();
  }

  @Override
  protected void addStringElement(String element) {
    byteVector[curPosition++] = Byte.parseByte(element);
  }

  @Override
  protected String errorMessage() {
    return "The expected format is:'[b1,b2..b3]' where each element b is a byte (-128 to 127)";
  }
}
