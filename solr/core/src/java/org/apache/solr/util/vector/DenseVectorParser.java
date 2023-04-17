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

import java.util.List;
import org.apache.solr.common.SolrException;

public abstract class DenseVectorParser {

  public enum BuilderPhase {
    INDEX,
    QUERY
  }

  protected BuilderPhase builderPhase;

  protected int dimension;
  protected Object inputValue;

  public float[] getFloatVector() {
    throw new UnsupportedOperationException("Float vector parsing is not supported");
  }

  public byte[] getByteVector() {
    throw new UnsupportedOperationException("Byte vector parsing is not supported");
  }

  protected void parseVector() {
    switch (builderPhase) {
      case INDEX:
        parseIndexVector();
        break;
      case QUERY:
        parseQueryVector();
        break;
    }
  }

  protected void parseIndexVector() {
    if (!(inputValue instanceof List)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "incorrect vector format. " + errorMessage());
    }
    List<?> inputVector = (List<?>) inputValue;
    checkVectorDimension(inputVector.size());
    if (inputVector.get(0) instanceof CharSequence) {
      for (int i = 0; i < dimension; i++) {
        try {
          addStringElement(inputVector.get(i).toString());
        } catch (NumberFormatException e) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "incorrect vector element: '" + inputVector.get(i) + "'. " + errorMessage());
        }
      }
    } else if (inputVector.get(0) instanceof Number) {
      for (int i = 0; i < dimension; i++) {
        addNumberElement((Number) inputVector.get(i));
      }
    } else {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "incorrect vector format. " + errorMessage());
    }
  }

  protected void parseQueryVector() {

    String value = inputValue.toString();
    if (!value.startsWith("[") || !value.endsWith("]")) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "incorrect vector format. " + errorMessage());
    }

    String[] elements = value.substring(1, value.length() - 1).split(",");
    checkVectorDimension(elements.length);

    for (int i = 0; i < dimension; i++) {
      try {
        addStringElement(elements[i].trim());
      } catch (NumberFormatException e) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "incorrect vector element: '" + elements[i] + "'. " + errorMessage());
      }
    }
  }

  protected void checkVectorDimension(int inputVectorDimension) {
    if (inputVectorDimension != dimension) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "incorrect vector dimension."
              + " The vector value has size "
              + inputVectorDimension
              + " while it is expected a vector with size "
              + dimension);
    }
  }

  protected abstract void addNumberElement(Number element);

  protected abstract void addStringElement(String element);

  protected abstract String errorMessage();
}
