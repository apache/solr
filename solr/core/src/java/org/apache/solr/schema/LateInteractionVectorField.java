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
package org.apache.solr.schema;

import org.apache.lucene.search.DoubleValuesSource;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;

/**
 * This is a marrker interface indicating that a {@link FieldType} supports query time usage as
 * "late interaction" vector field
 *
 * @lucene.internal
 * @lucene.experimental Not currently intended for implementation by custom FieldTypes
 */
public interface LateInteractionVectorField {

  // TODO: Refactor StrFloatLateInteractionVectorField if/when more classes implement this
  // interface.
  //
  // The surface area of this interface is intentionally small to focus on the query time aspects
  //
  // If/When we want to add more FieldTypes that implement this interface, we should strongly
  // consider refactoring some of the StrFloatLateInteractionVectorField internals into an
  // abstract base class for re-use.
  //
  // What that abstract base class should look like (and what should be refactored will largely
  // depend on *why* more implementations are being added:
  //  - new external representations ? (ie: not a single String)
  //  - new internal implementation ? (ie: int/byte vectors)

  /**
   * Method used for parsing some type specific query input structure into Value source.
   *
   * <p>At the time this method is called, the {@link FunctionQParser} will have already been used
   * to parse the <code>fieldName</code> which will have been resolved to a {@link FieldType} which
   * must implement this interface
   *
   * <p>This method should be responsible for whatever type specific argument parsing is neccessary,
   * and confirm that no invalid (or unexpected "extra") arguments exist in the <code>
   * FunctionQParser</code>
   *
   * <p>(If field types implementing this method need the {@link SchemaField} corrisponding to the
   * <code>fieldName</code>, they should maintain a reference to the {@link IndexSchema} used to
   * initialize the <code>FieldType</code> instance.
   */
  public DoubleValuesSource parseLateInteractionValuesSource(
      final String fieldName, final FunctionQParser fp) throws SyntaxError;
}
