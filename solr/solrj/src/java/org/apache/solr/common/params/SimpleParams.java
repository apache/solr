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
package org.apache.solr.common.params;

/** Parameters used by the SimpleQParser. */
public interface SimpleParams {
  /** Query fields and boosts. */
  String QF = "qf";

  /** Override the currently enabled/disabled query operators. */
  String QO = "q.operators";

  /** Enables {@code AND} operator (+) */
  String AND_OPERATOR = "AND";

  /** Enables {@code NOT} operator (-) */
  String NOT_OPERATOR = "NOT";

  /** Enables {@code OR} operator (|) */
  String OR_OPERATOR = "OR";

  /** Enables {@code PREFIX} operator (*) */
  String PREFIX_OPERATOR = "PREFIX";

  /** Enables {@code PHRASE} operator (") */
  String PHRASE_OPERATOR = "PHRASE";

  /** Enables {@code PRECEDENCE} operators: {@code (} and {@code )} */
  String PRECEDENCE_OPERATORS = "PRECEDENCE";

  /** Enables {@code ESCAPE} operator (\) */
  String ESCAPE_OPERATOR = "ESCAPE";

  /** Enables {@code WHITESPACE} operators: ' ' '\n' '\r' '\t' */
  String WHITESPACE_OPERATOR = "WHITESPACE";

  /** Enables {@code FUZZY} operator (~) */
  String FUZZY_OPERATOR = "FUZZY";

  /** Enables {@code NEAR} operator (~) */
  String NEAR_OPERATOR = "NEAR";
}
