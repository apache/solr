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

package org.apache.solr.shell.exec;

/**
 * Everything a scope has run so far, for a failure message to quote.
 *
 * <p>An interface so that {@link CommandResult}, which is what a test holds when it asserts, can
 * reach the running transcript without this package knowing about sessions.
 */
public interface CommandHistory {

  /** The commands run so far, in order, each with its exit status and output. */
  String text();

  /** A history that has nothing to show, for a result built outside a session. */
  CommandHistory EMPTY = () -> "";
}
