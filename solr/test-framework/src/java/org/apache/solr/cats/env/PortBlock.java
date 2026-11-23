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

package org.apache.solr.cats.env;

/**
 * The consecutive ports one test class owns, as many as the program under test asked for.
 *
 * <p>An interface rather than a record so that a project can put its own names on them — an {@code
 * admin()} or a {@code registry()} — by implementing it over the block the harness handed out, and
 * still hand that back to the generic code. The core knows only how many ports there are and which
 * index is the primary one; what each is for is the project's business.
 */
public interface PortBlock {

  /** The first port of the block, which is what the block is identified by. */
  int base();

  /** The port at {@code index}, counting from the base. */
  int at(int index);

  /** Every port of the block, in order. */
  int[] all();

  /** Whether {@code port} is one of this block's. */
  boolean contains(int port);

  /** Whether the program listens on nothing, and so was handed no ports. */
  default boolean isEmpty() {
    return all().length == 0;
  }
}
