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

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class CircularIterator<T> implements Iterator<T> {

  private final Object[] collection;
  private final AtomicInteger idx;

  public CircularIterator(Collection<T> collection) {
    this.collection = Objects.requireNonNull(collection).toArray();
    if (this.collection.length == 0) {
      throw new IllegalArgumentException("This iterator doesn't support empty collections");
    }
    this.idx = new AtomicInteger();
  }

  @Override
  public boolean hasNext() {
    return true;
  }

  @SuppressWarnings("unchecked")
  @Override
  public T next() {
    return (T) collection[idx.incrementAndGet() % collection.length];
  }

  public int cycles() {
    return ((idx.get() - 1) / collection.length) + 1;
  }
}
