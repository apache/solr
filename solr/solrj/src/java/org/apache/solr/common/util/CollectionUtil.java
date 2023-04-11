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
package org.apache.solr.common.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;

/**
 * Methods for creating collections with exact sizes, and other convenience methods
 *
 * @lucene.internal
 */
@SuppressForbidden(reason = "This class properly sizes the collections")
public final class CollectionUtil {

  private CollectionUtil() {} // no instance

  /**
   * Returns a new {@link HashMap} sized to contain {@code size} items without resizing the internal
   * array.
   */
  public static <K, V> HashMap<K, V> newHashMap(int size) {
    // With Lucene 9.5 - we should replace this with
    // org.apache.lucene.util.CollectionUtil.newHashMap(int size)
    // Replace with HashMap.newHashMap when Solr moves to minimum jdk19
    return new HashMap<>((int) (size / 0.75f) + 1);
  }

  /**
   * Returns a new {@link LinkedHashMap} sized to contain {@code size} items without resizing the
   * internal array.
   */
  public static <K, V> LinkedHashMap<K, V> newLinkedHashMap(int size) {
    // Replace with LinkedHashMap.newLinkedHashMap when Solr moves to minimum jdk19
    return new LinkedHashMap<>((int) (size / 0.75f) + 1);
  }

  /**
   * Returns a new {@link HashSet} sized to contain {@code size} items without resizing the internal
   * array.
   */
  public static <E> HashSet<E> newHashSet(int size) {
    // With Lucene 9.5 - we should replace this with
    // org.apache.lucene.util.CollectionUtil.newHashSet(int size)
    // Replace with HashSet.newHashSet when Solr moves to minimum jdk19
    return new HashSet<>((int) (size / 0.75f) + 1);
  }

  public static boolean isEmpty(Collection<?> collection) {
    return collection == null || collection.isEmpty();
  }

  public static boolean isNotEmpty(Collection<?> collection) {
    return !isEmpty(collection);
  }
}
