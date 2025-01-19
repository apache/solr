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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.ImmutablePair;

/**
 * <code>SimpleOrderedMap</code> is a {@link NamedList} where access by key is more important than
 * maintaining order when it comes to representing the held data in other forms, as ResponseWriters
 * normally do. It's normally not a good idea to repeat keys or use null keys, but this is not
 * enforced. If key uniqueness enforcement is desired, use a regular {@link Map}.
 *
 * <p>For example, a JSON response writer may choose to write a SimpleOrderedMap as
 * {"foo":10,"bar":20} and may choose to write a NamedList as ["foo",10,"bar",20]. An XML response
 * writer may choose to render both the same way.
 *
 * <p>This class does not provide efficient lookup by key, its main purpose is to hold data to be
 * serialized. It aims to minimize overhead and to be efficient at adding new elements.
 */
public class SimpleOrderedMap<T> extends NamedList<T> implements Map<String, T> {

  private static final SimpleOrderedMap<Object> EMPTY = new SimpleOrderedMap<>(List.of());

  /** Creates an empty instance */
  public SimpleOrderedMap() {
    super();
  }

  public SimpleOrderedMap(int sz) {
    super(sz);
  }

  /**
   * Creates an instance backed by an explicitly specified list of pairwise names/values.
   *
   * <p>TODO: this method was formerly public, now that it's not we can change the impl details of
   * this class to be based on a Map.Entry[]
   *
   * @param nameValuePairs underlying List which should be used to implement a SimpleOrderedMap;
   *     modifying this List will affect the SimpleOrderedMap.
   * @lucene.internal
   */
  private SimpleOrderedMap(List<Object> nameValuePairs) {
    super(nameValuePairs);
  }

  public SimpleOrderedMap(Map.Entry<String, T>[] nameValuePairs) {
    super(nameValuePairs);
  }

  @Override
  public SimpleOrderedMap<T> clone() {
    ArrayList<Object> newList = new ArrayList<>(nvPairs.size());
    newList.addAll(nvPairs);
    return new SimpleOrderedMap<T>(newList);
  }

  /**
   * Returns a shared, empty, and immutable instance of SimpleOrderedMap.
   *
   * @return Empty SimpleOrderedMap (immutable)
   */
  public static SimpleOrderedMap<Object> of() {
    return EMPTY;
  }

  /**
   * Returns an immutable instance of SimpleOrderedMap with a single key-value pair.
   *
   * @return SimpleOrderedMap containing one key-value pair
   */
  public static <T> SimpleOrderedMap<T> of(String name, T val) {
    return new SimpleOrderedMap<T>(List.of(name, val));
  }

  @Override
  public boolean isEmpty() {
    return nvPairs.isEmpty();
  }

  @Override
  public boolean containsKey(final Object key) {
    return super.get((String) key) != null;
  }

  @Override
  public boolean containsValue(final Object value) {
    int sz = size();
    for (int i = 0; i < sz; i++) {
      if (value.equals(getVal(i))) {
        return true;
      }
    }
    return false;
  }

  @Override
  public T get(final Object key) {
    return super.get((String) key);
  }

  @Override
  public T put(final String key, final T value) {
    T oldValue = get(key);
    add(key, value);
    return oldValue;
  }

  @Override
  public T remove(final Object key) {
    return super.remove((String) key);
  }

  @Override
  public void putAll(final Map<? extends String, ? extends T> m) {
    m.forEach(this::put);
  }

  @Override
  public Set<String> keySet() {
    var keys = new HashSet<String>();
    for (int i = 0; i < nvPairs.size(); i += 2) {
      keys.add((String) nvPairs.get(i));
    }
    return keys;
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public Collection<T> values() {
    var values = new ArrayList<T>();
    for (int i = 1; i < nvPairs.size(); i += 2) {
      values.add((T) nvPairs.get(i));
    }
    return values;
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public Set<Entry<String, T>> entrySet() {
    var values = new HashSet<Entry<String, T>>();
    for (int i = 0; i < nvPairs.size() - 1; i +=2) {
		values.add(ImmutablePair.of((String) nvPairs.get(i), (T) nvPairs.get(i + 1)));
    }
    return values;
  }
}
