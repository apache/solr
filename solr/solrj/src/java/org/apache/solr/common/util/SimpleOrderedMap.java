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

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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

  @Override
  public boolean isEmpty() {
    return nvPairs.isEmpty();
  }

  @Override
  public boolean containsKey(final Object key) {
    return this.indexOf((String) key) >= 0;
  }

  /**
   * Returns {@code true} if this map maps one or more keys to the specified value.
   *
   * @param value value whose presence in this map is to be tested
   * @return {@code true} if this map maps one or more keys to the specified value
   */
  @Override
  public boolean containsValue(final Object value) {
    int sz = size();
    for (int i = 0; i < sz; i++) {
      T val = getVal(i);
      if (Objects.equals(value, val)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Has linear lookup time O(N)
   * @see NamedList#get(String)
   */
  @Override
  public T get(final Object key) {
    return super.get((String) key);
  }

  /**
   * Associates the specified value with the specified key in this map
   * If the map previously contained a mapping for the key, the old value is replaced by the specified value.
   *
   * @param key  key with which the specified value is to be associated value – value to be associated with the specified key
   * @param value to be associated with the specified key            
   * @return the previous value associated with key, or null if there was no mapping for key. 
   * (A null return can also indicate that the map previously associated null with key)
   */
  @Override
  public T put(String key, T value) {
    int idx = indexOf(key);
    if (idx == -1) {
      add(key, value);
      return null;
    } else {
      T t = get(key);
      setVal(idx, value);
      return t;
    }
  }

  /**
   * @see NamedList#remove(String)
   */
  @Override
  public T remove(final Object key) {
    return super.remove((String) key);
  }

  /**
   * Copies all of the mappings from the specified map to this map.
   * These mappings will replace any mappings that this map had for
   * any of the keys currently in the specified map.
   *
   * @param m mappings to be stored in this map
   * @throws NullPointerException if the specified map is null
   */
  @Override
  public void putAll(final Map<? extends String, ? extends T> m) {
    m.forEach(this::put);
  }

  /**
   * return a  {@link Set} of all keys in the map.
   * @return  {@link Set} of all keys in the map
   */
  @Override
  public Set<String> keySet() {
    return new InnerMap().keySet();
  }

  /**
   * return a {@link Collection} of all values in the map.
   * @return {@link Collection} of all values in the map
   */
  @Override
  public Collection<T> values() {
    return new InnerMap().values();
  }

  /**
   * Returns a {@link Set} view of the mappings contained in this map. 
   * @return  {@link Set} view of mappings
   */
  @Override
  public Set<Entry<String, T>> entrySet() {

    return new AbstractSet<>() {
      @Override
      public Iterator<Entry<String, T>> iterator() {
        return SimpleOrderedMap.this.iterator();
      }

      @Override
      public int size() {
        return SimpleOrderedMap.this.size();
      }
    };
  }

  /**
   * Returns an immutable instance of {@link SimpleOrderedMap} with a single key-value pair.
   *
   * @return {@link SimpleOrderedMap} containing one key-value pair
   */
  public static <T> SimpleOrderedMap<T> of(String name, T val) {
    return new SimpleOrderedMap<T>(List.of(name, val));
  }

  /**
   * Returns a shared, empty, and immutable instance of {@link SimpleOrderedMap}.
   *
   * @return Empty {@link SimpleOrderedMap} (immutable)
   */
  public static SimpleOrderedMap<Object> of() {
    return EMPTY;
  }
  
  private class InnerMap extends AbstractMap<String, T> {
    @Override
    public Set<Entry<String, T>> entrySet() {
      return SimpleOrderedMap.this.entrySet();
    }
  }
}
