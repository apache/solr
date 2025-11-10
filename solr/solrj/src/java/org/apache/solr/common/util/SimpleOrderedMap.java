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

import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.params.SolrParams;

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
 * <p>This class does not provide efficient lookup by key. The lookup performance is only O(N), and
 * not O(1) or O(Log N) as it is for the most common Map-implementations. Its main purpose is to
 * hold data to be serialized. It aims to minimize overhead and to be efficient at adding new
 * elements.
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
   * @param nameValuePairs underlying List which should be used to implement a SimpleOrderedMap;
   *     modifying this List will affect the SimpleOrderedMap.
   */
  private SimpleOrderedMap(List<Object> nameValuePairs) {
    super(nameValuePairs);
  }

  public SimpleOrderedMap(Map.Entry<String, T>[] nameValuePairs) {
    super(nameValuePairs);
  }

  /** Can convert a {@link SolrParams} and other things. */
  public SimpleOrderedMap(MapWriter mapWriter) {
    try {
      mapWriter.writeMap(
          new EntryWriter() {
            @SuppressWarnings("unchecked")
            @Override
            public EntryWriter put(CharSequence k, Object v) throws IOException {
              SimpleOrderedMap.this.add(k.toString(), (T) v);
              return this;
            }
          });
    } catch (IOException e) {
      throw new RuntimeException(e); // impossible?
    }
  }

  @Override
  public SimpleOrderedMap<T> clone() {
    ArrayList<Object> newList = new ArrayList<>(nvPairs.size());
    newList.addAll(nvPairs);
    return new SimpleOrderedMap<>(newList);
  }

  @SuppressWarnings("EqualsDoesntCheckParameterClass")
  @Override
  public boolean equals(Object obj) {
    return obj == this || new InnerMap().equals(obj); // Use AbstractMap's code
  }

  @Override
  public int hashCode() {
    return new InnerMap().hashCode(); // Use AbstractMap's code
  }

  // toString is inherited and implements the Map contract (and this is tested)

  @Override
  public boolean isEmpty() {
    return nvPairs.isEmpty();
  }

  @Override
  public boolean containsKey(final Object key) {
    return this.indexOf((String) key) >= 0;
  }

  @Override
  public boolean containsValue(final Object value) {
    return values().contains(value);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Has linear lookup time O(N)
   *
   * @see NamedList#get(String)
   */
  @Override
  public T get(final Object key) {
    return super.get((String) key);
  }

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

  @Override
  public void putAll(final Map<? extends String, ? extends T> m) {
    if (isEmpty()) {
      m.forEach(this::add);
    } else {
      m.forEach(this::put);
    }
  }

  @Override
  public Set<String> keySet() {
    return new InnerMap().keySet();
  }

  @Override
  public Collection<T> values() {
    return new InnerMap().values();
  }

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
    return new SimpleOrderedMap<>(List.of(name, val));
  }

  /**
   * Returns a shared, empty, and immutable instance of {@link SimpleOrderedMap}.
   *
   * @return Empty {@link SimpleOrderedMap} (immutable)
   */
  public static SimpleOrderedMap<Object> of() {
    return EMPTY;
  }

  /**
   * {@link SimpleOrderedMap} extending {@link NamedList}, we are not able to extend {@link
   * AbstractMap}. With the help of InnerMap we can still use {@link AbstractMap} methods.
   */
  private class InnerMap extends AbstractMap<String, T> {
    @Override
    public Set<Entry<String, T>> entrySet() {
      return SimpleOrderedMap.this.entrySet();
    }
  }
}
