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
package org.apache.solr.util;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.solr.common.util.CollectionUtil;

/**
 * Wraps another map, keeping track of each key that was seen via {@link #get(Object)} or {@link
 * #remove(Object)}.
 */
public class MapListener<K, V> implements Map<K, V> {
  private final Map<K, V> target;
  private final Set<K> seenKeys;

  public MapListener(Map<K, V> target) {
    this.target = target;
    seenKeys = CollectionUtil.newHashSet(target.size());
  }

  public Set<K> getSeenKeys() {
    return seenKeys;
  }

  @Override
  public int size() {
    return target.size();
  }

  @Override
  public boolean isEmpty() {
    return target.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return target.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return target.containsValue(value);
  }

  @Override
  @SuppressWarnings("unchecked")
  public V get(Object key) {
    seenKeys.add((K) key);
    return target.get(key);
  }

  @Override
  public V put(K key, V value) {
    return target.put(key, value);
  }

  @Override
  @SuppressWarnings("unchecked")
  public V remove(Object key) {
    seenKeys.add((K) key);
    return target.remove(key);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    target.putAll(m);
  }

  @Override
  public void clear() {
    target.clear();
  }

  @Override
  public Set<K> keySet() {
    return target.keySet();
  }

  @Override
  public Collection<V> values() {
    return target.values();
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return target.entrySet();
  }
}
