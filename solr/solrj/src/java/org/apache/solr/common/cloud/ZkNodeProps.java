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
package org.apache.solr.common.cloud;

import java.io.IOException;
import java.util.*;

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMaps;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.Utils;
import org.noggit.JSONWriter;

import static org.apache.solr.common.util.Utils.toJSONString;

/**
 * ZkNodeProps contains generic immutable properties.
 */
public class ZkNodeProps implements JSONWriter.Writable {

  protected final Object2ObjectMap<String,Object> propMap;

  /**
   * Construct ZKNodeProps from map.
   */
  public ZkNodeProps(Object2ObjectMap<String,Object> propMap) {
    this.propMap = propMap;
    // TODO: store an unmodifiable map, but in a way that guarantees not to wrap more than once.
    // Always wrapping introduces a memory leak.
  }

  public ZkNodeProps plus(String key , Object val) {
    return plus(Collections.singletonMap(key,val));
  }

  public ZkNodeProps plus(Map<String, Object> newVals) {
    Object2ObjectMap<String, Object> copy = new Object2ObjectLinkedOpenHashMap<>(propMap, 0.25f);
    if (newVals == null || newVals.isEmpty()) return new ZkNodeProps(copy);
    copy.putAll(newVals);
    return new ZkNodeProps(copy);
  }

  public ZkNodeProps minus(String... minusKeys) {
    ZkNodeProps props = new ZkNodeProps(propMap);
    Arrays.asList(minusKeys).forEach(props.keySet()::remove);
    return props;
  }

  /**
   * Constructor that populates the from array of Strings in form key1, value1,
   * key2, value2, ..., keyN, valueN
   */
  public ZkNodeProps(String... keyVals) {
    this( Utils.makeMap((Object[]) keyVals) );
  }

  public static ZkNodeProps fromKeyVals(Object... keyVals)  {
    return new ZkNodeProps( Utils.makeMap(keyVals) );
  }


  /**
   * Get property keys.
   */
  public Set<String> keySet() {
    return propMap.keySet();
  }

  /**
   * Get all properties as map.
   */
  public Object2ObjectMap<String, Object> getProperties() {
    return propMap;
  }

  /** Returns a shallow writable copy of the properties */
  public Object2ObjectMap<String,Object> shallowCopy() {
    return new Object2ObjectLinkedOpenHashMap<>(propMap, 0.25f);
  }

  /**
   * Create Replica from json string that is typically stored in zookeeper.
   */
  public static ZkNodeProps load(byte[] bytes) {
    Object2ObjectMap<String, Object> props = null;
    // Detect javabin by its leading version byte. The fork bumped JavaBinCodec.VERSION (2 -> 3), so this
    // must compare against the codec's actual VERSION rather than a hardcoded literal; JSON payloads start
    // with '{' or '[' and never collide with the version byte.
    if (bytes[0] == JavaBinCodec.VERSION) {
      try (JavaBinCodec jbc = new JavaBinCodec()) {
        props = (Object2ObjectMap<String, Object>) jbc.unmarshal(bytes);
      } catch (IOException e) {
        throw new RuntimeException("Unable to parse javabin content");
      }
    } else {
      props = (Object2ObjectMap<String, Object>) new Object2ObjectLinkedOpenHashMap((Map)Utils.fromJSON(bytes), 0.5f);
    }
    return new ZkNodeProps(props);
  }

  @Override
  public void write(JSONWriter jsonWriter) {
    jsonWriter.startObject();
    int sz = propMap.size();
    boolean first = true;
    for (Map.Entry<?, ?> entry : Object2ObjectMaps.fastIterable(propMap)) {
      if (first) {
        first = false;
      } else {
        jsonWriter.writeValueSeparator();
      }
      if (sz > 1)  jsonWriter.indent();
      if (entry.getKey() != null)  jsonWriter.writeString(entry.getKey().toString());
      jsonWriter.writeNameSeparator();
      jsonWriter.write(entry.getValue());
    }
    jsonWriter.endObject();
  }
  
  /**
   * Get a string property value.
   */
  public String getStr(String key) {
    Object o = propMap.get(key);
    return o == null ? null : o.toString();
  }

  /**
   * Get a string property value.
   */
  public Integer getInt(String key, Integer def) {
    Object o = propMap.get(key);
    return o == null ? def : Integer.valueOf(o.toString());
  }

  /**
   * Get a string property value.
   */
  public String getStr(String key,String def) {
    Object o = propMap.get(key);
    return o == null ? def : o.toString();
  }

  public Object get(String key) {
    return propMap.get(key);
  }

  @Override
  public String toString() {
    return toJSONString(this);
    /***
    StringBuilder sb = new StringBuilder();
    Set<Entry<String,Object>> entries = propMap.entrySet();
    for(Entry<String,Object> entry : entries) {
      sb.append(entry.getKey() + "=" + entry.getValue() + "\n");
    }
    return sb.toString();
    ***/
  }

  /**
   * Check if property key exists.
   */
  public boolean containsKey(String key) {
    return propMap.containsKey(key);
  }

  public boolean getBool(String key, boolean b) {
    Object o = propMap.get(key);
    if (o == null) return b;
    if (o instanceof Boolean) return (boolean) o;
    return Boolean.parseBoolean(o.toString());
  }


  public int size() {
    return propMap.size();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ZkNodeProps that = (ZkNodeProps) o;
    return propMap.equals(that.propMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(propMap);
  }
}
