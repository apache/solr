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
package org.apache.solr.zero.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.zero.metadata.ZeroStoreShardMetadata;

/** Serializing/deserializing Json (mostly for {@link ZeroStoreShardMetadata}). */
public class ToFromJson<T> {
  /** Create easier to (human) read but a bit longer json output */
  static final boolean PRETTY_JSON = true;

  private final ObjectMapper objectMapper;

  public ToFromJson() {
    objectMapper = new ObjectMapper();

    if (PRETTY_JSON) {
      objectMapper.writerWithDefaultPrettyPrinter();
    }
  }

  /** Builds an object instance from a String representation of the Json. */
  public T fromJson(String input, Class<T> c) throws Exception {
    return objectMapper.readValue(input, c);
  }

  /** Returns the Json String of the passed object instance. */
  public String toJson(T t) throws Exception {
    return objectMapper.writeValueAsString(t);
  }
}
