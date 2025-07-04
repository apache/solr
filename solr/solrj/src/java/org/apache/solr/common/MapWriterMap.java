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

package org.apache.solr.common;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Deprecated // see NavigableMap.wrap.  May keep but use package scope.
public class MapWriterMap implements MapWriter {
  private final Map<String, Object> delegate;

  public MapWriterMap(Map<String, Object> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    delegate.forEach(ew.getBiConsumer());
  }

  @Override
  public Object _get(String path) {
    if (path.indexOf('/') == -1) return delegate.get(path);
    return MapWriter.super._get(path);
  }

  @Override
  public Object _get(List<String> path, Object def) {
    if (path.size() == 1) return delegate.getOrDefault(path.get(0), def);
    return MapWriter.super._get(path, def);
  }

  @Override
  public int _size() {
    return delegate.size();
  }

  @Override
  public Map<String, Object> toMap(Map<String, Object> map) {
    return delegate;
  }
}
