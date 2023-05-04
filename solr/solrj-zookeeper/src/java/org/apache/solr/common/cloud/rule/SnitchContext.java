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
package org.apache.solr.common.cloud.rule;

import java.util.HashMap;
import java.util.Map;
import org.apache.zookeeper.KeeperException;

/**
 * This is the context provided to the snitches to interact with the system. This is a
 * per-node-per-snitch instance.
 */
public abstract class SnitchContext {
  private final Map<String, Object> tags = new HashMap<>();
  private String node;
  private Map<String, Object> session;

  public SnitchContext(String node, Map<String, Object> session) {
    this.node = node;
    this.session = session;
  }

  public Map<String, Object> getTags() {
    return tags;
  }

  public void store(String s, Object val) {
    if (session != null) session.put(s, val);
  }

  public Object retrieve(String s) {
    return session != null ? session.get(s) : null;
  }

  public abstract Map<?, ?> getZkJson(String path) throws KeeperException, InterruptedException;

  public String getNode() {
    return node;
  }
}
