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
package org.apache.solr.core;

import java.io.IOException;
import java.util.*;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.util.StrUtils;

public class NodeRole implements MapWriter {
  private boolean hasData = true;
  private Type role  =Type.data;

  public NodeRole(String role) {
    if (StringUtils.isEmpty(role)) {
      return;
    }
    Set<String> roles = new HashSet<>(StrUtils.split(role, ','));
    if (roles.isEmpty()) return;
    for (String s : roles) {
      if (Type.data.name().equals(s)) {
        hasData = true;
        continue;
      }
      if (Type.overseer.name().equals(s)) {
        this.role = Type.overseer;
        hasData = false;
      } else {
        throw new RuntimeException("Unknown type");
      }
    }

  }

  public boolean hasData() {
    return hasData;
  }

  public Type role() {
    return role;
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    ew.put(HAS_DATA, hasData);
    ew.put("role", role.name());
  }

  public enum Type {
    data, overseer;
  }
  public static final String HAS_DATA = "hasData";
  public static final String NODE_ROLE = "solr.node.role";
}
