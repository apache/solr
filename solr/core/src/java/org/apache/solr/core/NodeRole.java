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

import com.google.common.collect.ImmutableSet;
import java.util.*;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.util.StrUtils;

public class NodeRole {
  private boolean hasData;
  private Type role;

  public NodeRole(String role) {
    if (StringUtils.isEmpty(role)) {
      hasData = true;
      this.role = Type.data;
      return;
    }
    Set<String> roles = new HashSet<>(StrUtils.split(role, ','));
    if (roles.isEmpty()) {
      hasData = true;
      return;
    }
    for (String s : roles) {
      if (Type.data.name().equals(s)) {
        hasData = true;
        continue;
      }
      if (Type.overseer.name().equals(s)) {
        this.role = Type.overseer;
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

  @SuppressWarnings("unchecked")
  public Map<String, Object> modifyRoleData(Map<String, Object> rolesData, String nodeName) {
    if (this.role == Type.data) {
      //this is a normal data node, nothing to do
      if (rolesData == null) return null;// no role info, return
      if (removeStaleRoles(nodeName, rolesData, Collections.emptySet())) {
        return rolesData;
      } else {
        return null;
      }
    } else {
      if (rolesData == null) rolesData = new HashMap<>();
      List<String> nodes = (List<String>) rolesData.computeIfAbsent(role.name(), s -> new ArrayList<String>());
      boolean isModified = false;
      if (!nodes.contains(nodeName)) {
        nodes.add(nodeName);
        isModified = true;
      }
      List<String> nonReplicaNodes = (List<String>) rolesData.computeIfAbsent(NO_REPLICAS, s -> new ArrayList<String>());
      if (hasData) {
        if(nonReplicaNodes.contains(nodeName)) {
          nonReplicaNodes.remove(nodeName);
          isModified = true;
        }
      } else {
        if(!nonReplicaNodes.contains(nodeName)){
          nonReplicaNodes.add(nodeName);
          isModified = true;
        }
      }
      if (isModified || removeStaleRoles(nodeName, rolesData, ImmutableSet.of(role.name(), NO_REPLICAS))) {
        return rolesData;
      } else {
        return null;
      }
    }

  }

  @SuppressWarnings("unchecked")
  private boolean removeStaleRoles(String nodeName, Map<String, Object> rolesData, Set<String> ignore) {
    boolean[] isModified = new boolean[]{false};
    rolesData.forEach((s, o) -> {
      if (ignore.contains(s)) return;
      if (o instanceof List) {
        List<String> list = (List<String>) o;
        if (list.contains(nodeName)) {
          isModified[0] = true;
          list.remove(nodeName);
        }
      }
    });
    return isModified[0];
  }

  public enum Type {
    data, overseer;
  }

  public static final String NO_REPLICAS = "no-replicas";

}
