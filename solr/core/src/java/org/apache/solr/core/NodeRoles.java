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

import java.lang.invoke.MethodHandles;
import java.util.*;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.util.StrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeRoles implements MapWriter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String NODE_ROLES_PROP = "solr.node.roles";

  public static final String ON =  "on";
  public static final String OFF =  "off";
  public static final String ALLOWED =  "allowed";
  public static final String DISALLOWED =  "disallowed";
  public static final String PREFERRED =  "preferred";
  public static final Set<String> OVERSEER_MODES = Set.of(ALLOWED, PREFERRED, DISALLOWED);
  public static final Set<String> ON_OFF = Set.of(ON,OFF);

  public static final String DEFAULT_ROLES_STRING = "data:on,overseer:allowed";

  // Map of roles to mode that are applicable for this node.
  private Map<Role, String> nodeRoles;

  public NodeRoles(String rolesString) {
    Map<Role, String> roles = new EnumMap<>(Role.class);
    if (StringUtils.isEmpty(rolesString)) {
     rolesString = DEFAULT_ROLES_STRING;
    }
    List<String> rolesList = StrUtils.splitSmart(rolesString, ',');
    for (String s: rolesList) {
      List<String> roleMode =  StrUtils.splitSmart(s,':');
      Role r = Role.getRole(roleMode.get(0));
      if (r.supportedModes().contains(roleMode.get(1))) {
        roles.put(r, roleMode.get(1));
      } else {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown role mode: " + roleMode.get(0));
      }
    }
    for(Role r: Role.values()) {
      if (!roles.containsKey(r)) {
        roles.put(r, r.defaultIfAbsent());
      }
    }
    nodeRoles = Collections.unmodifiableMap(roles);
  }

  public Map<Role, String> getRoles() {
    return nodeRoles;
  }

  public String getRoleMode(Role role) {
    return nodeRoles.get(role);
  }

  @Override
  public void writeMap(EntryWriter ew) {
    nodeRoles.forEach((role, s) -> ew.putNoEx(role.roleName, s));
  }

  public boolean isOverseerAllowed() {
    String roleMode = nodeRoles.get(Role.OVERSEER);
    return ALLOWED.equals(roleMode) || PREFERRED.equals(roleMode);
  }

  public enum Role {
    DATA("data"),
    OVERSEER("overseer") {
      @Override
      public Set<String> supportedModes() {
        return OVERSEER_MODES;
      }
      @Override
      public String defaultIfAbsent() {
        return DISALLOWED;
      }
    };

    public final String roleName;

    Role(String name) {
      this.roleName = name;
    }

    public static Role getRole(String value) {
      for (Role role: Role.values()) {
        if (value.equals(role.roleName)) return role;
      }
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown role: " + value);
    }

    public Set<String> supportedModes() {
      return ON_OFF;
    }

    /**
     * Default mode for a role in nodes where this role is not specified.
     */
    public String defaultIfAbsent() {
      return OFF;
    }

    @Override
    public String toString() {
      return super.toString().toLowerCase();
    }
  }
}
