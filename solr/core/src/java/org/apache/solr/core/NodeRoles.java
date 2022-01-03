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
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.StrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeRoles {
  public static final String NODE_ROLES_PROP = "solr.node.roles";

  /**
   * Roles to be assumed on nodes that don't have roles specified for them at startup
   */
  public static final String DEFAULT_ROLES_STRING = "data:on,overseer:allowed";

  // Map of roles to mode that are applicable for this node.
  private Map<Role, Mode> nodeRoles;

  public NodeRoles(String rolesString) {
    Map<Role, Mode> roles = new EnumMap<>(Role.class);
    if (StringUtils.isEmpty(rolesString)) {
     rolesString = DEFAULT_ROLES_STRING;
    }
    List<String> rolesList = StrUtils.splitSmart(rolesString, ',');
    for (String s: rolesList) {
      List<String> roleMode =  StrUtils.splitSmart(s,':');
      Role r = Role.getRole(roleMode.get(0));
      Mode m = Mode.valueOf(roleMode.get(1).toUpperCase(Locale.ROOT));
      if (r.supportedModes().contains(m)) {
        roles.put(r, m);
      } else {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "Unknown role mode '" + roleMode.get(1) + "' for role '" + r + "'");
      }
    }
    for(Role r: Role.values()) {
      if (!roles.containsKey(r)) {
        roles.put(r, r.modeWhenRoleIsAbsent());
      }
    }
    nodeRoles = Collections.unmodifiableMap(roles);
  }

  public Map<Role, Mode> getRoles() {
    return nodeRoles;
  }

  public Mode getRoleMode(Role role) {
    return nodeRoles.get(role);
  }

  public boolean isOverseerAllowedOrPreferred() {
    Mode roleMode = nodeRoles.get(Role.OVERSEER);
    return Mode.ALLOWED.equals(roleMode) || Mode.PREFERRED.equals(roleMode);
  }

  public enum Mode {
    ON, OFF, ALLOWED, PREFERRED, DISALLOWED;

    /**
     * Need this lowercasing so that the ZK references use the lowercase form, which is
     * also the form documented in user facing documentation.
     */
    @Override
    public String toString() {
      return name().toLowerCase(Locale.ROOT);
    }
  };

  public enum Role {
    DATA("data") {
      @Override
      public Set<Mode> supportedModes() {
        return Set.of(Mode.ON, Mode.OFF);
      }
      @Override
      public Mode modeWhenRoleIsAbsent() {
        return Mode.OFF;
      }
    },
    OVERSEER("overseer") {
      @Override
      public Set<Mode> supportedModes() {
        return Set.of(Mode.ALLOWED, Mode.PREFERRED, Mode.DISALLOWED);
      }
      @Override
      public Mode modeWhenRoleIsAbsent() {
        return Mode.DISALLOWED;
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

    public abstract Set<Mode> supportedModes();

    /**
     * Default mode for a role in nodes where this role is not specified.
     */
    public abstract Mode modeWhenRoleIsAbsent();

    @Override
    public String toString() {
      return roleName;
    }
  }

  public static String getZNodeForRole(Role role) {
    return ZkStateReader.NODE_ROLES + "/" + role.roleName;
  }

  public static String getZNodeForRoleMode(Role role, Mode mode) {
    return ZkStateReader.NODE_ROLES + "/" + role.roleName + "/" + mode;
  }

}
