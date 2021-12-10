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

  public Map<Role, String> ROLES ;

  public NodeRoles(String role) {
    ROLES = new EnumMap<>(Role.class);
    if (StringUtils.isEmpty(role)) {
     role = DEFAULT_ROLE;
    }
    List<String> rolesList = StrUtils.splitSmart(role, ',');
    for (String s : rolesList) {
      List<String> roleVal =  StrUtils.splitSmart(s,':');
      Role r = Role.getRole(roleVal.get(0));
      if(r.supportedVals().contains(roleVal.get(1))) {
        ROLES.put(r, roleVal.get(1));
      } else {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "UNKNOWN role value :"+roleVal.get(0));
      }
    }
    ROLES = Collections.unmodifiableMap(ROLES);
  }

  public String getRoleVal(Role role) {
    return ROLES.get(role);
  }



  @Override
  public void writeMap(EntryWriter ew) {
    ROLES.forEach((role, s) -> ew.putNoEx(role.roleName, s));
  }

  public enum Role {
    DATA("data") ,
    OVERSEER("overseer") {
      @Override
      public Set<String> supportedVals() {
        return OVERSEER_VALS;
      }
    },
    COORDINATOR("coordinator");

    public final String roleName;

    Role(String name) {
      this.roleName = name;
    }

    public static Role getRole(String value) {
      // Given a user string "overseer", convert to OVERSEER and return the enum value
      for (Role role : Role.values()) {
        if(value.equals(role.roleName)) return role;
      }
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown role : "+value);
    }

    public Set<String> supportedVals() {
      return ON_OFF;

    }

    @Override
    public String toString() {
      return super.toString().toLowerCase();
    }
  }

  public static final String ON =  "on";
  public static final String OFF =  "off";
  public static final Set<String> ON_OFF = Set.of(ON,OFF);
  public static final String ALLOWED =  "allowed";
  public static final String DISALLOWED =  "disallowed";
  public static final String PREFERRED =  "preferred";
  public static final Set<String> OVERSEER_VALS = Set.of(ALLOWED, DISALLOWED, PREFERRED);
  public static final String DEFAULT_ROLE = "data:on,overseer:allowed";
}
