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
import java.lang.invoke.MethodHandles;
import java.util.*;

import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.util.StrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeRole implements IteratorWriter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String NODE_ROLES_PROP = "solr.node.roles";

  private final Set<Role> roles;

  public NodeRole(String role) {
    if (StringUtils.isEmpty(role)) {
      // if no roles were specified, assume "data" role for backcompat reasons
      roles = Set.of(Role.DATA);
      return;
    }
    Set<String> rolesSet = new HashSet<>(StrUtils.split(role, ','));
    if (rolesSet.isEmpty()) {
      // if no roles were specified, assume "data" role for backcompat reasons
      roles = Set.of(Role.DATA);
      return;
    }
    roles = new TreeSet<>();
    for (String r: rolesSet) {
      roles.add(Role.valueOfCaseInsensitive(r));
    }
  }

  public Set<Role> getRoles() {
    return roles;
  }

  @Override
  public void writeIter(ItemWriter iw) throws IOException {
    for (Role role: roles) iw.add(role.toString());
  }

  public enum Role {
    DATA, OVERSEER;

    public static Role valueOfCaseInsensitive(String value) {
      // Given a user string "overseer", convert to OVERSEER and return the enum value
      String canonicalValue = value.toUpperCase().replace(' ', '_');
      return Role.valueOf(canonicalValue);
    }

    @Override
    public String toString() {
      return super.toString().toLowerCase();
    }
  }
}
