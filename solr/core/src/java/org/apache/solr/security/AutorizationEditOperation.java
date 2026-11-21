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

package org.apache.solr.security;

import static org.apache.solr.common.util.Utils.getDeepCopy;
import static org.apache.solr.handler.admin.SecurityConfHandler.getListValue;
import static org.apache.solr.handler.admin.SecurityConfHandler.getMapValue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.Utils;

// TODO is this a typo - should be Authorization?
enum AutorizationEditOperation {
  SET_USER_ROLE("set-user-role") {
    @Override
    public Map<String, Object> edit(Map<String, Object> latestConf, CommandOperation op) {
      Map<String, Object> roleMap = getMapValue(latestConf, "user-role");
      Map<String, Object> map = op.getDataMap();
      if (op.hasError()) return null;
      for (Map.Entry<String, Object> e : map.entrySet()) {
        if (e.getValue() == null) {
          roleMap.remove(e.getKey());
          continue;
        }
        if (e.getValue() instanceof String || e.getValue() instanceof List) {
          roleMap.put(e.getKey(), e.getValue());
        } else {
          op.addError("Unexpected value ");
          return null;
        }
      }
      return latestConf;
    }
  },
  SET_PERMISSION("set-permission") {
    @Override
    @SuppressWarnings({"unchecked"})
    public Map<String, Object> edit(Map<String, Object> latestConf, CommandOperation op) {
      Integer index = op.getInt(INDEX, null);
      Integer beforeIdx = op.getInt(BEFORE, null);
      Map<String, Object> dataMap = op.getDataMap();
      if (op.hasError()) return null;
      dataMap = getDeepCopy(dataMap, 3);
      dataMap.remove(BEFORE);
      if (beforeIdx != null && index != null) {
        op.addError("Cannot use 'index' and 'before together ");
        return null;
      }
      Object clientId = dataMap.remove(ID);
      if (clientId != null && index == null) {
        op.addError("'id' is assigned by Solr");
        return null;
      }

      for (String key : dataMap.keySet()) {
        if (!Permission.knownKeys.contains(key)) op.addError("Unknown key, " + key);
      }
      try {
        Permission.load(dataMap);
      } catch (Exception e) {
        op.addError(e.getMessage());
        return null;
      }
      if (op.hasError()) return null;

      List<Map<String, Object>> permissions =
          AutorizationEditOperation.ensureIndexOnPermissions(latestConf);
      List<Map<String, Object>> permissionsCopy = new ArrayList<>();
      boolean beforeSatisfied = beforeIdx == null;
      boolean indexSatisfied = index == null;
      for (Map<String, Object> perm : permissions) {
        Integer thisIdx = getIndex(perm);
        if (thisIdx.equals(beforeIdx)) {
          beforeSatisfied = true;
          permissionsCopy.add(dataMap);
          permissionsCopy.add(perm);
        } else if (thisIdx.equals(index)) {
          // overwriting an existing one
          if (clientId != null && !clientId.equals(perm.get(ID))) {
            op.addError(ID_INDEX_MISMATCH);
            return null;
          }
          indexSatisfied = true;
          dataMap.put(ID, perm.get(ID));
          permissionsCopy.add(dataMap);
        } else {
          permissionsCopy.add(perm);
        }
      }

      if (!beforeSatisfied) {
        op.addError("Invalid 'before' :" + beforeIdx);
        return null;
      }
      if (!indexSatisfied) {
        op.addError("Invalid 'index' :" + index);
        return null;
      }

      // just add the new permission at the end if not already added via index / before
      if (!permissionsCopy.contains(dataMap)) {
        permissionsCopy.add(dataMap);
      }
      dataMap.putIfAbsent(ID, UUID.randomUUID().toString());

      setIndex(permissionsCopy);

      latestConf.put(PERMISSIONS, permissionsCopy);
      return latestConf;
    }
  },
  UPDATE_PERMISSION("update-permission") {
    @Override
    public Map<String, Object> edit(Map<String, Object> latestConf, CommandOperation op) {
      Integer index = op.getInt(INDEX, null);
      String id = op.getStr(ID, null);
      if (op.hasError()) return null;
      if (index == null && id == null) {
        op.addError("Use 'index' or 'id'");
        return null;
      }

      List<Map<String, Object>> permissions =
          AutorizationEditOperation.ensureIndexOnPermissions(latestConf);
      for (Map<String, Object> permission : permissions) {
        if (id == null ? index.equals(getIndex(permission)) : id.equals(permission.get(ID))) {
          if (index != null && !index.equals(getIndex(permission))) {
            op.addError(ID_INDEX_MISMATCH);
            return null;
          }
          LinkedHashMap<String, Object> copy = new LinkedHashMap<>(permission);
          copy.putAll(op.getDataMap());
          copy.remove(ID);
          op.setCommandData(copy);
          latestConf.put(PERMISSIONS, permissions);
          return SET_PERMISSION.edit(latestConf, op);
        }
      }
      op.addError(id == null ? "No such permission " + name : "No such permission id: " + id);
      return null;
    }
  },
  DELETE_PERMISSION("delete-permission") {
    @Override
    public Map<String, Object> edit(Map<String, Object> latestConf, CommandOperation op) {
      boolean byId = op.getCommandData() instanceof Map;
      Object key = byId ? op.getStr(ID) : op.getInt("");
      if (op.hasError()) return null;

      // find the permission to delete using the "index"
      List<Map<String, Object>> p = AutorizationEditOperation.ensureIndexOnPermissions(latestConf);
      Predicate<Map<String, Object>> match =
          byId ? map -> key.equals(map.get(ID)) : map -> key.equals(getIndex(map));
      if (p.stream().anyMatch(match)) {
        // filter out the deleted index, then re-order the remaining indexes
        List<Map<String, Object>> c =
            p.stream().filter(match.negate()).collect(Collectors.toList());
        setIndex(c);
        latestConf.put(PERMISSIONS, c);
        return latestConf;
      } else {
        op.addError(byId ? "No such permission id: " + key : "No such index: " + key);
        return null;
      }
    }
  };

  private static final String PERMISSIONS = "permissions";
  private static final String INDEX = "index";
  private static final String ID = "id";
  private static final String BEFORE = "before";
  private static final String ID_INDEX_MISMATCH = "'id' and 'index' name different permissions";

  public final String name;

  AutorizationEditOperation(String s) {
    this.name = s;
  }

  public static AutorizationEditOperation get(String name) {
    for (AutorizationEditOperation o : values()) if (o.name.equals(name)) return o;
    return null;
  }

  static void setIndex(List<Map<String, Object>> permissionsCopy) {
    final AtomicInteger counter = new AtomicInteger(0);
    permissionsCopy.forEach(map -> map.put(INDEX, counter.incrementAndGet()));
  }

  static Integer getIndex(final Map<String, Object> permission) {
    final Object idx = permission.get(INDEX);
    return (idx instanceof Number) ? ((Number) idx).intValue() : -1;
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> ensureIndexOnPermissions(
      Map<String, Object> latestConf) {
    List<Map<String, Object>> permissions =
        (List<Map<String, Object>>) getListValue(latestConf, PERMISSIONS);
    Set<Integer> indexes = new HashSet<>();
    if (!permissions.stream()
        .allMatch(map -> map.get(INDEX) instanceof Number && indexes.add(getIndex(map)))) {
      setIndex(permissions);
    }
    ensureIds(permissions);
    return permissions;
  }

  // Derived from position and content, so every node backfills the same ids for one config.
  private static void ensureIds(List<Map<String, Object>> permissions) {
    Set<Object> seen = new HashSet<>();
    for (int i = 0; i < permissions.size(); i++) {
      Map<String, Object> perm = permissions.get(i);
      Object id = perm.get(ID);
      if (id instanceof String && seen.add(id)) continue;
      Map<String, Object> content = new TreeMap<>(perm);
      content.remove(ID);
      content.remove(INDEX);
      String seed = i + ":" + Utils.toJSONString(content);
      String newId = UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8)).toString();
      perm.put(ID, newId);
      seen.add(newId);
    }
  }

  public abstract Map<String, Object> edit(Map<String, Object> latestConf, CommandOperation op);

  public String getOperationName() {
    return name;
  }
}
