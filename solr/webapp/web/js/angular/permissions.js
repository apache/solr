/*
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

const permissions = {
  COLL_EDIT_PERM: "collection-admin-edit",
  COLL_READ_PERM: "collection-admin-read",
  CORE_READ_PERM: "core-admin-read",
  CORE_EDIT_PERM: "core-admin-edit",
  ZK_READ_PERM: "zk-read",
  READ_PERM: "read",
  UPDATE_PERM: "update",
  CONFIG_EDIT_PERM: "config-edit",
  CONFIG_READ_PERM: "config-read",
  SCHEMA_READ_PERM: "schema-read",
  SCHEMA_EDIT_PERM: "schema-edit",
  SECURITY_EDIT_PERM: "security-edit",
  SECURITY_READ_PERM: "security-read",
  METRICS_READ_PERM: "metrics-read",
  FILESTORE_READ_PERM: "filestore-read",
  FILESTORE_WRITE_PERM: "filestore-write",
  PACKAGE_EDIT_PERM: "package-edit",
  PACKAGE_READ_PERM: "package-read",
  ALL_PERM: "all"
}

/**
 * Returns true if all required permissions are available. Also returns true if RBAC is not enabled
 * @param requiredPermissions the permission(s) to check for, can be a single or array
 * @param userPermissions the actual permissions of current user
 * @returns {boolean}
 */
var hasAllRequiredPermissions = function (requiredPermissions, userPermissions) {
  if (!Array.isArray(requiredPermissions)) {
    requiredPermissions = [requiredPermissions];
  }
  if (userPermissions !== undefined) {
    return requiredPermissions.every(elem => userPermissions.indexOf(elem) > -1);
  } else {
    // RBAC not enabled, always return true
    return true;
  }
}