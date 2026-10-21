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
package org.apache.solr.handler;

import java.security.Principal;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider.Name;
import org.junit.Test;

/**
 * Unit coverage for {@link ReplicationHandler#getPermissionName(AuthorizationContext)}:
 * state-changing commands map to {@link Name#UPDATE_PERM}; read-only commands stay on {@link
 * Name#READ_PERM}.
 */
public class ReplicationHandlerPermissionNameTest extends SolrTestCaseJ4 {

  private static Name permFor(ReplicationHandler handler, String command) {
    return handler.getPermissionName(new FixedParamsAuthorizationContext(command));
  }

  @Test
  public void testStateChangingCommandsRequireUpdatePerm() {
    ReplicationHandler handler = new ReplicationHandler();
    assertEquals(Name.UPDATE_PERM, permFor(handler, ReplicationHandler.CMD_BACKUP));
    assertEquals(Name.UPDATE_PERM, permFor(handler, ReplicationHandler.CMD_RESTORE));
    assertEquals(Name.UPDATE_PERM, permFor(handler, ReplicationHandler.CMD_DELETE_BACKUP));
    assertEquals(Name.UPDATE_PERM, permFor(handler, ReplicationHandler.CMD_DISABLE_REPL));
    assertEquals(Name.UPDATE_PERM, permFor(handler, ReplicationHandler.CMD_ENABLE_REPL));
    assertEquals(Name.UPDATE_PERM, permFor(handler, ReplicationHandler.CMD_ABORT_FETCH));
    assertEquals(Name.UPDATE_PERM, permFor(handler, ReplicationHandler.CMD_DISABLE_POLL));
    assertEquals(Name.UPDATE_PERM, permFor(handler, ReplicationHandler.CMD_ENABLE_POLL));
    assertEquals(Name.UPDATE_PERM, permFor(handler, ReplicationHandler.CMD_FETCH_INDEX));
  }

  @Test
  public void testStateChangingCommandsCaseInsensitive() {
    // handleRequestBody dispatches commands with equalsIgnoreCase; the mapping matches.
    ReplicationHandler handler = new ReplicationHandler();
    assertEquals(Name.UPDATE_PERM, permFor(handler, "BACKUP"));
    assertEquals(Name.UPDATE_PERM, permFor(handler, "Restore"));
    assertEquals(Name.UPDATE_PERM, permFor(handler, "DeleteBackup"));
    assertEquals(Name.UPDATE_PERM, permFor(handler, "DisableReplication"));
  }

  @Test
  public void testReadOnlyCommandsStayOnReadPerm() {
    ReplicationHandler handler = new ReplicationHandler();
    assertEquals(Name.READ_PERM, permFor(handler, ReplicationHandler.CMD_INDEX_VERSION));
    assertEquals(Name.READ_PERM, permFor(handler, ReplicationHandler.CMD_GET_FILE));
    assertEquals(Name.READ_PERM, permFor(handler, ReplicationHandler.CMD_GET_FILE_LIST));
    assertEquals(Name.READ_PERM, permFor(handler, ReplicationHandler.CMD_DETAILS));
    assertEquals(Name.READ_PERM, permFor(handler, ReplicationHandler.CMD_SHOW_COMMITS));
    assertEquals(Name.READ_PERM, permFor(handler, ReplicationHandler.CMD_RESTORE_STATUS));
  }

  @Test
  public void testUnknownCommandDefaultsToUpdatePerm() {
    // Unknown commands require UPDATE_PERM, so a future state-changing command is not reachable
    // with "read".
    ReplicationHandler handler = new ReplicationHandler();
    assertEquals(Name.UPDATE_PERM, permFor(handler, "someFutureUnknownCommand"));
  }

  @Test
  public void testMissingCommandDefaultsToReadPerm() {
    // No command parameter means handleRequestBody dispatches nothing; stay on READ_PERM.
    ReplicationHandler handler = new ReplicationHandler();
    assertEquals(
        Name.READ_PERM, handler.getPermissionName(new FixedParamsAuthorizationContext(null)));
  }

  @Test
  public void testNullParamsDefaultsToReadPerm() {
    // HttpSolrCall.AuthorizationContext.getParams() returns null when solrReq is unset; must not
    // NPE.
    ReplicationHandler handler = new ReplicationHandler();
    AuthorizationContext ctx =
        new FixedParamsAuthorizationContext(null) {
          @Override
          public SolrParams getParams() {
            return null;
          }
        };
    assertEquals(Name.READ_PERM, handler.getPermissionName(ctx));
  }

  /** Minimal AuthorizationContext that exposes a single {@code command} param. */
  private static class FixedParamsAuthorizationContext extends AuthorizationContext {
    private final SolrParams params;

    FixedParamsAuthorizationContext(String command) {
      this.params =
          command == null ? SolrParams.of() : new MapSolrParams(Map.of("command", command));
    }

    @Override
    public SolrParams getParams() {
      return params;
    }

    @Override
    public Principal getUserPrincipal() {
      return null;
    }

    @Override
    public String getUserName() {
      return null;
    }

    @Override
    public String getHttpHeader(String header) {
      return null;
    }

    @Override
    public Enumeration<String> getHeaderNames() {
      return null;
    }

    @Override
    public String getRemoteAddr() {
      return null;
    }

    @Override
    public String getRemoteHost() {
      return null;
    }

    @Override
    public List<CollectionRequest> getCollectionRequests() {
      return List.of();
    }

    @Override
    public RequestType getRequestType() {
      return RequestType.UNKNOWN;
    }

    @Override
    public String getResource() {
      return ReplicationHandler.PATH;
    }

    @Override
    public String getHttpMethod() {
      return "GET";
    }

    @Override
    public Object getHandler() {
      return null;
    }
  }
}
