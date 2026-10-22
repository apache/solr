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
package org.apache.solr.handler.admin;

import static org.apache.solr.handler.admin.SecurityConfHandler.SecurityConfig;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.SolrErrorWrappingException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.BasicAuthPlugin;
import org.apache.solr.security.RuleBasedAuthorizationPlugin;

public class SecurityConfHandlerTest extends SolrTestCaseJ4 {

  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testEdit() throws Exception {
    MockSecurityHandler handler = new MockSecurityHandler();
    String command =
        "{\n"
            + "'set-user': {'tom':'TomIsCool'},\n"
            + "'set-user':{ 'tom':'TomIsUberCool'}\n"
            + "}";
    SolrQueryRequestBase req = new SolrQueryRequestBase(null, new ModifiableSolrParams());
    req.getContext().put("httpMethod", SolrRequest.METHOD.POST);
    req.getContext().put("path", "/admin/authentication");
    ContentStreamBase.ByteArrayStream o =
        new ContentStreamBase.ByteArrayStream(command.getBytes(StandardCharsets.UTF_8), "");
    req.setContentStreams(List.of(o));
    handler.handleRequestBody(req, new SolrQueryResponse());

    try (BasicAuthPlugin basicAuth = new BasicAuthPlugin()) {
      SecurityConfig securityCfg = handler.m.get("/security.json");
      basicAuth.init((Map<String, Object>) securityCfg.getData().get("authentication"));
      assertTrue(basicAuth.authenticate("tom", "TomIsUberCool"));

      command = "{\n" + "'set-user': {'harry':'HarryIsCool'},\n" + "'delete-user': ['tom']\n" + "}";
      o = new ContentStreamBase.ByteArrayStream(command.getBytes(StandardCharsets.UTF_8), "");
      req.setContentStreams(List.of(o));
      handler.handleRequestBody(req, new SolrQueryResponse());
      securityCfg = handler.m.get("/security.json");
      assertEquals(3, securityCfg.getVersion());
      Map result = (Map) securityCfg.getData().get("authentication");
      result = (Map) result.get("credentials");
      assertEquals(1, result.size());
    }

    command =
        "{'set-permission':{ collection : acoll ,\n"
            + "                      path : '/nonexistentpath',\n"
            + "                      role :guest },\n"
            + "'set-user-role': { 'tom': ['admin','dev']},"
            + "'set-permission':{'name': 'security-edit',\n"
            + "                  'role': 'admin'}\n"
            + "}";

    req = new SolrQueryRequestBase(null, new ModifiableSolrParams());
    req.getContext().put("httpMethod", SolrRequest.METHOD.POST);
    req.getContext().put("path", "/admin/authorization");
    o = new ContentStreamBase.ByteArrayStream(command.getBytes(StandardCharsets.UTF_8), "");
    req.setContentStreams(List.of(o));
    SolrQueryResponse rsp = new SolrQueryResponse();
    handler.handleRequestBody(req, rsp);
    assertNull(rsp.getValues().get(CommandOperation.ERR_MSGS));
    Map authzconf = (Map) handler.m.get("/security.json").getData().get("authorization");
    Map userRoles = (Map) authzconf.get("user-role");
    List tomRoles = (List) userRoles.get("tom");
    assertTrue(tomRoles.contains("admin"));
    assertTrue(tomRoles.contains("dev"));
    List<Map> permissions = (List<Map>) authzconf.get("permissions");
    assertEquals(2, permissions.size());
    for (Map p : permissions) {
      assertEquals("acoll", p.get("collection"));
      break;
    }
    command =
        "{\n"
            + "'set-permission':{index : 2,  name : security-edit,\n"
            + "                  'role': ['admin','dev']\n"
            + "                  }}";
    req = new SolrQueryRequestBase(null, new ModifiableSolrParams());
    req.getContext().put("httpMethod", SolrRequest.METHOD.POST);
    req.getContext().put("path", "/admin/authorization");
    o = new ContentStreamBase.ByteArrayStream(command.getBytes(StandardCharsets.UTF_8), "");
    req.setContentStreams(List.of(o));
    rsp = new SolrQueryResponse();
    handler.handleRequestBody(req, rsp);
    authzconf = (Map) handler.m.get("/security.json").getData().get("authorization");
    permissions = (List<Map>) authzconf.get("permissions");

    Map p = permissions.get(1);
    assertEquals("security-edit", p.get("name"));
    List rol = (List) p.get("role");
    assertEquals("admin", rol.get(0));
    assertEquals("dev", rol.get(1));

    command =
        "{\n"
            + "'update-permission':{'index': 1,\n"
            + "                  'role': ['guest','admin']\n"
            + "                  }}";
    req = new SolrQueryRequestBase(null, new ModifiableSolrParams());
    req.getContext().put("httpMethod", SolrRequest.METHOD.POST);
    req.getContext().put("path", "/admin/authorization");
    o = new ContentStreamBase.ByteArrayStream(command.getBytes(StandardCharsets.UTF_8), "");
    req.setContentStreams(List.of(o));
    rsp = new SolrQueryResponse();
    handler.handleRequestBody(req, rsp);
    authzconf = (Map) handler.m.get("/security.json").getData().get("authorization");
    permissions = (List<Map>) authzconf.get("permissions");

    p = permissions.get(0);
    assertEquals("acoll", p.get("collection"));
    rol = (List) p.get("role");
    assertEquals("guest", rol.get(0));
    assertEquals("admin", rol.get(1));

    command = "{\n" + "delete-permission: 1,\n" + " set-user-role : { tom :null}\n" + "}";
    req = new SolrQueryRequestBase(null, new ModifiableSolrParams());
    req.getContext().put("httpMethod", SolrRequest.METHOD.POST);
    req.getContext().put("path", "/admin/authorization");
    o = new ContentStreamBase.ByteArrayStream(command.getBytes(StandardCharsets.UTF_8), "");
    req.setContentStreams(List.of(o));
    rsp = new SolrQueryResponse();
    handler.handleRequestBody(req, rsp);
    assertNull(rsp.getValues().get(CommandOperation.ERR_MSGS));
    authzconf = (Map) handler.m.get("/security.json").getData().get("authorization");
    userRoles = (Map) authzconf.get("user-role");
    assertEquals(0, userRoles.size());
    permissions = (List<Map>) authzconf.get("permissions");
    assertEquals(1, permissions.size());

    for (Map permission : permissions) {
      assertNotEquals("some-permission", permission.get("name"));
    }
    // -ve test security edit is a well-known permission, only role attribute should be provided
    command =
        "{\n"
            + "'set-permission':{index : 2,  'name': 'security-edit',\n"
            + "                  'method':'POST',"
            + "                  'role': 'admin'\n"
            + "                  }}";
    final SolrQueryRequestBase badReq = new SolrQueryRequestBase(null, new ModifiableSolrParams());
    badReq.getContext().put("httpMethod", SolrRequest.METHOD.POST);
    badReq.getContext().put("path", "/admin/authorization");
    o = new ContentStreamBase.ByteArrayStream(command.getBytes(StandardCharsets.UTF_8), "");
    badReq.setContentStreams(List.of(o));
    final SolrQueryResponse badRsp = new SolrQueryResponse();
    SolrErrorWrappingException ex =
        assertThrows(
            SolrErrorWrappingException.class, () -> handler.handleRequestBody(badReq, badRsp));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().contains("method is not a valid key for the permission"));
    handler.close();
  }

  /**
   * Two overlapping writes of security.json must never leave a torn file behind: the document on
   * disk afterwards is always exactly one of the written documents, never the beginning of one
   * followed by the tail of a longer one (the corruption shape in SOLR-18010). The first write is
   * held open inside serialization while the second write runs, which is the interleave that
   * produced that shape.
   */
  public void testConcurrentPersistConfLeavesOneWholeDocument() throws Exception {
    assumeWorkingMockito();
    Path solrHome = createTempDir("securityConfHandlerLocalPersist");
    Path securityJson = solrHome.resolve("security.json");
    CoreContainer cc = mock(CoreContainer.class);
    when(cc.getSolrHome()).thenReturn(solrHome);
    SecurityConfHandlerLocalForTesting handler = new SecurityConfHandlerLocalForTesting(cc);

    byte[] shortBytes = Utils.toJSON(shortSecurityData("x"));
    byte[] longBytes = Utils.toJSON(longSecurityData());
    Files.write(securityJson, Utils.toJSON(shortSecurityData("seed")));

    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicReference<Throwable> firstErr = new AtomicReference<>();
    Thread first =
        new Thread(
            () -> {
              try {
                handler.persistConf(
                    new SecurityConfig()
                        .setData(shortSecurityData(new LatchingValue("x", entered, release))));
              } catch (Throwable t) {
                firstErr.set(t);
              }
            });
    first.start();
    assertTrue("first write reached serialization", entered.await(60, TimeUnit.SECONDS));
    handler.persistConf(new SecurityConfig().setData(longSecurityData()));
    release.countDown();
    first.join(TimeUnit.SECONDS.toMillis(60));
    assertFalse("first write still running", first.isAlive());
    assertNull("first write failed: " + firstErr.get(), firstErr.get());

    byte[] fileBytes = Files.readAllBytes(securityJson);
    assertSingleJsonDocument(fileBytes);
    assertTrue(
        "security.json holds neither written document ("
            + fileBytes.length
            + " bytes; the writes were "
            + shortBytes.length
            + " and "
            + longBytes.length
            + " bytes)",
        Arrays.equals(fileBytes, shortBytes) || Arrays.equals(fileBytes, longBytes));
  }

  /** An edit keeps the POSIX permissions of the existing file. */
  public void testEditKeepsExistingSecurityJsonPermissions() throws Exception {
    assumeWorkingMockito();
    Path solrHome = createTempDir("securityConfHandlerLocalPerms");
    assumeTrue(
        "POSIX file permissions are not supported on this file system",
        solrHome.getFileSystem().supportedFileAttributeViews().contains("posix"));
    Path securityJson = solrHome.resolve("security.json");
    Files.write(securityJson, seedSecurityJson().getBytes(StandardCharsets.UTF_8));
    EnumSet<PosixFilePermission> expected =
        EnumSet.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.GROUP_READ);
    Files.setPosixFilePermissions(securityJson, expected);
    SecurityConfHandlerLocal handler = new SecurityConfHandlerLocal(mockCoreContainer(solrHome));
    postAuthorizationEdit(handler, "{'set-user-role': {'alice': 'admin'}}");
    assertTrue(Files.readString(securityJson, StandardCharsets.UTF_8).contains("alice"));
    assertEquals(expected, Files.getPosixFilePermissions(securityJson));
    assertNoTemporaryFiles(solrHome);
  }

  /** A read-only SOLR_HOME still allows an edit when security.json itself is writable. */
  public void testEditInPlaceWhenSolrHomeIsReadOnly() throws Exception {
    assumeWorkingMockito();
    Path solrHome = createTempDir("securityConfHandlerLocalReadOnly");
    assumeTrue(
        "POSIX file permissions are not supported on this file system",
        solrHome.getFileSystem().supportedFileAttributeViews().contains("posix"));
    Path securityJson = solrHome.resolve("security.json");
    Files.write(securityJson, seedSecurityJson().getBytes(StandardCharsets.UTF_8));
    Files.setPosixFilePermissions(
        solrHome, EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_EXECUTE));
    try {
      assumeFalse("the directory is writable for this user", Files.isWritable(solrHome));
      SecurityConfHandlerLocal handler = new SecurityConfHandlerLocal(mockCoreContainer(solrHome));
      postAuthorizationEdit(handler, "{'set-user-role': {'alice': 'admin'}}");
      assertTrue(Files.readString(securityJson, StandardCharsets.UTF_8).contains("alice"));
    } finally {
      Files.setPosixFilePermissions(
          solrHome,
          EnumSet.of(
              PosixFilePermission.OWNER_READ,
              PosixFilePermission.OWNER_WRITE,
              PosixFilePermission.OWNER_EXECUTE));
    }
  }

  /** A reader of security.json never sees a partial document while edits are written. */
  public void testReaderNeverSeesPartialSecurityJsonDuringEdits() throws Exception {
    assumeWorkingMockito();
    Path solrHome = createTempDir("securityConfHandlerLocalReaders");
    assumeTrue(
        "atomic replacement needs POSIX file attributes",
        solrHome.getFileSystem().supportedFileAttributeViews().contains("posix"));
    Path securityJson = solrHome.resolve("security.json");
    SecurityConfHandlerLocal handler = new SecurityConfHandlerLocal(mockCoreContainer(solrHome));
    Map<String, Object> small = shortSecurityData("x");
    Map<String, Object> large = largeSecurityData();
    byte[] smallBytes = Utils.toJSON(small);
    byte[] largeBytes = Utils.toJSON(large);
    Files.write(securityJson, smallBytes);
    AtomicReference<Throwable> writerErr = new AtomicReference<>();
    AtomicBoolean writing = new AtomicBoolean(true);
    Thread writer =
        new Thread(
            () -> {
              try {
                for (int i = 0; i < 100; i++) {
                  handler.persistConf(new SecurityConfig().setData(i % 2 == 0 ? large : small));
                }
              } catch (Throwable t) {
                writerErr.set(t);
              } finally {
                writing.set(false);
              }
            });
    writer.start();
    try {
      while (writing.get()) {
        byte[] got = Files.readAllBytes(securityJson);
        assertTrue(
            "reader saw a partial security.json of " + got.length + " bytes",
            Arrays.equals(got, smallBytes) || Arrays.equals(got, largeBytes));
      }
    } finally {
      writer.join(TimeUnit.SECONDS.toMillis(60));
    }
    assertNull("writer failed: " + writerErr.get(), writerErr.get());
  }

  private static Map<String, Object> largeSecurityData() {
    Map<String, Object> creds = new LinkedHashMap<>();
    creds.put("large", "L".repeat(1_000_000));
    Map<String, Object> authc = new LinkedHashMap<>();
    authc.put("class", "solr.BasicAuthPlugin");
    authc.put("credentials", creds);
    Map<String, Object> data = new LinkedHashMap<>();
    data.put("authentication", authc);
    return data;
  }

  /** A security.json created by a write gets the mode a newly created file gets. */
  public void testPersistCreatesSecurityJsonWithDefaultMode() throws Exception {
    assumeWorkingMockito();
    Path solrHome = createTempDir("securityConfHandlerLocalCreate");
    assumeTrue(
        "POSIX file permissions are not supported on this file system",
        solrHome.getFileSystem().supportedFileAttributeViews().contains("posix"));
    Path control = solrHome.resolve("control.json");
    Files.write(control, new byte[] {'{', '}'});
    SecurityConfHandlerLocal handler = new SecurityConfHandlerLocal(mockCoreContainer(solrHome));
    assertTrue(handler.persistConf(new SecurityConfig().setData(shortSecurityData("x"))));
    Path securityJson = solrHome.resolve("security.json");
    assertEquals(
        Files.getPosixFilePermissions(control), Files.getPosixFilePermissions(securityJson));
    assertNoTemporaryFiles(solrHome);
  }

  /** Atomic replacement needs a POSIX view and a file that is not a symbolic link. */
  public void testAtomicReplaceNeedsPosixViewAndNoSymbolicLink() {
    assertTrue(
        SecurityConfHandlerLocal.supportsAtomicReplace(
            Set.of("basic", "posix", "unix", "owner"), false));
    assertFalse(
        SecurityConfHandlerLocal.supportsAtomicReplace(
            Set.of("basic", "owner", "dos", "acl", "user"), false));
    assertFalse(
        SecurityConfHandlerLocal.supportsAtomicReplace(Set.of("basic", "posix", "owner"), true));
  }

  private static String seedSecurityJson() {
    return "{\"authentication\":{\"class\":\"solr.BasicAuthPlugin\","
        + "\"credentials\":{\"solr\":\"solr\"}},"
        + "\"authorization\":{\"class\":\"solr.RuleBasedAuthorizationPlugin\",\"user-role\":{}}}";
  }

  private static CoreContainer mockCoreContainer(Path solrHome) {
    CoreContainer cc = mock(CoreContainer.class);
    when(cc.getSolrHome()).thenReturn(solrHome);
    when(cc.getAuthorizationPlugin()).thenReturn(new RuleBasedAuthorizationPlugin());
    return cc;
  }

  private static void assertNoTemporaryFiles(Path solrHome) throws IOException {
    try (Stream<Path> files = Files.list(solrHome)) {
      assertEquals(
          "temporary files left behind",
          List.of(),
          files.filter(f -> f.getFileName().toString().endsWith(".tmp")).toList());
    }
  }

  private static void postAuthorizationEdit(SecurityConfHandler handler, String command)
      throws Exception {
    SolrQueryRequestBase req = new SolrQueryRequestBase(null, new ModifiableSolrParams());
    req.getContext().put("httpMethod", SolrRequest.METHOD.POST);
    req.getContext().put("path", "/admin/authorization");
    req.setContentStreams(
        List.of(
            new ContentStreamBase.ByteArrayStream(command.getBytes(StandardCharsets.UTF_8), "")));
    handler.handleRequestBody(req, new SolrQueryResponse());
  }

  private static void assertSingleJsonDocument(byte[] fileBytes) {
    ObjectMapper mapper =
        JsonMapper.builder().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).build();
    try {
      mapper.readValue(fileBytes, Map.class);
    } catch (Exception e) {
      fail("security.json is not a single JSON document: " + e);
    }
  }

  private static Map<String, Object> shortSecurityData(CharSequence credValue) {
    Map<String, Object> creds = new LinkedHashMap<>();
    creds.put("u", credValue);
    Map<String, Object> authc = new LinkedHashMap<>();
    authc.put("class", "solr.BasicAuthPlugin");
    authc.put("credentials", creds);
    Map<String, Object> data = new LinkedHashMap<>();
    data.put("authentication", authc);
    return data;
  }

  private static Map<String, Object> longSecurityData() {
    StringBuilder pad = new StringBuilder();
    for (int i = 0; i < 6000; i++) {
      pad.append('L');
    }
    Map<String, Object> creds = new LinkedHashMap<>();
    creds.put("longuser", pad.toString());
    Map<String, Object> authc = new LinkedHashMap<>();
    authc.put("class", "solr.BasicAuthPlugin");
    authc.put("credentials", creds);
    Map<String, Object> data = new LinkedHashMap<>();
    data.put("authentication", authc);
    return data;
  }

  /**
   * A {@link CharSequence} whose first read blocks until released. Serialization evaluates it, so a
   * write whose data carries one is held open at a known point inside the write.
   */
  private static final class LatchingValue implements CharSequence {
    private final String delegate;
    private final CountDownLatch entered;
    private final CountDownLatch release;
    private final AtomicBoolean armed = new AtomicBoolean(true);

    LatchingValue(String delegate, CountDownLatch entered, CountDownLatch release) {
      this.delegate = delegate;
      this.entered = entered;
      this.release = release;
    }

    private void gate() {
      if (armed.compareAndSet(true, false)) {
        entered.countDown();
        try {
          release.await(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    @Override
    public int length() {
      gate();
      return delegate.length();
    }

    @Override
    public char charAt(int index) {
      gate();
      return delegate.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
      gate();
      return delegate.subSequence(start, end);
    }

    @Override
    public String toString() {
      gate();
      return delegate;
    }
  }

  public static class MockSecurityHandler extends SecurityConfHandler {
    private Map<String, SecurityConfig> m;
    final BasicAuthPlugin basicAuthPlugin = new BasicAuthPlugin();
    final RuleBasedAuthorizationPlugin rulesBasedAuthorizationPlugin =
        new RuleBasedAuthorizationPlugin();

    public MockSecurityHandler() {
      super(null);
      m = new HashMap<>();
      SecurityConfig sp = new SecurityConfig();
      Map<String, Object> securityData = new HashMap<>();
      securityData.put(
          "authentication", Map.of("class", "solr." + BasicAuthPlugin.class.getSimpleName()));
      securityData.put(
          "authorization",
          Map.of("class", "solr." + RuleBasedAuthorizationPlugin.class.getSimpleName()));
      sp.setVersion(1);
      sp.setData(securityData);
      m.put("/security.json", sp);

      basicAuthPlugin.init(Map.of("credentials", Map.of("ignore", "me")));

      rulesBasedAuthorizationPlugin.init(new HashMap<>());
    }

    @Override
    Object getPlugin(String key) {
      if (key.equals("authentication")) {
        return basicAuthPlugin;
      }
      if (key.equals("authorization")) {
        return rulesBasedAuthorizationPlugin;
      }
      return null;
    }

    @Override
    protected void getConf(SolrQueryResponse rsp, String key) {
      // NOP
    }

    @Override
    public SecurityConfig getSecurityConfig(boolean getFresh) {
      return m.get("/security.json");
    }

    @Override
    protected boolean persistConf(SecurityConfig props) {
      SecurityConfig fromMap = m.get("/security.json");
      if (fromMap.getVersion() == props.getVersion()) {
        props.setVersion(props.getVersion() + 1);
        m.put("/security.json", props);
        return true;
      } else {
        return false;
      }
    }

    public String getStandardJson() throws Exception {
      String command = "{\n" + "'set-user': {'solr':'SolrRocks'}\n" + "}";
      SolrQueryRequestBase req = new SolrQueryRequestBase(null, new ModifiableSolrParams());
      req.getContext().put("httpMethod", SolrRequest.METHOD.POST);
      req.getContext().put("path", "/admin/authentication");
      ContentStreamBase.ByteArrayStream o =
          new ContentStreamBase.ByteArrayStream(command.getBytes(StandardCharsets.UTF_8), "");
      req.setContentStreams(List.of(o));
      handleRequestBody(req, new SolrQueryResponse());

      command =
          "{'set-user-role': { 'solr': 'admin'},\n"
              + "'set-permission':{'name': 'security-edit', 'role': 'admin'}"
              + "}";
      req = new SolrQueryRequestBase(null, new ModifiableSolrParams());
      req.getContext().put("httpMethod", SolrRequest.METHOD.POST);
      req.getContext().put("path", "/admin/authorization");
      o = new ContentStreamBase.ByteArrayStream(command.getBytes(StandardCharsets.UTF_8), "");
      req.setContentStreams(List.of(o));
      SolrQueryResponse rsp = new SolrQueryResponse();
      handleRequestBody(req, rsp);
      Map<String, Object> data = m.get("/security.json").getData();
      ((Map) data.get("authentication")).remove("");
      ((Map) data.get("authorization")).remove("");
      return Utils.toJSONString(data);
    }
  }

  public static void main(String[] args) throws Exception {
    try (MockSecurityHandler msh = new MockSecurityHandler()) {
      System.out.println(msh.getStandardJson());
    }
  }
}
