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

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.CopyOption;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.lucene.tests.mockfile.FilterFileChannel;
import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.RuleBasedAuthorizationPlugin;

/**
 * Failure paths of the temporary-file write, forced through a file system that wraps the default
 * one. The production code is not changed for the tests.
 */
public class SecurityConfHandlerFailurePathsTest extends SolrTestCase {

  private static final String EDIT = "{'set-user-role': {'alice': 'admin'}}";
  private static final String FAKE_OWNER_NAME = "other-owner";
  private static final String FAKE_GROUP_NAME = "other-group";
  private static final UserPrincipal FAKE_OWNER = () -> FAKE_OWNER_NAME;
  private static final GroupPrincipal FAKE_GROUP = () -> FAKE_GROUP_NAME;

  private enum Fault {
    SYNC_TEMP_FILE,
    SYNC_DIRECTORY,
    MOVE,
    PERMISSION_COPY,
    OWNER_MISMATCH,
    GROUP_COPY,
    GROUP_SET_FAILS
  }

  private FaultyFileSystemProvider faulty;
  private Path solrHome;
  private Path securityJson;

  public void testFailedSyncKeepsExistingSecurityJson() throws Exception {
    assumeWorkingMockito();
    setUpSolrHome("securityConfHandlerFaultsSync");
    byte[] seed = Files.readAllBytes(securityJson);
    faulty.faults.add(Fault.SYNC_TEMP_FILE);
    SecurityConfHandlerLocal handler = new SecurityConfHandlerLocal(mockCoreContainer(solrHome));
    assertThrows(SolrException.class, () -> postAuthorizationEdit(handler, EDIT));
    assertFalse("the file was moved after a failed sync", faulty.events.contains("move"));
    assertArrayEquals(seed, Files.readAllBytes(securityJson));
    assertNoTemporaryFiles(solrHome);
  }

  public void testTemporaryFileIsSyncedBeforeItIsMoved() throws Exception {
    assumeWorkingMockito();
    setUpSolrHome("securityConfHandlerFaultsOrder");
    SecurityConfHandlerLocal handler = new SecurityConfHandlerLocal(mockCoreContainer(solrHome));
    postAuthorizationEdit(handler, EDIT);
    assertEquals(List.of("sync", "move", "dirSync"), faulty.events);
    assertTrue(Files.readString(securityJson, StandardCharsets.UTF_8).contains("alice"));
    assertNoTemporaryFiles(solrHome);
  }

  public void testDirectorySyncFailureAfterMoveDoesNotFailEdit() throws Exception {
    assumeWorkingMockito();
    setUpSolrHome("securityConfHandlerFaultsDirSync");
    faulty.faults.add(Fault.SYNC_DIRECTORY);
    SecurityConfHandlerLocal handler = new SecurityConfHandlerLocal(mockCoreContainer(solrHome));
    postAuthorizationEdit(handler, EDIT);
    assertEquals(List.of("sync", "move", "dirSync"), faulty.events);
    assertTrue(Files.readString(securityJson, StandardCharsets.UTF_8).contains("alice"));
    assertNoTemporaryFiles(solrHome);
  }

  public void testRefusedMoveFallsBackToInPlaceWrite() throws Exception {
    assumeWorkingMockito();
    setUpSolrHome("securityConfHandlerFaultsMove");
    Object fileKey = fileKey();
    faulty.faults.add(Fault.MOVE);
    SecurityConfHandlerLocal handler = new SecurityConfHandlerLocal(mockCoreContainer(solrHome));
    postAuthorizationEdit(handler, EDIT);
    assertTrue("the move was not attempted", faulty.events.contains("move"));
    assertTrue(Files.readString(securityJson, StandardCharsets.UTF_8).contains("alice"));
    assertEquals("security.json was replaced, not written in place", fileKey, fileKey());
    assertNoTemporaryFiles(solrHome);
  }

  public void testFailedPermissionCopyKeepsExistingSecurityJson() throws Exception {
    assumeWorkingMockito();
    setUpSolrHome("securityConfHandlerFaultsPerms");
    byte[] seed = Files.readAllBytes(securityJson);
    faulty.faults.add(Fault.PERMISSION_COPY);
    SecurityConfHandlerLocal handler = new SecurityConfHandlerLocal(mockCoreContainer(solrHome));
    assertThrows(SolrException.class, () -> postAuthorizationEdit(handler, EDIT));
    assertFalse(
        "the file was moved after a failed permission copy", faulty.events.contains("move"));
    assertArrayEquals(seed, Files.readAllBytes(securityJson));
    assertNoTemporaryFiles(solrHome);
  }

  public void testOwnerMismatchFallsBackToInPlaceWrite() throws Exception {
    assumeWorkingMockito();
    setUpSolrHome("securityConfHandlerFaultsOwner");
    Object fileKey = fileKey();
    faulty.faults.add(Fault.OWNER_MISMATCH);
    SecurityConfHandlerLocal handler = new SecurityConfHandlerLocal(mockCoreContainer(solrHome));
    postAuthorizationEdit(handler, EDIT);
    assertFalse("the file was replaced for another owner", faulty.events.contains("move"));
    assertTrue(Files.readString(securityJson, StandardCharsets.UTF_8).contains("alice"));
    assertEquals(fileKey, fileKey());
    assertNoTemporaryFiles(solrHome);
  }

  public void testGroupIsCopiedToTemporaryFile() throws Exception {
    assumeWorkingMockito();
    setUpSolrHome("securityConfHandlerFaultsGroup");
    Object fileKey = fileKey();
    faulty.faults.add(Fault.GROUP_COPY);
    SecurityConfHandlerLocal handler = new SecurityConfHandlerLocal(mockCoreContainer(solrHome));
    postAuthorizationEdit(handler, EDIT);
    assertEquals(List.of(FAKE_GROUP_NAME), faulty.groupsSet);
    assertTrue(faulty.events.contains("move"));
    assertNotEquals("security.json was not replaced", fileKey, fileKey());
    assertNoTemporaryFiles(solrHome);
  }

  public void testFailedGroupCopyFallsBackToInPlaceWrite() throws Exception {
    assumeWorkingMockito();
    setUpSolrHome("securityConfHandlerFaultsGroupFail");
    Object fileKey = fileKey();
    faulty.faults.add(Fault.GROUP_COPY);
    faulty.faults.add(Fault.GROUP_SET_FAILS);
    SecurityConfHandlerLocal handler = new SecurityConfHandlerLocal(mockCoreContainer(solrHome));
    postAuthorizationEdit(handler, EDIT);
    assertFalse("the file was replaced after a failed group copy", faulty.events.contains("move"));
    assertTrue(Files.readString(securityJson, StandardCharsets.UTF_8).contains("alice"));
    assertEquals(fileKey, fileKey());
    assertNoTemporaryFiles(solrHome);
  }

  private void setUpSolrHome(String name) throws IOException {
    Path base = createTempDir(name);
    assumeTrue(
        "POSIX file permissions are not supported on this file system",
        base.getFileSystem().supportedFileAttributeViews().contains("posix"));
    faulty = new FaultyFileSystemProvider(base.getFileSystem());
    solrHome = faulty.wrapPath(base);
    securityJson = solrHome.resolve("security.json");
    Files.write(securityJson, seedSecurityJson().getBytes(StandardCharsets.UTF_8));
  }

  private Object fileKey() throws IOException {
    return Files.readAttributes(securityJson, BasicFileAttributes.class).fileKey();
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

  private static boolean isSecurityJson(Path path) {
    return path.getFileName().toString().equals("security.json");
  }

  private static boolean isTemporary(Path path) {
    return path.getFileName().toString().endsWith(".tmp");
  }

  private static final class FaultyFileSystemProvider extends FilterFileSystemProvider {
    final Set<Fault> faults = EnumSet.noneOf(Fault.class);
    final List<String> events = new ArrayList<>();
    final List<String> groupsSet = new ArrayList<>();

    FaultyFileSystemProvider(FileSystem delegate) {
      super("faulty", delegate);
    }

    @Override
    public FileChannel newFileChannel(
        Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs)
        throws IOException {
      FileChannel channel = super.newFileChannel(path, options, attrs);
      boolean temporary = isTemporary(path);
      boolean directory = !temporary && Files.isDirectory(path);
      return new FilterFileChannel(channel) {
        @Override
        public void force(boolean metaData) throws IOException {
          if (temporary) {
            events.add("sync");
            if (faults.contains(Fault.SYNC_TEMP_FILE)) {
              throw new IOException("injected sync failure");
            }
          } else if (directory) {
            events.add("dirSync");
            if (faults.contains(Fault.SYNC_DIRECTORY)) {
              throw new IOException("injected directory sync failure");
            }
          }
          super.force(metaData);
        }
      };
    }

    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {
      events.add("move");
      if (faults.contains(Fault.MOVE) && isSecurityJson(target)) {
        throw new FileSystemException(target.toString(), null, "injected refused move");
      }
      super.move(source, target, options);
    }

    @Override
    public <A extends BasicFileAttributes> A readAttributes(
        Path path, Class<A> type, LinkOption... options) throws IOException {
      A attributes = super.readAttributes(path, type, options);
      if (type == PosixFileAttributes.class
          && faults.contains(Fault.GROUP_COPY)
          && isSecurityJson(path)) {
        return type.cast(new OtherGroupAttributes((PosixFileAttributes) attributes));
      }
      return attributes;
    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(
        Path path, Class<V> type, LinkOption... options) {
      V view = super.getFileAttributeView(path, type, options);
      if (type == PosixFileAttributeView.class) {
        return type.cast(new PosixView((PosixFileAttributeView) view, path));
      }
      if (type == FileOwnerAttributeView.class) {
        return type.cast(new OwnerView((FileOwnerAttributeView) view, path));
      }
      return view;
    }

    private final class PosixView implements PosixFileAttributeView {
      private final PosixFileAttributeView view;
      private final Path path;

      PosixView(PosixFileAttributeView view, Path path) {
        this.view = view;
        this.path = path;
      }

      @Override
      public String name() {
        return view.name();
      }

      @Override
      public PosixFileAttributes readAttributes() throws IOException {
        return view.readAttributes();
      }

      @Override
      public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime)
          throws IOException {
        view.setTimes(lastModifiedTime, lastAccessTime, createTime);
      }

      @Override
      public void setPermissions(Set<PosixFilePermission> perms) throws IOException {
        if (faults.contains(Fault.PERMISSION_COPY) && isTemporary(path)) {
          throw new IOException("injected permission copy failure");
        }
        view.setPermissions(perms);
      }

      @Override
      public void setOwner(UserPrincipal owner) throws IOException {
        view.setOwner(owner);
      }

      @Override
      public UserPrincipal getOwner() throws IOException {
        return view.getOwner();
      }

      @Override
      public void setGroup(GroupPrincipal group) throws IOException {
        if (isTemporary(path)) {
          groupsSet.add(group.getName());
          if (faults.contains(Fault.GROUP_SET_FAILS)) {
            throw new IOException("injected group failure");
          }
        }
        if (Objects.equals(group, FAKE_GROUP)) {
          return;
        }
        view.setGroup(group);
      }
    }

    private final class OwnerView implements FileOwnerAttributeView {
      private final FileOwnerAttributeView view;
      private final Path path;

      OwnerView(FileOwnerAttributeView view, Path path) {
        this.view = view;
        this.path = path;
      }

      @Override
      public String name() {
        return view.name();
      }

      @Override
      public UserPrincipal getOwner() throws IOException {
        if (faults.contains(Fault.OWNER_MISMATCH) && isSecurityJson(path)) {
          return FAKE_OWNER;
        }
        return view.getOwner();
      }

      @Override
      public void setOwner(UserPrincipal owner) throws IOException {
        view.setOwner(owner);
      }
    }
  }

  private static final class OtherGroupAttributes implements PosixFileAttributes {
    private final PosixFileAttributes attributes;

    OtherGroupAttributes(PosixFileAttributes attributes) {
      this.attributes = attributes;
    }

    @Override
    public GroupPrincipal group() {
      return FAKE_GROUP;
    }

    @Override
    public UserPrincipal owner() {
      return attributes.owner();
    }

    @Override
    public Set<PosixFilePermission> permissions() {
      return attributes.permissions();
    }

    @Override
    public FileTime lastModifiedTime() {
      return attributes.lastModifiedTime();
    }

    @Override
    public FileTime lastAccessTime() {
      return attributes.lastAccessTime();
    }

    @Override
    public FileTime creationTime() {
      return attributes.creationTime();
    }

    @Override
    public boolean isRegularFile() {
      return attributes.isRegularFile();
    }

    @Override
    public boolean isDirectory() {
      return attributes.isDirectory();
    }

    @Override
    public boolean isSymbolicLink() {
      return attributes.isSymbolicLink();
    }

    @Override
    public boolean isOther() {
      return attributes.isOther();
    }

    @Override
    public long size() {
      return attributes.size();
    }

    @Override
    public Object fileKey() {
      return attributes.fileKey();
    }
  }
}
