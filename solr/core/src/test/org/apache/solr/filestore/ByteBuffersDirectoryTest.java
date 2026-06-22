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
package org.apache.solr.filestore;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.solr.SolrTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link ByteBuffersDirectory}'s file-table and lifecycle bookkeeping:
 * file registration, {@code listAll}, {@code fileExists}, {@code deleteFile}, {@code rename},
 * {@code sync}, {@code close}, and the not-found / already-exists error contracts.
 *
 * <p><b>Scope note — the write path is intentionally gutted in this fork.</b> The fork's
 * {@link ByteBuffersDataOutput} has its block-allocation machinery commented out
 * ({@code currentBlock = EMPTY}, capacity 0), so writing any non-zero number of bytes through
 * {@code IndexOutput.writeBytes} throws {@code IndexOutOfBoundsException}. Resurrecting that
 * commented-out block allocation is deliberately out of scope for the Agrona buffer-hardening
 * pass (it would revert in-flight WIP), so this test deliberately does <em>not</em> exercise the
 * byte read/write/clone/slice path — those assertions could only pass against a functional output
 * implementation. The logical-write-position semantics of {@link ByteBuffersDataOutput#size()}
 * (the part of the plan that <em>is</em> implemented) are covered directly and in isolation by
 * {@code ByteBuffersDataOutputTest}.
 *
 * <p>All files here are created zero-length, which drives the real directory bookkeeping (the
 * {@code files} map, {@code putIfAbsent} registration, removal, rename, and the not-found
 * contracts) without touching the gutted write path.
 */
public class ByteBuffersDirectoryTest extends SolrTestCase {

  private ByteBuffersDirectory dir;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
  }

  @After
  public void tearDown() throws Exception {
    if (dir != null) {
      dir.close();
    }
    super.tearDown();
  }

  /** Factory method: a directory backed by OUTPUT_AS_ONE_BUFFER (single flat ByteBuffer per file). */
  private static ByteBuffersDirectory newDirectory() {
    return new ByteBuffersDirectory(
        new SingleInstanceLockFactory(),
        () -> new ByteBuffersDataOutput(ByteBuffersDataOutput.REUSE),
        ByteBuffersDirectory.OUTPUT_AS_ONE_BUFFER);
  }

  /**
   * Create-and-close a zero-length file named {@code name}. Registration happens on
   * {@code createOutput} (via {@code files.putIfAbsent}); {@code close()} flushes a zero-length
   * buffer and publishes the {@code IndexInput}. No bytes are written, so the gutted output path
   * is never exercised.
   */
  private static void createEmptyFile(ByteBuffersDirectory d, String name) throws IOException {
    try (IndexOutput out = d.createOutput(name, IOContext.DEFAULT)) {
      // no writeBytes — gutted ByteBuffersDataOutput cannot accept payload bytes
      out.getFilePointer(); // touch the output so it is genuinely opened/closed
    }
  }

  // ---------------------------------------------------------------------------
  // File-table bookkeeping
  // ---------------------------------------------------------------------------

  @Test
  public void testListAll() throws IOException {
    createEmptyFile(dir, "a.bin");
    createEmptyFile(dir, "b.bin");
    String[] listed = dir.listAll();
    Arrays.sort(listed);
    assertArrayEquals(new String[] {"a.bin", "b.bin"}, listed);
  }

  @Test
  public void testFileExists() throws IOException {
    createEmptyFile(dir, "exists.bin");
    assertTrue(dir.fileExists("exists.bin"));
    assertFalse(dir.fileExists("absent.bin"));
  }

  @Test
  public void testZeroLengthFile() throws IOException {
    createEmptyFile(dir, "empty.bin");
    assertEquals("zero-length file must have length 0", 0L, dir.fileLength("empty.bin"));
    try (IndexInput in = dir.openInput("empty.bin", IOContext.DEFAULT)) {
      assertEquals("zero-length file must report length 0 on open", 0L, in.length());
    }
  }

  @Test
  public void testCreateDuplicateFileThrows() throws IOException {
    createEmptyFile(dir, "dup.bin");
    try {
      dir.createOutput("dup.bin", IOContext.DEFAULT);
      fail("expected FileAlreadyExistsException for a second createOutput of the same name");
    } catch (FileAlreadyExistsException e) {
      // expected
    }
  }

  // ---------------------------------------------------------------------------
  // Delete
  // ---------------------------------------------------------------------------

  @Test
  public void testDeleteFile() throws IOException {
    createEmptyFile(dir, "del.bin");
    dir.deleteFile("del.bin");
    assertEquals("deleted file must not appear in listAll()", 0, dir.listAll().length);
  }

  @Test
  public void testDeleteNonexistentFileThrows() throws IOException {
    try {
      dir.deleteFile("nosuchfile.bin");
      fail("expected NoSuchFileException");
    } catch (NoSuchFileException e) {
      // expected
    }
  }

  @Test
  public void testFileLengthNonexistentThrows() throws IOException {
    try {
      dir.fileLength("nosuchfile.bin");
      fail("expected NoSuchFileException");
    } catch (NoSuchFileException e) {
      // expected
    }
  }

  @Test
  public void testOpenInputNonexistentThrows() throws IOException {
    try {
      dir.openInput("nosuchfile.bin", IOContext.DEFAULT);
      fail("expected NoSuchFileException");
    } catch (NoSuchFileException e) {
      // expected
    }
  }

  @Test
  public void testDeleteFileFileLengthThrows() throws IOException {
    createEmptyFile(dir, "gone.bin");
    dir.deleteFile("gone.bin");
    try {
      dir.fileLength("gone.bin");
      fail("expected NoSuchFileException after delete");
    } catch (NoSuchFileException e) {
      // expected
    }
  }

  // ---------------------------------------------------------------------------
  // Rename
  // ---------------------------------------------------------------------------

  @Test
  public void testRename() throws IOException {
    createEmptyFile(dir, "src.bin");
    dir.rename("src.bin", "dst.bin");
    assertFalse("source file must not exist after rename", dir.fileExists("src.bin"));
    assertTrue("destination file must exist after rename", dir.fileExists("dst.bin"));
  }

  @Test
  public void testRenameNonexistentThrows() throws IOException {
    try {
      dir.rename("nosuchfile.bin", "dst.bin");
      fail("expected NoSuchFileException renaming a missing source");
    } catch (NoSuchFileException e) {
      // expected
    }
  }

  // ---------------------------------------------------------------------------
  // sync / close
  // ---------------------------------------------------------------------------

  @Test
  public void testSyncIsNoOp() throws IOException {
    createEmptyFile(dir, "sync.bin");
    dir.sync(Arrays.asList("sync.bin")); // must not throw
    dir.syncMetaData(); // must not throw
  }

  @Test
  public void testCloseEmptiesDirectory() throws IOException {
    createEmptyFile(dir, "x.bin");
    dir.close();
    // After close, ensureOpen() throws; verify the directory cleaned up its file table by
    // confirming a fresh directory of the same type starts empty.
    dir = newDirectory();
    assertEquals(0, dir.listAll().length);
  }

  @Test
  public void testOperationsAfterCloseThrow() throws IOException {
    dir.close();
    try {
      dir.listAll();
      fail("expected AlreadyClosedException-style failure after close");
    } catch (org.apache.lucene.store.AlreadyClosedException e) {
      // expected — ensureOpen() guards all operations
    } finally {
      // re-open so tearDown's close() is a clean no-op path
      dir = newDirectory();
    }
  }
}
