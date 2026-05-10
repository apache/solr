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

package org.apache.solr.util;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.SuppressForbidden;

/**
 * Manual regression test that downloads small binary sample files from Apache Tika's test corpus
 * (Apache License 2.0) and verifies that {@link FileTypeMagicUtil} correctly identifies each MIME
 * type it claims to handle.
 *
 * <p><b>This test is disabled by default.</b> It requires outbound network access and downloads
 * several binary files at runtime. It is intended to be run manually in two situations:
 *
 * <ol>
 *   <li><b>Adding a new format:</b> after extending {@link FileTypeMagicUtil} with a new magic-byte
 *       detector, add a corresponding entry to the URL map below so future runs validate that real
 *       files of that format are detected correctly.
 *   <li><b>Regression validation:</b> when refactoring the detector, run this test to confirm that
 *       real-world binary samples are still detected as expected.
 * </ol>
 *
 * <p>To run this test, temporarily remove the {@code @Ignore} annotation and run:
 *
 * <pre>{@code
 * ./gradlew :solr:core:test \
 *     --tests "org.apache.solr.util.FileTypeMagicUtilTikaCorpusTest"
 * }</pre>
 *
 * <p>Sample sources:
 *
 * <ul>
 *   <li>Apache Tika test corpus (Apache 2.0): <a
 *       href="https://github.com/apache/tika/tree/main/tika-parsers">tika-parsers</a>
 *   <li>XZ Utils test files (public domain): <a
 *       href="https://github.com/tukaani-project/xz/tree/master/tests/files">xz tests</a>
 *   <li>TAR, shell script, and Java serialised object: generated in-test via JDK APIs (no public
 *       corpus provides a reliably uncompressed TAR or a {@code .ser} file).
 * </ul>
 */
// @Ignore("Corpus validation test — downloads external files at runtime. Remove @Ignore to run
// manually.")
public class FileTypeMagicUtilTikaCorpusTest extends SolrTestCaseJ4 {

  private static final String TIKA_PKG =
      "https://raw.githubusercontent.com/apache/tika/main/tika-parsers/"
          + "tika-parsers-standard/tika-parsers-standard-modules/"
          + "tika-parser-pkg-module/src/test/resources/test-documents/";

  private static final String TIKA_CODE =
      "https://raw.githubusercontent.com/apache/tika/main/tika-parsers/"
          + "tika-parsers-standard/tika-parsers-standard-modules/"
          + "tika-parser-code-module/src/test/resources/test-documents/";

  private static final String XZ_UTILS =
      "https://raw.githubusercontent.com/tukaani-project/xz/master/tests/files/";

  /**
   * Downloads one representative binary file per MIME type that {@link FileTypeMagicUtil} claims to
   * detect, runs the detector on each, and fails if any claimed type is not recognised (i.e. falls
   * back to {@code application/octet-stream}).
   */
  public void testDetectAllClaimedTypes() throws Exception {
    Path downloadDir = createTempDir("magic-corpus-remote");
    Path generatedDir = createTempDir("magic-corpus-generated");

    // Pre-generate local files for formats with no suitable public corpus entry.
    // Each is written to generatedDir so they are treated identically to downloads below.
    generateUstarTar(generatedDir.resolve("generated.tar"));
    Files.writeString(
        generatedDir.resolve("generated.sh"), "#!/bin/sh\necho hello\n", StandardCharsets.US_ASCII);
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    writeSerializedObject(buf);
    Files.write(generatedDir.resolve("generated.ser"), buf.toByteArray());

    // MIME type → file to test. Remote files reference a URL (downloaded to downloadDir);
    // generated files are pre-built in generatedDir.
    // Add a new entry here whenever a new format is added to FileTypeMagicUtil.
    Map<String, Object> corpus = new LinkedHashMap<>();
    corpus.put("application/zip", TIKA_PKG + "gbk.zip"); // 432 B, empty-archive ZIP
    corpus.put("application/gzip", TIKA_PKG + "bob.gz"); // 41 B
    corpus.put("application/x-bzip2", TIKA_PKG + "test-bz2.txt.bz2"); // 56 B
    corpus.put("application/x-xz", XZ_UTILS + "good-0-empty.xz"); // minimal valid XZ stream
    corpus.put("application/x-java-applet", TIKA_CODE + "AppleSingleFileParser.class");
    corpus.put("application/x-dosexec", TIKA_CODE + "testWindows-x86-32.exe");
    corpus.put("application/x-executable", TIKA_CODE + "testLinux-arm-32le");
    corpus.put("application/x-mach-binary", TIKA_CODE + "testMacOS-x86_64");
    corpus.put("application/x-tar", generatedDir.resolve("generated.tar")); // no public corpus
    corpus.put("text/x-shellscript", generatedDir.resolve("generated.sh")); // trivially generated
    corpus.put(
        "application/x-java-serialized-object",
        generatedDir.resolve("generated.ser")); // via ObjectOutputStream

    List<String> failures = new ArrayList<>();
    System.out.println("\n=== FileTypeMagicUtil real-world detection results ===");

    for (Map.Entry<String, Object> entry : corpus.entrySet()) {
      String expectedMime = entry.getKey();
      Object source = entry.getValue();
      Path file;
      String label;
      if (source instanceof String url) {
        label = url.substring(url.lastIndexOf('/') + 1);
        file = downloadDir.resolve(label);
        try (InputStream in = URI.create(url).toURL().openStream()) {
          Files.copy(in, file);
        }
      } else {
        file = (Path) source;
        label = file.getFileName().toString();
      }
      record(expectedMime, FileTypeMagicUtil.INSTANCE.guessMimeType(file), label, failures);
    }

    System.out.println("=== End of results ===\n");
    assertTrue(
        "FileTypeMagicUtil failed to detect types it claims to handle:\n"
            + String.join("\n", failures),
        failures.isEmpty());
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static void record(
      String expected, String detected, String label, List<String> failures) {
    if (expected.equals(detected)) {
      System.out.printf(Locale.ROOT, "  PASS  %-50s  %s%n", label, detected);
    } else {
      String msg =
          String.format(
              Locale.ROOT, "  FAIL  %-50s  expected=%-45s  got=%s", label, expected, detected);
      System.out.println(msg);
      failures.add(msg);
    }
  }

  /**
   * Writes a Java serialized object stream to {@code out}. Isolated into its own method so the
   * {@code @SuppressForbidden} annotation is scoped narrowly to the serialization code.
   */
  @SuppressForbidden(
      reason =
          "ObjectOutputStream is used here to PRODUCE (not consume) a serialized-object byte stream"
              + " for magic-byte detection testing. No untrusted data is deserialized.")
  private static void writeSerializedObject(ByteArrayOutputStream out) throws Exception {
    try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
      oos.writeObject(new ArrayList<>());
    }
  }

  /**
   * Writes a minimal valid POSIX ustar TAR archive to {@code dest}. The file contains one empty
   * entry and is structurally correct (valid checksum), making it detectable as {@code
   * application/x-tar} by any magic-byte detector that checks for "ustar" at offset 257.
   */
  private static void generateUstarTar(Path dest) throws Exception {
    byte[] tar = new byte[1536]; // header block + 2 end-of-archive zero blocks

    byte[] name = "hello.txt".getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(name, 0, tar, 0, name.length);

    // Mode, uid, gid
    System.arraycopy(new byte[] {'0', '0', '0', '0', '6', '4', '4', 0}, 0, tar, 100, 8);
    System.arraycopy(new byte[] {'0', '0', '0', '0', '0', '0', '0', 0}, 0, tar, 108, 8);
    System.arraycopy(new byte[] {'0', '0', '0', '0', '0', '0', '0', 0}, 0, tar, 116, 8);

    // Size and mtime (zero)
    System.arraycopy(
        new byte[] {'0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', 0}, 0, tar, 124, 12);
    System.arraycopy(
        new byte[] {'0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', 0}, 0, tar, 136, 12);

    tar[156] = '0'; // type flag: regular file

    // ustar magic + version
    tar[257] = 'u';
    tar[258] = 's';
    tar[259] = 't';
    tar[260] = 'a';
    tar[261] = 'r';
    tar[262] = 0;
    tar[263] = '0';
    tar[264] = '0';

    // Checksum: sum of header bytes treating bytes 148-155 as spaces, stored as 6-digit octal.
    for (int i = 148; i < 156; i++) tar[i] = ' ';
    int checksum = 0;
    for (int i = 0; i < 512; i++) checksum += (tar[i] & 0xFF);
    byte[] cs = String.format(Locale.ROOT, "%06o\0 ", checksum).getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(cs, 0, tar, 148, 8);

    Files.write(dest, tar);
  }
}
