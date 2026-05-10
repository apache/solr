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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.EnvUtils;

/** Utility class to guess the mime type of file based on its magic number. */
public class FileTypeMagicUtil {
  private static final Set<String> SKIP_FOLDERS = new HashSet<>(Arrays.asList(".", ".."));

  public static final FileTypeMagicUtil INSTANCE = new FileTypeMagicUtil();

  /**
   * Default set of MIME types forbidden in configsets. Overridable via the system property {@code
   * solr.configset.upload.mimetypes.forbidden}.
   */
  public static final List<String> DEFAULT_FORBIDDEN_MIME_TYPES =
      List.of(
          "application/x-java-applet",
          "application/zip",
          "application/x-tar",
          "application/gzip",
          "application/x-bzip2",
          "application/x-xz",
          "text/x-shellscript",
          "application/x-dosexec",
          "application/x-executable",
          "application/x-java-serialized-object",
          "application/x-mach-binary");

  // TAR ustar magic is at offset 257-261; 512 bytes (one full TAR block) covers all formats.
  private static final int HEADER_BYTES = 512;

  // ZIP End-of-Central-Directory (EOCD) record is 22 bytes; comment field is at most 65535 bytes.
  private static final int EOCD_SIZE = 22;
  private static final int EOCD_MAX_SCAN = EOCD_SIZE + 65535;

  FileTypeMagicUtil() {}

  /**
   * Asserts that an entire configset folder is legal to upload.
   *
   * @param confPath the path to the folder
   * @throws SolrException if an illegal file is found in the folder structure
   */
  public static void assertConfigSetFolderLegal(Path confPath) throws IOException {
    Files.walkFileTree(
        confPath,
        new SimpleFileVisitor<>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
            if (Files.isSymbolicLink(file)) {
              throw new SolrException(
                  SolrException.ErrorCode.BAD_REQUEST,
                  String.format(
                      Locale.ROOT,
                      "Not uploading symbolic link %s to configset, as symbolic links are not supported in ZooKeeper",
                      file));
            }
            String mimeType = FileTypeMagicUtil.INSTANCE.guessMimeType(file);
            if (forbiddenTypes.contains(mimeType)) {
              throw new SolrException(
                  SolrException.ErrorCode.BAD_REQUEST,
                  String.format(
                      Locale.ROOT,
                      "Not uploading file %s to configset, as it matched the MAGIC signature of a forbidden mime type %s",
                      file,
                      mimeType));
            }
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
            if (SKIP_FOLDERS.contains(dir.getFileName().toString()))
              return FileVisitResult.SKIP_SUBTREE;

            return FileVisitResult.CONTINUE;
          }
        });
  }

  /**
   * Guess the mime type of file based on its magic number. In addition to the standard header scan,
   * this method performs a tail scan for polyglot ZIP files (e.g. a JPEG that also contains a valid
   * ZIP End-of-Central-Directory record appended at the end).
   *
   * @param file file to check
   * @return string with content-type or "application/octet-stream" if unknown
   */
  public String guessMimeType(Path file) {
    try {
      String headerResult;
      try (InputStream in = Files.newInputStream(file)) {
        headerResult = guessMimeType(in);
      }
      // If the header already identified a ZIP, skip the redundant tail scan.
      if (!"application/zip".equals(headerResult) && hasZipEocdTail(file)) {
        return "application/zip";
      }
      return headerResult;
    } catch (IOException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Failed to guess mime type for file " + file, e);
    }
  }

  /**
   * Scans the tail of a file for a ZIP End-of-Central-Directory (EOCD) record. This detects
   * polyglot files (e.g. JPEG+ZIP) where the ZIP content is appended after another format's data.
   */
  private static boolean hasZipEocdTail(Path file) throws IOException {
    long fileSize = Files.size(file);
    if (fileSize < EOCD_SIZE) return false;
    long readStart = Math.max(0, fileSize - EOCD_MAX_SCAN);
    int readLen = (int) (fileSize - readStart);
    byte[] tail = new byte[readLen];
    try (InputStream in = Files.newInputStream(file)) {
      in.skipNBytes(readStart);
      int n = 0, r;
      while (n < readLen && (r = in.read(tail, n, readLen - n)) != -1) n += r;
    }
    return hasZipEocd(tail);
  }

  /**
   * Scans backward through {@code b} for a ZIP End-of-Central-Directory signature ({@code
   * PK\x05\x06}). The scan is bounded to the last {@link #EOCD_MAX_SCAN} bytes to match the ZIP
   * specification's maximum comment length.
   *
   * <p>When {@code b} contains the complete file content, this detects polyglot ZIPs regardless of
   * what format occupies offset 0 (e.g. JPEG, PDF). When {@code b} contains only a leading chunk,
   * polyglot ZIPs whose EOCD falls outside that chunk will not be detected.
   */
  static boolean hasZipEocd(byte[] b) {
    int start = Math.max(0, b.length - EOCD_MAX_SCAN);
    for (int i = b.length - EOCD_SIZE; i >= start; i--) {
      if (b[i] == 'P' && b[i + 1] == 'K' && b[i + 2] == 0x05 && b[i + 3] == 0x06) {
        return true;
      }
    }
    return false;
  }

  /**
   * Guess the mime type of file based on its magic number. Package-private; external callers with
   * an InputStream should use {@link #isFileForbiddenInConfigset(InputStream)}.
   *
   * @param stream input stream of the file
   * @return string with content-type or "application/octet-stream" if unknown
   */
  String guessMimeType(InputStream stream) {
    try {
      byte[] buf = new byte[HEADER_BYTES];
      int n = 0, read;
      while (n < HEADER_BYTES && (read = stream.read(buf, n, HEADER_BYTES - n)) != -1) {
        n += read;
      }
      return guessMimeType(Arrays.copyOf(buf, n));
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  /**
   * Guess the mime type of file bytes based on its magic number.
   *
   * <p>When {@code bytes} contains the complete file content, polyglot ZIPs (e.g. JPEG+ZIP) are
   * also detected via a tail scan for the ZIP End-of-Central-Directory record. Callers that supply
   * only a leading chunk of a file will not benefit from polyglot detection; prefer {@link
   * #guessMimeType(Path)} when the full file is available on disk.
   *
   * @param bytes the file bytes (ideally the complete file content)
   * @return string with content-type or "application/octet-stream" if unknown
   */
  public String guessMimeType(byte[] bytes) {
    if (bytes == null || bytes.length == 0) return "application/octet-stream";
    if (isJavaClass(bytes)) return "application/x-java-applet";
    if (isZip(bytes)) return "application/zip";
    if (isTar(bytes)) return "application/x-tar";
    if (isGzip(bytes)) return "application/gzip";
    if (isBzip2(bytes)) return "application/x-bzip2";
    if (isXz(bytes)) return "application/x-xz";
    if (isShellScript(bytes)) return "text/x-shellscript";
    if (isMzExecutable(bytes)) return "application/x-dosexec";
    if (isElf(bytes)) return "application/x-executable";
    if (isJavaSerialized(bytes)) return "application/x-java-serialized-object";
    if (isMachO(bytes)) return "application/x-mach-binary";
    if (hasZipEocd(bytes)) return "application/zip";
    return "application/octet-stream";
  }

  /**
   * Determine forbidden file type based on magic bytes matching of the file itself. Includes tail
   * scanning for polyglot ZIP files. The default forbidden types are listed in {@link
   * #DEFAULT_FORBIDDEN_MIME_TYPES} and can be overridden via the system property {@code
   * solr.configset.upload.mimetypes.forbidden}.
   *
   * @param file file to check
   * @return true if file is among the forbidden mime-types
   */
  public static boolean isFileForbiddenInConfigset(Path file) {
    return forbiddenTypes.contains(FileTypeMagicUtil.INSTANCE.guessMimeType(file));
  }

  /**
   * Determine forbidden file type based on magic bytes matching of the file itself. See {@link
   * #isFileForbiddenInConfigset(Path)} for the list of forbidden types.
   *
   * @param fileStream stream from the file content
   * @return true if file is among the forbidden mime-types
   */
  static boolean isFileForbiddenInConfigset(InputStream fileStream) {
    return forbiddenTypes.contains(FileTypeMagicUtil.INSTANCE.guessMimeType(fileStream));
  }

  /**
   * Determine forbidden file type based on magic bytes matching of the first bytes of the file.
   *
   * @param bytes byte array of the file content
   * @return true if file is among the forbidden mime-types
   */
  public static boolean isFileForbiddenInConfigset(byte[] bytes) {
    if (bytes == null || bytes.length == 0)
      return false; // A ZK znode may be a folder with no content
    return forbiddenTypes.contains(FileTypeMagicUtil.INSTANCE.guessMimeType(bytes));
  }

  private static final Set<String> forbiddenTypes = buildForbiddenTypes();

  private static Set<String> buildForbiddenTypes() {
    return buildForbiddenTypes(
        EnvUtils.getPropertyAsList(
            "solr.configset.upload.mimetypes.forbidden", DEFAULT_FORBIDDEN_MIME_TYPES));
  }

  // Package-private to allow direct testing without system-property manipulation.
  static Set<String> buildForbiddenTypes(List<String> configured) {
    Set<String> known = new HashSet<>(DEFAULT_FORBIDDEN_MIME_TYPES);
    if (!known.containsAll(configured)) {
      Set<String> unknown = new HashSet<>(configured);
      unknown.removeAll(known);
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "System property solr.configset.upload.mimetypes.forbidden contains unrecognized MIME type(s) "
              + unknown
              + ". Only the following types are supported: "
              + DEFAULT_FORBIDDEN_MIME_TYPES);
    }
    return new HashSet<>(configured);
  }

  /**
   * Detects JVM class files by the 0xCAFEBABE magic. Kotlin, Scala and Groovy use the same bytes.
   */
  private static boolean isJavaClass(byte[] b) {
    return b.length >= 4
        && (b[0] & 0xFF) == 0xCA
        && (b[1] & 0xFF) == 0xFE
        && (b[2] & 0xFF) == 0xBA
        && (b[3] & 0xFF) == 0xBE;
  }

  /**
   * Detects ZIP/JAR archives by the PK magic at offset 0. Handles three signatures: PK\x03\x04
   * (local file header), PK\x05\x06 (empty archive), PK\x07\x08 (data descriptor).
   *
   * <p>Polyglot files (e.g. JPEG+ZIP) are not detected: their ZIP content is appended after the
   * outer format's end marker and does not appear at offset 0.
   */
  private static boolean isZip(byte[] b) {
    return b.length >= 4
        && b[0] == 'P'
        && b[1] == 'K'
        && ((b[2] == 0x03 && b[3] == 0x04)
            || (b[2] == 0x05 && b[3] == 0x06)
            || (b[2] == 0x07 && b[3] == 0x08));
  }

  /**
   * Detects POSIX/GNU ustar TAR archives via the magic at offset 257. V7-format tars have no magic
   * and are not detected, but they are essentially extinct.
   */
  private static boolean isTar(byte[] b) {
    return b.length >= 262
        && b[257] == 'u'
        && b[258] == 's'
        && b[259] == 't'
        && b[260] == 'a'
        && b[261] == 'r';
  }

  /** Detects gzip compressed files (magic: 1F 8B). */
  private static boolean isGzip(byte[] b) {
    return b.length >= 2 && (b[0] & 0xFF) == 0x1F && (b[1] & 0xFF) == 0x8B;
  }

  /** Detects bzip2 compressed files (magic: "BZh"). */
  private static boolean isBzip2(byte[] b) {
    return b.length >= 3 && b[0] == 'B' && b[1] == 'Z' && b[2] == 'h';
  }

  /** Detects xz compressed files (magic: FD 37 7A 58 5A 00). */
  private static boolean isXz(byte[] b) {
    return b.length >= 6
        && (b[0] & 0xFF) == 0xFD
        && b[1] == '7'
        && b[2] == 'z'
        && b[3] == 'X'
        && b[4] == 'Z'
        && b[5] == 0x00;
  }

  /**
   * Detects shebang-based scripts by the {@code #!} marker at the start of the file. This covers
   * Unix shell scripts, Python, Ruby, Perl, and similar interpreted languages.
   */
  private static boolean isShellScript(byte[] b) {
    return b.length >= 2 && b[0] == '#' && b[1] == '!';
  }

  /** Detects Windows PE/EXE/DLL and self-extracting ZIPs by the MZ header. */
  private static boolean isMzExecutable(byte[] b) {
    return b.length >= 2 && b[0] == 'M' && b[1] == 'Z';
  }

  /** Detects Linux/Unix ELF executables and shared libraries. */
  private static boolean isElf(byte[] b) {
    return b.length >= 4 && (b[0] & 0xFF) == 0x7F && b[1] == 'E' && b[2] == 'L' && b[3] == 'F';
  }

  /** Detects Java serialized object streams. */
  private static boolean isJavaSerialized(byte[] b) {
    return b.length >= 4
        && (b[0] & 0xFF) == 0xAC
        && (b[1] & 0xFF) == 0xED
        && b[2] == 0x00
        && b[3] == 0x05;
  }

  /**
   * Detects macOS Mach-O binaries in all four variants (32/64-bit, big/little-endian). Fat binaries
   * (0xCAFEBABE) are already blocked by isJavaClass().
   */
  private static boolean isMachO(byte[] b) {
    if (b.length < 4) return false;
    int m = ((b[0] & 0xFF) << 24) | ((b[1] & 0xFF) << 16) | ((b[2] & 0xFF) << 8) | (b[3] & 0xFF);
    return m == 0xFEEDFACE // 32-bit big-endian
        || m == 0xFEEDFACF // 64-bit big-endian
        || m == 0xCEFAEDFE // 32-bit little-endian
        || m == 0xCFFAEDFE; // 64-bit little-endian
  }
}
