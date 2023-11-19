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
package org.apache.jmh;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.PumpStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JMHUtils {

  private static final Logger log = LoggerFactory.getLogger(JMHUtils.class);

  public static final String USER_HOME = "user.home";
  private static final String JOVERFLOW_LOC = System.getProperty(USER_HOME) + "/Sync/joverflow";

  private static final String JXRAY_LOC = System.getProperty(USER_HOME) + "/Sync/jxray";
  private static final String MAT_LOC = System.getProperty(USER_HOME) + "/Sync/mat";

  private JMHUtils() {
  }

  public static List<String> processHeapDumpFolder(File directory, File resourceDir, Path resourcePath) throws IOException {
    Path dest = Paths.get(resourceDir.getAbsolutePath(), resourcePath.toString());
    log.info("process heapdump folder: {} destDir: {}", directory, dest);
    Files.createDirectories(dest);
    String[] files = directory.list((dir, name) -> name.endsWith(".hprof"));
    List<String> paths = new ArrayList<>();
    for (String file : files) {
      log.info("Processing heapdump: {}", file);
      paths.addAll(processJXray(directory, file, dest, resourcePath));
      paths.addAll(processJoverflow(directory, file, dest, resourcePath));
      paths.addAll(processMat(directory, file, dest, resourcePath));
    }

    return paths;
  }

  private static List<String> processMat(File directory, String file, Path destDir, Path resourcePath) throws IOException {
    DefaultExecutor executor = new DefaultExecutor();
    executor.setWorkingDirectory(new File(MAT_LOC));
    ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
    executor.setStreamHandler(new PumpStreamHandler(baos, baos));
    CommandLine cmdLine = CommandLine.parse("bash");

    cmdLine.addArgument("ParseHeapDump.sh", false);
    cmdLine.addArgument(new File(directory, file).getAbsolutePath(), false);
    cmdLine.addArgument("org.eclipse.mat.api:suspects", false);
    cmdLine.addArgument("org.eclipse.mat.api:top_components", false);
    cmdLine.addArgument("org.eclipse.mat.api:overview", false);

    //System.out.println("execute: " + cmdLine);
    try {
      int exitVal = executor.execute(cmdLine);
    } catch(ExecuteException e) {
      log.error("{} :{}", e.getMessage(), baos.toString(UTF_8));
      throw e;
    }

    String[] files = directory.list((dir, name) -> name.endsWith(".zip"));
    List<String> paths = new ArrayList<>();
    for (String zipFile : files) {
      unzip(new File(directory, zipFile), new File(new File(destDir.toAbsolutePath().toString(), "mat"), zipFile));
      paths.add(resourcePath + "/mat/" + zipFile + "/index.html");
    }
    return paths;
  }

  private static List<String> processJoverflow(File directory, String file, Path destDir, Path resourcePath) throws IOException {
    DefaultExecutor executor = new DefaultExecutor();
    executor.setWorkingDirectory(new File(JOVERFLOW_LOC));
    ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
    executor.setStreamHandler(new PumpStreamHandler(baos, baos));
    CommandLine cmdLine = CommandLine.parse("java");

    cmdLine.addArgument("-cp", false);
    cmdLine.addArgument("joverflow.jar:jmc.common.jar:org.lz4.lz4-java_1.7.1.jar", false);
    cmdLine.addArgument("org.openjdk.jmc.joverflow.Main", false);
    cmdLine.addArgument("-use_mmap", false);
    cmdLine.addArgument(new File(directory, file).getAbsolutePath(), false);

    //System.out.println("execute: " + cmdLine);
    int exitVal = executor.execute(cmdLine);

    Path outDir = Paths.get(destDir.toAbsolutePath().toString(), "joverflow");
    Files.createDirectories(outDir);
    Files.writeString(Paths.get(outDir.toString(), "report.txt"), baos.toString(UTF_8));
    return List.of(Paths.get(resourcePath.toString(), "joverflow", "report.txt").toString());
  }

  private static List<String> processJXray(File directory, String file, Path destDir, Path resourcePath) throws IOException {
    DefaultExecutor executor = new DefaultExecutor();
    executor.setWorkingDirectory(new File(JXRAY_LOC));
    ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
    executor.setStreamHandler(new PumpStreamHandler(baos, baos));
    CommandLine cmdLine = CommandLine.parse("bash");

    File outDir = new File(destDir.toAbsolutePath().toString(), "jxray");
    outDir.mkdirs();

    cmdLine.addArgument("jxray.sh", false);
    cmdLine.addArgument(new File(directory, file).getAbsolutePath(), false);
    cmdLine.addArgument(new File(outDir, "index.html").getAbsolutePath(), false);

    //System.out.println("execute: " + cmdLine);
    int exitVal = executor.execute(cmdLine);

    return List.of(Paths.get(resourcePath.toString(), "jxray", "index.html").toString());

  }

  public static void unzip(File fileZip, File destDir) throws IOException {

    final byte[] buffer = new byte[1024];
    final ZipInputStream zis = new ZipInputStream(Files.newInputStream(fileZip.toPath()));
    ZipEntry zipEntry = zis.getNextEntry();
    while (zipEntry != null) {
      final File newFile = newFile(destDir, zipEntry);
      if (zipEntry.isDirectory()) {
        if (!newFile.isDirectory() && !newFile.mkdirs()) {
          throw new IOException("Failed to create directory " + newFile);
        }
      } else {
        File parent = newFile.getParentFile();
        if (!parent.isDirectory() && !parent.mkdirs()) {
          throw new IOException("Failed to create directory " + parent);
        }

        final FileOutputStream fos = new FileOutputStream(newFile);
        int len;
        while ((len = zis.read(buffer)) > 0) {
          fos.write(buffer, 0, len);
        }
        fos.close();
      }
      zipEntry = zis.getNextEntry();
    }
    zis.closeEntry();
    zis.close();
  }

  /**
   *  https://snyk.io/research/zip-slip-vulnerability
   */
  public static File newFile(File destinationDir, ZipEntry zipEntry) throws IOException {
    File destFile = new File(destinationDir, zipEntry.getName());

    String destDirPath = destinationDir.getCanonicalPath();
    String destFilePath = destFile.getCanonicalPath();

    if (!destFilePath.startsWith(destDirPath + File.separator)) {
      throw new IOException("Entry is outside of the target dir: " + zipEntry.getName());
    }

    return destFile;
  }

}
