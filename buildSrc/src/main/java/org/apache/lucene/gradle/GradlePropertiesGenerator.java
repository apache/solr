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
package org.apache.lucene.gradle;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.APPEND;

/**
 * Standalone class that generates a populated gradle.properties from a template.
 * <p>
 * Has no dependencies outside of standard java libraries
 */
public class GradlePropertiesGenerator {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java GradlePropertiesGenerator.java <source> <destination>");
            System.exit(2);
        }

        try {
            new GradlePropertiesGenerator().run(Paths.get(args[0]), Paths.get(args[1]));
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            System.exit(3);
        }
    }

    public void run(Path source, Path destination) throws IOException {
        if (!Files.exists(source)) {
            throw new IOException("template file not found: " + source);
        }
        if (Files.exists(destination)) {
            System.out.println(destination + " already exists, skipping generation.");
        }

        // Approximate a common-sense default for running gradle/tests with parallel
        // workers: half the count of available cpus but not more than 12.
        var cpus = Runtime.getRuntime().availableProcessors();
        var maxWorkers = (int) Math.max(1d, Math.min(cpus * 0.5d, 12));
        var testsJvms = (int) Math.max(1d, Math.min(cpus * 0.5d, 12));

        var replacements = Map.of("org.gradle.workers.max", maxWorkers, "tests.jvms", testsJvms);

        // Java properties doesn't preserve comments, so going line by line instead
        // The properties file isn't big, nor is the map of replacements, so not worrying about speed here.
        System.out.println("Generating gradle.properties");
        final var lines = Files.readAllLines(source).stream().map(line -> {
            if (!line.startsWith("#") && line.contains("=")) {
                final String key = line.split("=")[0];
                var value = replacements.get(key);
                if (value != null) {
                  final String newLine = key + '=' + value;
                  System.out.println("Setting " + newLine);
                  return newLine;
              }
            }
            return line;
        }).collect(Collectors.toList());

        Files.write(destination, lines, StandardOpenOption.CREATE_NEW);
    }
}
