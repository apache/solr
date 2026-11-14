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

package org.apache.solr.cats;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import org.apache.solr.cats.env.PortBlock;
import org.apache.solr.cats.exec.BackgroundCommand;
import org.apache.solr.cats.exec.CommandResult;
import org.apache.solr.cats.session.CatsCliHelpers;
import org.apache.solr.cats.session.CliSession;
import org.apache.solr.cats.session.HttpsRequest;

/**
 * The vocabulary a test writes in: {@code run}, {@code runFail}, {@code env}, {@code cd}, the file
 * helpers and the HTTP helpers, each one line over the session they belong to.
 *
 * <p>An interface rather than a base class, because a project that already has a test base class of
 * its own cannot extend a second one. Implementing this adds the whole vocabulary to that class for
 * the cost of {@link #cli()}; {@link CatsTestCase} is the ready-made implementation for a project
 * with no base class to keep.
 *
 * <p>Interface methods are public, which is the one visible consequence: a test case implementing
 * this cannot keep the vocabulary protected. Test classes are public anyway.
 */
public interface CatsDsl {

  /**
   * The session the running test runs its commands in, which is what every method here delegates
   * to.
   */
  CliSession cli();

  // ------------------------------------------------- method-scope shorthands
  // These all delegate to the session for the running test, so a test method reads like the
  // command line it stands for: run("mytool ls /"). Only a class-scoped setup has to name a
  // session, and there the session is the class' own — which keeps that scope visible.

  default CommandResult run(String line) {
    return cli().run(line);
  }

  /** For a command expected to fail; {@link #run(String)} asserts success. */
  default CommandResult runFail(String line) {
    return cli().runFail(line);
  }

  default CommandResult runFail(Duration commandTimeout, String line) {
    return cli().runFail(commandTimeout, line);
  }

  /** For a predicate that inspects the status itself; asserts nothing. */
  default CommandResult tryRun(String line) {
    return cli().tryRun(line);
  }

  default CommandResult run(Duration commandTimeout, String line) {
    return cli().run(commandTimeout, line);
  }

  default CommandResult run(List<String> argv) {
    return cli().run(argv);
  }

  default CommandResult runSeparated(String line) {
    return cli().runSeparated(line);
  }

  /** For a command that reads its input: the text goes in on standard input, not through a pipe. */
  default CommandResult runWithStdin(String stdin, String line) {
    return cli().runWithStdin(stdin, line);
  }

  /**
   * For a program that only exits when it is stopped, or a long-running tool a test watches: starts
   * the command and hands back the live process instead of waiting for it.
   */
  default BackgroundCommand runInBackground(String line) {
    return cli().runInBackground(line);
  }

  default CommandResult lastResult() {
    return cli().lastResult();
  }

  default CliSession env(String key, String value) {
    return cli().env(key, value);
  }

  default CliSession unset(String key) {
    return cli().unset(key);
  }

  default String env(String key) {
    return cli().env(key);
  }

  default CliSession cd(String dir) {
    return cli().cd(dir);
  }

  default CliSession cd(Path dir) {
    return cli().cd(dir);
  }

  default Path touch(String path) {
    return cli().touch(path);
  }

  // File work a shell script would do with mkdir, rm, cp, cat, ls and find. Paths resolve against
  // the session's working directory, so a relative path reads exactly as it would in the shell.

  default Path path(String path) {
    return cli().path(path);
  }

  default Path mkdirs(String dir) {
    return cli().mkdirs(dir);
  }

  default Path write(String path, String content) {
    return cli().write(path, content);
  }

  default Path append(String path, String content) {
    return cli().append(path, content);
  }

  default String read(String path) {
    return cli().read(path);
  }

  default List<String> readLinesMatching(String path, String regex) {
    return cli().readLinesMatching(path, regex);
  }

  default boolean fileContains(String path, String text) {
    return cli().fileContains(path, text);
  }

  default boolean exists(String path) {
    return cli().exists(path);
  }

  default void rm(String path) {
    cli().rm(path);
  }

  default Path copy(String from, String to) {
    return cli().copy(from, to);
  }

  default List<Path> list(String dir) {
    return cli().list(dir);
  }

  default List<Path> list(String dir, String glob) {
    return cli().list(dir, glob);
  }

  /**
   * Copies a file from the test classpath into the session's working directory, for a fixture that
   * is checked in rather than generated — a configuration file, a document, a web page to serve.
   *
   * <p>{@code resource} is resolved against the test class's own package, or from the classpath
   * root if it starts with {@code /}. Test resources live in {@code src/test-files}.
   *
   * @return the absolute path of the copy
   */
  default Path copyResource(String resource, String path) {
    String name =
        resource.startsWith("/")
            ? resource.substring(1)
            : getClass().getPackageName().replace('.', '/') + "/" + resource;
    try (InputStream in = getClass().getClassLoader().getResourceAsStream(name)) {
      if (in == null) {
        throw new AssertionError("No such resource on the test classpath: " + name);
      }
      Path target = cli().path(path);
      Files.createDirectories(target.getParent());
      Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
      return target;
    } catch (IOException e) {
      throw new UncheckedIOException("Could not copy resource " + name, e);
    }
  }

  /**
   * Serves a directory over HTTP on a free port, for a test whose subject is the program reading a
   * URL. Stopped automatically when the test ends.
   */
  default int serveHttp(String webRoot) {
    return cli().serveHttp(webRoot);
  }

  /** Stops a server from {@link #serveHttp} early, for a test that wants a fetch to fail. */
  default void serveHttpShutdown(int port) {
    cli().serveHttpShutdown(port);
  }

  default Path download(String url, String path) {
    return cli().download(url, path);
  }

  default Path cwd() {
    return cli().cwd();
  }

  default Path workDir() {
    return cli().workDir();
  }

  default boolean waitFor(Duration waitTimeout, Duration interval, BooleanSupplier condition) {
    return cli().waitFor(waitTimeout, interval, condition);
  }

  default void awaitOrFail(
      String what, Duration waitTimeout, Duration interval, BooleanSupplier condition) {
    cli().awaitOrFail(what, waitTimeout, interval, condition);
  }

  /** The ports this run owns. */
  default PortBlock ports() {
    return cli().ports();
  }

  /** The named traits of this run. */
  default Map<String, String> traits() {
    return cli().traits();
  }

  /** The wording trait {@code name}'s value calls for in this run. */
  default String wordingFor(String name, Map<String, String> byValue) {
    return cli().wordingFor(name, byValue);
  }

  default CatsCliHelpers.HttpResult http(String pathAndQuery) {
    return cli().helpers().http(pathAndQuery);
  }

  default CatsCliHelpers.HttpResult http(int port, String pathAndQuery) {
    return cli().helpers().http(port, pathAndQuery);
  }

  default CatsCliHelpers.HttpResult http(String pathAndQuery, Map<String, String> requestHeaders) {
    return cli().helpers().http(pathAndQuery, requestHeaders);
  }

  default CatsCliHelpers.HttpResult http(
      int port, String pathAndQuery, Map<String, String> requestHeaders) {
    return cli().helpers().http(port, pathAndQuery, requestHeaders);
  }

  default CatsCliHelpers.HttpResult http(String pathAndQuery, String user, String password) {
    return cli().helpers().http(pathAndQuery, user, password);
  }

  default CatsCliHelpers.HttpResult http(
      int port, String pathAndQuery, String user, String password) {
    return cli().helpers().http(port, pathAndQuery, user, password);
  }

  default CatsCliHelpers.HttpResult httpPost(String pathAndQuery, String contentType, String body) {
    return cli().helpers().httpPost(pathAndQuery, contentType, body);
  }

  default CatsCliHelpers.HttpResult httpPost(
      int port, String pathAndQuery, String contentType, String body) {
    return cli().helpers().httpPost(port, pathAndQuery, contentType, body);
  }

  default CatsCliHelpers.HttpResult httpPut(String pathAndQuery, String contentType, Path file) {
    return cli().helpers().httpPut(pathAndQuery, contentType, file);
  }

  default CatsCliHelpers.HttpResult httpPut(
      int port, String pathAndQuery, String contentType, Path file) {
    return cli().helpers().httpPut(port, pathAndQuery, contentType, file);
  }

  default HttpsRequest https(String pathAndQuery) {
    return cli().helpers().https(pathAndQuery);
  }

  default HttpsRequest https(int port, String pathAndQuery) {
    return cli().helpers().https(port, pathAndQuery);
  }
}
