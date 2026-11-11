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

package org.apache.solr.shell.session;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.apache.solr.shell.exec.CommandResult;

/**
 * The Solr-aware conveniences a shell test reaches for, ported from {@code bats_helper.bash}.
 *
 * <p>Separate from {@link CliSession}, which knows only about environments, working directories and
 * running commands. Everything here is expressed in terms of that: it either shells out through the
 * session or talks HTTP to a node the session started.
 *
 * <p>Reached through {@link CliSession#helpers()}, or directly as inherited shorthands on {@code
 * SolrShellTestCase}.
 */
public final class SolrCliHelpers {

  private final CliSession cli;

  SolrCliHelpers(CliSession cli) {
    this.cli = cli;
  }

  /** The response to a harness HTTP request. */
  public record HttpResult(int status, String body) {}

  /**
   * A GET against {@code http://localhost:<SOLR_PORT><pathAndQuery>}.
   *
   * <p>Stands in for the {@code curl} calls in the BATS suites, which is most of what makes
   * readiness checks portable: there is no curl on a stock Windows runner.
   */
  public HttpResult http(String pathAndQuery) {
    return http(cli.ports().solr(), pathAndQuery);
  }

  /** A GET against an explicit port on localhost. */
  public HttpResult http(int port, String pathAndQuery) {
    String url = "http://localhost:" + port + pathAndQuery;
    try (HttpClient client =
        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
      HttpRequest request =
          HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(30)).GET().build();
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      return new HttpResult(response.statusCode(), response.body());
    } catch (IOException e) {
      return new HttpResult(-1, e.toString());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted requesting " + url, e);
    }
  }

  /** Whether the named collection is in ZooKeeper. Port of {@code collection_exists}. */
  public boolean collectionExists(String collection) {
    return zkChildren("/collections").contains(collection);
  }

  /** Whether the named config set is in ZooKeeper. Port of {@code config_exists}. */
  public boolean configExists(String configName) {
    return zkChildren("/configs").contains(configName);
  }

  private List<String> zkChildren(String zkPath) {
    CommandResult result = cli.run("solr zk ls " + zkPath + " -z localhost:${ZK_PORT}");
    if (result.failed()) {
      return List.of();
    }
    return result.lines().stream().map(String::trim).filter(s -> !s.isEmpty()).toList();
  }

  /** Port of {@code delete_all_collections}. */
  public void deleteAllCollections() {
    for (String collection : zkChildren("/collections")) {
      cli.run("solr delete -c " + collection + " --delete-config");
    }
  }

  /**
   * Port of {@code shutdown_all}: {@code solr stop --all}, ignoring the outcome.
   *
   * <p>Scoped to this class, because {@code bin/solr} implements {@code --all} by walking {@code
   * SOLR_PID_DIR} and the harness gives every class its own.
   */
  public void shutdownAll() {
    try {
      cli.run(Duration.ofSeconds(120), "solr stop --all");
    } catch (RuntimeException | AssertionError e) {
      // Cleanup helper: the caller is usually already handling a failure.
    }
  }

  /** Prints the tail of {@code solr.log} to stderr, for debugging a failing test. */
  public void dumpSolrLog() {
    Path logFile = cli.harnessEnv().logsDir().resolve("solr.log");
    if (!Files.isRegularFile(logFile)) {
      System.err.println("No solr.log at " + logFile);
      return;
    }
    try {
      List<String> lines = Files.readAllLines(logFile, StandardCharsets.UTF_8);
      List<String> tail = lines.subList(Math.max(0, lines.size() - 200), lines.size());
      System.err.println("--- tail of " + logFile + " ---");
      tail.forEach(System.err::println);
    } catch (IOException e) {
      System.err.println("Could not read " + logFile + ": " + e);
    }
  }
}
