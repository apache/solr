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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;
import org.apache.solr.shell.exec.CommandResult;
import org.apache.solr.shell.exec.ShellAssertionError;
import org.apache.solr.shell.exec.Variables;

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

  /** How long a request a node answers from memory may take. */
  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

  /** How long a request whose body is a file may take, which is bounded by its size. */
  private static final Duration UPLOAD_TIMEOUT = Duration.ofMinutes(5);

  private final CliSession cli;

  SolrCliHelpers(CliSession cli) {
    this.cli = cli;
  }

  /**
   * The response to a harness HTTP request, with the same shape of fluent assertions a command
   * result has.
   *
   * <p>{@code curl} appears seventy times across the BATS suites, almost always followed by an
   * assertion on the body, so this is what those lines port to.
   */
  public record HttpResult(int status, String body, HttpHeaders headers, Map<String, String> vars) {

    /** A result with no response headers, for a request that never completed. */
    public HttpResult(int status, String body, Map<String, String> vars) {
      this(status, body, HttpHeaders.of(Map.of(), (name, value) -> true), vars);
    }

    /**
     * Resolves {@code ${NAME}} in an assertion's expected text, exactly as {@link CommandResult}
     * does. The two assertion families have to behave the same way: a ported BATS line asserting on
     * {@code ${SOLR_PORT}} must either work or fail loudly, never pass by comparing against the
     * unresolved text.
     */
    private String resolve(String expected) {
      return Variables.resolve(expected, Variables.ANY, vars, "an HTTP assertion");
    }

    public HttpResult assertStatus(int expected) {
      if (status != expected) {
        throw fail("status was " + status + ", expected " + expected);
      }
      return this;
    }

    public HttpResult assertContains(String substring) {
      String expected = resolve(substring);
      if (!body.contains(expected)) {
        throw fail("body did not contain: " + expected);
      }
      return this;
    }

    public HttpResult assertNotContains(String substring) {
      String expected = resolve(substring);
      if (body.contains(expected)) {
        throw fail("body contained what it should not: " + expected);
      }
      return this;
    }

    /**
     * Port of {@code assert_output --regexp}, with the same semantics {@link CommandResult} has.
     */
    public HttpResult assertMatches(String regex) {
      if (!Pattern.compile(resolve(regex), Pattern.DOTALL).matcher(body).find()) {
        throw fail("body did not match: " + resolve(regex));
      }
      return this;
    }

    /** Port of {@code refute_output --regexp}. */
    public HttpResult assertNotMatches(String regex) {
      if (Pattern.compile(resolve(regex), Pattern.DOTALL).matcher(body).find()) {
        throw fail("body matched what it should not: " + resolve(regex));
      }
      return this;
    }

    /**
     * The response headers as {@code Name: value} lines, which is what {@code curl -I} prints and
     * so what a BATS assertion on a header was written against.
     */
    public String headerText() {
      StringBuilder text = new StringBuilder();
      headers
          .map()
          .forEach(
              (name, values) ->
                  values.forEach(
                      value -> text.append(name).append(": ").append(value).append('\n')));
      return text.toString();
    }

    /**
     * Port of {@code curl -s -I ...} followed by {@code assert_output --partial}.
     *
     * <p>Matched without regard to case, because header names are case-insensitive by RFC and the
     * client here speaks HTTP/2 where they arrive lowercased, while the BATS lines this ports were
     * written against curl's HTTP/1.1 {@code Cache-Control: no-cache} spelling.
     */
    public HttpResult assertHeaderContains(String substring) {
      if (!headerTextLower().contains(resolve(substring).toLowerCase(Locale.ROOT))) {
        throw failHeaders("no response header contained: " + substring);
      }
      return this;
    }

    /** Port of {@code curl -s -I ...} followed by {@code refute_output --partial}. */
    public HttpResult assertHeaderNotContains(String substring) {
      if (headerTextLower().contains(resolve(substring).toLowerCase(Locale.ROOT))) {
        throw failHeaders("a response header contained what it should not: " + substring);
      }
      return this;
    }

    private String headerTextLower() {
      return headerText().toLowerCase(Locale.ROOT);
    }

    /**
     * Port of {@code run ! curl ...}: the request must not have completed at all — a TLS handshake
     * failure or a refused connection rather than an HTTP error status.
     */
    public HttpResult assertConnectionFailed() {
      if (status >= 0) {
        throw fail("the request completed with status " + status + ", expected it to fail");
      }
      return this;
    }

    private AssertionError fail(String reason) {
      return new ShellAssertionError("\nHTTP " + status + "\n| " + body + "\n\nFAILED: " + reason);
    }

    private AssertionError failHeaders(String reason) {
      return new ShellAssertionError(
          "\nHTTP " + status + "\n| " + headerText() + "\nFAILED: " + reason);
    }
  }

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
    return get(port, pathAndQuery, Map.of());
  }

  /**
   * A GET carrying explicit request headers, which is what {@code curl -H "Name: value"} ports to.
   */
  public HttpResult http(String pathAndQuery, Map<String, String> requestHeaders) {
    return get(cli.ports().solr(), pathAndQuery, requestHeaders);
  }

  /** As {@link #http(String, Map)} but against an explicit port. */
  public HttpResult http(int port, String pathAndQuery, Map<String, String> requestHeaders) {
    return get(port, pathAndQuery, requestHeaders);
  }

  /**
   * A GET carrying basic credentials: the plain-HTTP port of {@code curl -u user:password --basic},
   * as {@link HttpsRequest#basicAuth} is the TLS one.
   */
  public HttpResult http(String pathAndQuery, String user, String password) {
    return http(cli.ports().solr(), pathAndQuery, user, password);
  }

  /** As {@link #http(String, String, String)} but against an explicit port. */
  public HttpResult http(int port, String pathAndQuery, String user, String password) {
    return get(
        port,
        pathAndQuery,
        Map.of(
            "Authorization",
            "Basic "
                + Base64.getEncoder()
                    .encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8))));
  }

  /**
   * A POST carrying a text body, which is what {@code curl -X POST -H 'Content-type:<type>' -d
   * '<body>'} ports to — the shape every BATS line that modifies a schema, a config or an index
   * uses.
   */
  public HttpResult httpPost(String pathAndQuery, String contentType, String body) {
    return httpPost(cli.ports().solr(), pathAndQuery, contentType, body);
  }

  /** As {@link #httpPost(String, String, String)} but against an explicit port. */
  public HttpResult httpPost(int port, String pathAndQuery, String contentType, String body) {
    return send(
        "POST",
        port,
        pathAndQuery,
        Map.of("Content-Type", contentType),
        HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8),
        REQUEST_TIMEOUT);
  }

  /**
   * A PUT whose body is a file's bytes: the port of {@code curl --data-binary @<file> -X PUT}.
   *
   * <p>Given a longer timeout than the other requests, because what this uploads is a file rather
   * than a request a node answers from memory.
   */
  public HttpResult httpPut(String pathAndQuery, String contentType, Path file) {
    return httpPut(cli.ports().solr(), pathAndQuery, contentType, file);
  }

  /** As {@link #httpPut(String, String, Path)} but against an explicit port. */
  public HttpResult httpPut(int port, String pathAndQuery, String contentType, Path file) {
    HttpRequest.BodyPublisher body;
    try {
      body = HttpRequest.BodyPublishers.ofFile(file);
    } catch (IOException e) {
      throw new UncheckedIOException("Could not read " + file + " to upload it", e);
    }
    return send(
        "PUT", port, pathAndQuery, Map.of("Content-Type", contentType), body, UPLOAD_TIMEOUT);
  }

  /**
   * A {@code multipart/form-data} POST of one file: the port of {@code curl -F "field=@<file>"},
   * which is how a BATS line hands a binary document to a request handler.
   *
   * <p>Shares {@link #httpPut}'s longer timeout, for the same reason.
   */
  public HttpResult httpPostFile(String pathAndQuery, String fieldName, Path file) {
    return httpPostFile(cli.ports().solr(), pathAndQuery, fieldName, file);
  }

  /** As {@link #httpPostFile(String, String, Path)} but against an explicit port. */
  public HttpResult httpPostFile(int port, String pathAndQuery, String fieldName, Path file) {
    String boundary = "SolrShellHarness" + UUID.randomUUID();
    byte[] body;
    try {
      body = filePart(boundary, fieldName, file);
    } catch (IOException e) {
      throw new UncheckedIOException("Could not read " + file + " to upload it", e);
    }
    return send(
        "POST",
        port,
        pathAndQuery,
        Map.of("Content-Type", "multipart/form-data; boundary=" + boundary),
        HttpRequest.BodyPublishers.ofByteArray(body),
        UPLOAD_TIMEOUT);
  }

  /** One file part, named the way {@code curl -F} names it: the field, and the file's base name. */
  private static byte[] filePart(String boundary, String fieldName, Path file) throws IOException {
    ByteArrayOutputStream body = new ByteArrayOutputStream();
    body.write(
        ("--"
                + boundary
                + "\r\nContent-Disposition: form-data; name=\""
                + fieldName
                + "\"; filename=\""
                + file.getFileName()
                + "\"\r\nContent-Type: application/octet-stream\r\n\r\n")
            .getBytes(StandardCharsets.UTF_8));
    body.write(Files.readAllBytes(file));
    body.write(("\r\n--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8));
    return body.toByteArray();
  }

  private HttpResult get(int port, String pathAndQuery, Map<String, String> requestHeaders) {
    return send(
        "GET",
        port,
        pathAndQuery,
        requestHeaders,
        HttpRequest.BodyPublishers.noBody(),
        REQUEST_TIMEOUT);
  }

  private HttpResult send(
      String method,
      int port,
      String pathAndQuery,
      Map<String, String> requestHeaders,
      HttpRequest.BodyPublisher body,
      Duration requestTimeout) {
    String url = "http://localhost:" + port + pathAndQuery;
    try (HttpClient client =
        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
      HttpRequest.Builder request =
          HttpRequest.newBuilder(URI.create(url)).timeout(requestTimeout).method(method, body);
      requestHeaders.forEach(request::header);
      HttpResponse<byte[]> response =
          client.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
      return new HttpResult(
          response.statusCode(), decodeBody(response), response.headers(), cli.varsForAssertions());
    } catch (IOException e) {
      return new HttpResult(-1, e.toString(), cli.varsForAssertions());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted requesting " + url, e);
    }
  }

  /**
   * The response body as text, decompressing it first when the server compressed it — what {@code
   * curl --compressed} does, and the reason a request may ask for {@code Accept-Encoding} at all.
   * UTF-8 throughout, which is what Solr answers in.
   */
  private static String decodeBody(HttpResponse<byte[]> response) throws IOException {
    String encoding =
        response
            .headers()
            .firstValue("Content-Encoding")
            .orElse("")
            .trim()
            .toLowerCase(Locale.ROOT);
    byte[] bytes = response.body();
    if ("gzip".equals(encoding)) {
      try (InputStream in = new GZIPInputStream(new ByteArrayInputStream(bytes))) {
        bytes = in.readAllBytes();
      }
    } else if ("deflate".equals(encoding)) {
      try (InputStream in = new InflaterInputStream(new ByteArrayInputStream(bytes))) {
        bytes = in.readAllBytes();
      }
    }
    return new String(bytes, StandardCharsets.UTF_8);
  }

  /**
   * A GET against {@code https://localhost:<SOLR_PORT><pathAndQuery>}, as a builder because TLS
   * needs the client-side options {@code curl} spells {@code --cacert}, {@code -u} and {@code
   * --resolve}'s host.
   */
  public HttpsRequest https(String pathAndQuery) {
    return https(cli.ports().solr(), pathAndQuery);
  }

  /** As {@link #https(String)} but against an explicit port. */
  public HttpsRequest https(int port, String pathAndQuery) {
    return new HttpsRequest(port, pathAndQuery, cli.varsForAssertions());
  }

  /** Whether the named collection is in ZooKeeper. Port of {@code collection_exists}. */
  public boolean collectionExists(String collection) {
    return zkChildren("/collections").contains(collection);
  }

  /** Whether the named config set is in ZooKeeper. Port of {@code config_exists}. */
  public boolean configExists(String configName) {
    return zkChildren("/configs").contains(configName);
  }

  /**
   * The child node names under a ZooKeeper path.
   *
   * <p>Reads stdout alone, then keeps only lines that could be a node name. Both filters earn their
   * place: addressing ZooKeeper directly with {@code -z} makes SolrZkClient log a WARN about
   * default ACLs, and the BATS helper this replaces fed exactly that line back into {@code solr
   * delete}. BATS never noticed, because it discarded the delete's output.
   */
  private List<String> zkChildren(String zkPath) {
    CommandResult result = cli.runSeparated("solr zk ls " + zkPath + " -z localhost:${ZK_PORT}");
    if (result.failed()) {
      return List.of();
    }
    return result
        .stdout()
        .lines()
        .map(String::trim)
        // A ZooKeeper node name carries no whitespace, while every log line does.
        .filter(line -> !line.isEmpty() && line.chars().noneMatch(Character::isWhitespace))
        .toList();
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
