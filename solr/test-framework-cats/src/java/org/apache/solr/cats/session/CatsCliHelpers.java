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

package org.apache.solr.cats.session;

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
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;
import org.apache.solr.cats.env.CatsHarnessEnv;
import org.apache.solr.cats.exec.CommandResult;
import org.apache.solr.cats.exec.Shell;
import org.apache.solr.cats.exec.ShellAssertionError;
import org.apache.solr.cats.exec.Variables;

/**
 * The conveniences a shell test reaches for beyond running a command: HTTP requests against the
 * program under test, and the tail of a log file.
 *
 * <p>Knows nothing of what the program is. A project extends this with the helpers that do — its
 * own resources, its administrative endpoints, a graceful shutdown — and the session hands back
 * whichever it was given.
 *
 * <p>Reached through the session that owns it, or directly as inherited shorthands on a test case.
 */
public class CatsCliHelpers {

  /** How long a request the program answers from memory may take. */
  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

  /** How long a request whose body is a file may take, which is bounded by its size. */
  private static final Duration UPLOAD_TIMEOUT = Duration.ofMinutes(5);

  private final CatsHarnessEnv harnessEnv;
  private final Shell shell;
  private final Transcript transcript;
  private final Map<String, String> vars;

  /**
   * @param harnessEnv where the ports and the echoed variable prefixes come from
   * @param shell the shell a transcript line is quoted for
   * @param transcript where a request is recorded as the {@code curl} that reproduces it
   * @param vars a live view of the session's variables, for resolving {@code ${NAME}} in an
   *     assertion's expected text
   */
  public CatsCliHelpers(
      CatsHarnessEnv harnessEnv, Shell shell, Transcript transcript, Map<String, String> vars) {
    this.harnessEnv = harnessEnv;
    this.shell = shell;
    this.transcript = transcript;
    this.vars = vars;
  }

  /**
   * The port a request goes to when it names none: the one the layout marks primary.
   *
   * <p>A program that listens on nothing, or on several ports with none of them primary, has no
   * answer here and must be asked for an explicit port instead.
   */
  private int defaultPort() {
    int primary = harnessEnv.portLayout().primary();
    if (primary < 0) {
      throw new IllegalStateException(
          "The program under test declares no primary port, so a request must name the port");
    }
    return harnessEnv.ports().at(primary);
  }

  /**
   * The response to a harness HTTP request, with the same shape of fluent assertions a command
   * result has.
   *
   * <p>An HTTP request in a shell test is almost always followed by an assertion on the body, so
   * this carries those assertions rather than returning bare text.
   */
  public record HttpResult(
      int status,
      String body,
      HttpHeaders headers,
      Map<String, String> vars,
      Set<String> echoedPrefixes) {

    /** A result with no response headers, for a request that never completed. */
    public HttpResult(
        int status, String body, Map<String, String> vars, Set<String> echoedPrefixes) {
      this(status, body, HttpHeaders.of(Map.of(), (name, value) -> true), vars, echoedPrefixes);
    }

    /**
     * Resolves {@code ${NAME}} in an assertion's expected text, exactly as {@link CommandResult}
     * does: an assertion on {@code ${SOME_PORT}} must either work or fail loudly, never pass by
     * comparing against the unresolved text.
     */
    private String resolve(String expected) {
      return Variables.resolve(expected, Variables.ANY, vars, "an HTTP assertion", echoedPrefixes);
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
     * so the shape an assertion on a header is written against.
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
     * client here speaks HTTP/2 where they arrive lowercased, while an assertion is usually written
     * in curl's HTTP/1.1 {@code Cache-Control: no-cache} spelling.
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
   * A GET against {@code http://localhost:<primary port><pathAndQuery>}.
   *
   * <p>Stands in for a {@code curl} call, which is most of what makes readiness checks portable:
   * there is no curl on a stock Windows runner.
   */
  public HttpResult http(String pathAndQuery) {
    return http(defaultPort(), pathAndQuery);
  }

  /** A GET against an explicit port on localhost. */
  public HttpResult http(int port, String pathAndQuery) {
    return get(port, pathAndQuery, Map.of());
  }

  /**
   * A GET carrying explicit request headers, which is what {@code curl -H "Name: value"} ports to.
   */
  public HttpResult http(String pathAndQuery, Map<String, String> requestHeaders) {
    return get(defaultPort(), pathAndQuery, requestHeaders);
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
    return http(defaultPort(), pathAndQuery, user, password);
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
   * '<body>'} ports to — the shape a shell line that modifies state uses.
   */
  public HttpResult httpPost(String pathAndQuery, String contentType, String body) {
    return httpPost(defaultPort(), pathAndQuery, contentType, body);
  }

  /** As {@link #httpPost(String, String, String)} but against an explicit port. */
  public HttpResult httpPost(int port, String pathAndQuery, String contentType, String body) {
    return send(
        "POST",
        port,
        pathAndQuery,
        Map.of("Content-Type", contentType),
        HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8),
        REQUEST_TIMEOUT,
        "--data " + shell.quoteForDisplay(body));
  }

  /**
   * A PUT whose body is a file's bytes: the port of {@code curl --data-binary @<file> -X PUT}.
   *
   * <p>Given a longer timeout than the other requests, because what this uploads is a file rather
   * than a request a node answers from memory.
   */
  public HttpResult httpPut(String pathAndQuery, String contentType, Path file) {
    return httpPut(defaultPort(), pathAndQuery, contentType, file);
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
        "PUT",
        port,
        pathAndQuery,
        Map.of("Content-Type", contentType),
        body,
        UPLOAD_TIMEOUT,
        "--data-binary @" + shell.quoteForDisplay(file.toString()));
  }

  /**
   * A {@code multipart/form-data} POST of one file: the port of {@code curl -F "field=@<file>"},
   * which is how a shell line hands a binary document to a request handler.
   *
   * <p>Shares {@link #httpPut}'s longer timeout, for the same reason.
   */
  public HttpResult httpPostFile(String pathAndQuery, String fieldName, Path file) {
    return httpPostFile(defaultPort(), pathAndQuery, fieldName, file);
  }

  /** As {@link #httpPostFile(String, String, Path)} but against an explicit port. */
  public HttpResult httpPostFile(int port, String pathAndQuery, String fieldName, Path file) {
    String boundary = "CatsHarness" + UUID.randomUUID();
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
        UPLOAD_TIMEOUT,
        "-F " + shell.quoteForDisplay(fieldName + "=@" + file));
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
        REQUEST_TIMEOUT,
        null);
  }

  private HttpResult send(
      String method,
      int port,
      String pathAndQuery,
      Map<String, String> requestHeaders,
      HttpRequest.BodyPublisher body,
      Duration requestTimeout,
      String curlBodyArg) {
    String url = "http://localhost:" + port + pathAndQuery;
    String curl = curl(shell, method, url, requestHeaders, curlBodyArg);
    try (HttpClient client =
        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
      HttpRequest.Builder request =
          HttpRequest.newBuilder(URI.create(url)).timeout(requestTimeout).method(method, body);
      requestHeaders.forEach(request::header);
      HttpResponse<byte[]> response =
          client.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
      String decoded = decodeBody(response);
      transcript.recordHttp(curl, response.statusCode(), decoded);
      return new HttpResult(
          response.statusCode(), decoded, response.headers(), vars, harnessEnv.echoedPrefixes());
    } catch (IOException e) {
      transcript.recordHttp(curl, -1, e.toString());
      return new HttpResult(-1, e.toString(), vars, harnessEnv.echoedPrefixes());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted requesting " + url, e);
    }
  }

  /**
   * The {@code curl} that reproduces a request, for the transcript. Quoted bash-style because it is
   * there for a reader to paste, not for the harness to run — and every variable is already
   * resolved by the time it is built, so what the transcript shows is the real request.
   */
  static String curl(
      Shell shell,
      String method,
      String url,
      Map<String, String> requestHeaders,
      String curlBodyArg) {
    StringBuilder curl = new StringBuilder("curl");
    if (!"GET".equals(method)) {
      curl.append(" -X ").append(method);
    }
    requestHeaders.forEach(
        (name, value) -> {
          // Not the multipart content type: curl derives the boundary from -F itself, and pasting
          // ours back would send one the body no longer uses.
          if (!value.startsWith("multipart/form-data")) {
            curl.append(" -H ").append(shell.quoteForDisplay(name + ": " + value));
          }
        });
    if (curlBodyArg != null) {
      curl.append(' ').append(curlBodyArg);
    }
    return curl.append(' ').append(shell.quoteForDisplay(url)).toString();
  }

  /**
   * The response body as text, decompressing it first when the server compressed it — what {@code
   * curl --compressed} does, and the reason a request may ask for {@code Accept-Encoding} at all.
   * UTF-8 throughout.
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
   * A GET against {@code https://localhost:<primary port><pathAndQuery>}, as a builder because TLS
   * needs the client-side options {@code curl} spells {@code --cacert}, {@code -u} and {@code
   * --resolve}'s host.
   */
  public HttpsRequest https(String pathAndQuery) {
    return https(defaultPort(), pathAndQuery);
  }

  /** As {@link #https(String)} but against an explicit port. */
  public HttpsRequest https(int port, String pathAndQuery) {
    return new HttpsRequest(
        port, pathAndQuery, vars, harnessEnv.echoedPrefixes(), transcript, shell);
  }

  /**
   * Prints the last {@code lines} lines of a file to stderr, for debugging a failing test.
   *
   * <p>A missing file is reported rather than thrown: this runs while a test is already failing,
   * and the reason is what the reader needs, not a second failure.
   */
  public void tailFile(Path file, int lines) {
    if (!Files.isRegularFile(file)) {
      System.err.println("No file at " + file);
      return;
    }
    try {
      List<String> all = Files.readAllLines(file, StandardCharsets.UTF_8);
      System.err.println("--- tail of " + file + " ---");
      all.subList(Math.max(0, all.size() - lines), all.size()).forEach(System.err::println);
    } catch (IOException e) {
      System.err.println("Could not read " + file + ": " + e);
    }
  }
}
