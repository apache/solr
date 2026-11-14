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

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.SimpleFileServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.solr.cats.SuppressForbidden;

/**
 * The network side of a session: fixture web servers and file downloads, both in Java rather than
 * through the shell, since neither curl nor wget is on a stock Windows runner.
 *
 * <p>Relative paths are resolved by the session that owns this instance, so they mean the same
 * thing here as in a command line.
 */
public class CatsNet {

  /** Timeout for {@link #download}, which fetches from the network rather than from a program. */
  private static final Duration DOWNLOAD_TIMEOUT = Duration.ofMinutes(10);

  private final Transcript transcript;
  private final Function<String, Path> resolver;
  private final Map<Integer, HttpServer> fileServers = new LinkedHashMap<>();

  public CatsNet(Transcript transcript, Function<String, Path> resolver) {
    this.transcript = transcript;
    this.resolver = resolver;
  }

  /**
   * Serves a directory over HTTP on a free port, and returns the port.
   *
   * <p>For a test whose subject is the program under test reading a URL — a remote config, a module
   * fetching a resource — without depending on the network or on a real site's current content. The
   * port is an ephemeral one the OS picks rather than one from the session's block: nothing needs
   * to predict it, and it does not spend one of the ports a class owns.
   *
   * <p>Every server a scope starts is stopped when the scope ends. {@link #serveHttpShutdown(int)}
   * stops one early, for a test that wants to observe a fetch failing.
   *
   * @param webRoot the directory to serve, resolved against the session's working directory
   * @return the port it is listening on
   */
  @SuppressForbidden(reason = "com.sun.net.httpserver is where the JDK's own web server lives")
  public int serveHttp(String webRoot) {
    Path root = resolver.apply(webRoot);
    if (!Files.isDirectory(root)) {
      throw new AssertionError("serveHttp: not a directory: " + root);
    }
    // The JDK's own file server, JEP 408. Nothing to get wrong here: it maps the path, picks the
    // content type, serves index.html for a directory and refuses anything outside the root.
    // Bound to loopback — a fixture has no business being reachable from off the machine — and to
    // port 0, meaning whatever is free.
    HttpServer server =
        SimpleFileServer.createFileServer(
            new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
            root.toAbsolutePath(),
            SimpleFileServer.OutputLevel.NONE);
    server.start();
    int port = server.getAddress().getPort();
    fileServers.put(port, server);
    transcript.note("serving " + root + " at http://localhost:" + port);
    return port;
  }

  /** Stops one server started by {@link #serveHttp}. Stopping one twice is not an error. */
  @SuppressForbidden(reason = "com.sun.net.httpserver is where the JDK's own web server lives")
  public void serveHttpShutdown(int port) {
    HttpServer server = fileServers.remove(port);
    if (server != null) {
      server.stop(0);
    }
  }

  /** The ports of the servers still running, in the order they were started. */
  public List<Integer> servingPorts() {
    return List.copyOf(fileServers.keySet());
  }

  /**
   * Port of {@code curl -o <path> <url>}: fetches a URL into a file, creating parent directories.
   *
   * <p>In Java because neither curl nor wget is on a stock Windows runner. A non-2xx response, or a
   * transport failure, fails the test rather than leaving a truncated file behind.
   *
   * @return the absolute path of the downloaded file
   */
  public Path download(String url, String path) {
    Path file = resolver.apply(path);
    try (HttpClient client =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build()) {
      Files.createDirectories(file.getParent());
      HttpRequest request =
          HttpRequest.newBuilder(URI.create(url)).timeout(DOWNLOAD_TIMEOUT).GET().build();
      HttpResponse<Path> response =
          client.send(
              request,
              HttpResponse.BodyHandlers.ofFile(
                  file,
                  StandardOpenOption.CREATE,
                  StandardOpenOption.WRITE,
                  StandardOpenOption.TRUNCATE_EXISTING));
      if (response.statusCode() / 100 != 2) {
        throw new AssertionError(
            "Downloading " + url + " answered HTTP " + response.statusCode() + ", not 2xx");
      }
      return file;
    } catch (IOException e) {
      throw new UncheckedIOException("Could not download " + url + " to " + file, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted downloading " + url, e);
    }
  }
}
