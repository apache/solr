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
package org.apache.solr.opentelemetry;

import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.RetryUtil;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.tracing.TraceUtils;
import org.junit.Assert;

/**
 * Verifies collected OpenTelemetry spans against a committed gold file.
 *
 * <p>Spans are normalized (IDs/timestamps stripped, URLs scrubbed) and arranged into a tree by
 * parent-child relationships. The resulting JSON is compared against a gold file committed in
 * {@code src/test-files/solr/tracing/}.
 *
 * <h2>Usage</h2>
 *
 * <pre>
 * var verifier = new GoldFileTraceVerifier("testV2Api");
 * // ... perform operations ...
 * verifier.verifyPhase(); // waits for spans, normalizes, compares
 * // ... more operations ...
 * verifier.verifyPhase();
 * verifier.done();
 * </pre>
 *
 * <h2>Regenerating Gold Files</h2>
 *
 * <p>Requires {@code -Ptests.useSecurityManager=false} since it writes to the source tree.
 *
 * <pre>
 * gradlew :solr:modules:opentelemetry:test --tests TestDistributedTracing.testV2Api \
 *   "-Ptests.jvmargs=-Dregenerate.golden.files=true" -Ptests.useSecurityManager=false
 * </pre>
 *
 * <h2>Dumping Raw Traces (no normalization, all attributes)</h2>
 *
 * <p>Also requires {@code -Ptests.useSecurityManager=false} (writes to source tree). Dump files are
 * written alongside gold files as {@code {testMethod}-phase{N}-dump.json}. Output paths are logged
 * to stdout (visible in {@code build/test-results/test/outputs/OUTPUT-*.txt}).
 *
 * <pre>
 * gradlew :solr:modules:opentelemetry:test --tests TestDistributedTracing.testV2Api \
 *   "-Ptests.jvmargs=-Ddump.traces=true" -Ptests.useSecurityManager=false
 * </pre>
 */
public class GoldFileTraceVerifier {

  private static final boolean REGENERATE = Boolean.getBoolean("regenerate.golden.files");
  private static final boolean DUMP_TRACES = Boolean.getBoolean("dump.traces");
  private static final Pattern URL_HOST_PORT = Pattern.compile("https?://[^/]+/");
  private static final Pattern REPLICA_SUFFIX = Pattern.compile("_replica_n\\d+");
  private static final Pattern ENCODED_URL_IN_PARAMS = Pattern.compile("https?%3A%2F%2F[^&=]+");

  private static final Comparator<Map<String, Object>> SPAN_COMPARATOR =
      Comparator.comparing((Map<String, Object> s) -> (String) s.get("name"))
          .thenComparing(s -> s.getOrDefault(TraceUtils.TAG_DB.getKey(), "").toString());

  private final String testName;
  private final Path goldFilePath;
  private final Map<String, Object> goldFile; // parsed gold file (phases list)
  private final List<List<Map<String, Object>>> recordedPhases = new ArrayList<>();
  private int currentPhaseIndex = 0;

  public GoldFileTraceVerifier(Class<?> testClass, String testMethodName) {
    this.testName = testMethodName;
    this.goldFilePath = resolveGoldFilePath(testClass, testMethodName);
    if (REGENERATE) {
      this.goldFile = null;
    } else {
      this.goldFile = loadGoldFile(goldFilePath);
    }
  }

  /**
   * Waits for the expected number of spans (from the gold file), collects them, normalizes into a
   * tree, and compares against the gold file's current phase.
   */
  @SuppressWarnings("unchecked")
  public void verifyPhase() {
    InMemorySpanExporter exporter = CustomTestOtelTracerConfigurator.getInMemorySpanExporter();

    List<SpanData> spans;
    if (REGENERATE) {
      spans = waitForStableSpans(exporter);
    } else {
      List<Map<String, Object>> phases = (List<Map<String, Object>>) goldFile.get("phases");
      Map<String, Object> phase = phases.get(currentPhaseIndex);
      List<Map<String, Object>> expectedSpans = (List<Map<String, Object>>) phase.get("spans");
      int expectedCount = countSpansRecursive(expectedSpans);
      spans = waitForSpans(exporter, expectedCount);
    }

    exporter.reset();

    if (DUMP_TRACES) {
      dumpRawSpans(spans);
    }

    List<Map<String, Object>> tree = buildTree(spans);
    recordedPhases.add(tree);

    if (!REGENERATE) {
      comparePhase(currentPhaseIndex, tree);
    }
    currentPhaseIndex++;
  }

  /** Call after all phases are verified. In regenerate mode, writes the gold file. */
  @SuppressWarnings("unchecked")
  public void done() {
    if (REGENERATE) {
      writeGoldFile();
    } else {
      List<Map<String, Object>> phases = (List<Map<String, Object>>) goldFile.get("phases");
      Assert.assertEquals(
          "Unverified phases remain in gold file for " + testName,
          phases.size(),
          currentPhaseIndex);
    }
  }

  // --- Span collection ---

  private List<SpanData> waitForSpans(InMemorySpanExporter exporter, int expectedCount) {
    try {
      RetryUtil.retryUntil(
          "Timed out waiting for " + expectedCount + " span(s) in phase " + currentPhaseIndex,
          500,
          20,
          TimeUnit.MILLISECONDS,
          () -> exporter.getFinishedSpanItems().size() >= expectedCount);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return new ArrayList<>(exporter.getFinishedSpanItems());
  }

  private List<SpanData> waitForStableSpans(InMemorySpanExporter exporter) {
    try {
      RetryUtil.retryUntil(
          "Timed out waiting for any spans in phase " + currentPhaseIndex,
          500,
          20,
          TimeUnit.MILLISECONDS,
          () -> !exporter.getFinishedSpanItems().isEmpty());
      int lastCount = -1;
      for (int i = 0; i < 10; i++) {
        TimeUnit.MILLISECONDS.sleep(500);
        int current = exporter.getFinishedSpanItems().size();
        if (current == lastCount) {
          break;
        }
        lastCount = current;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return new ArrayList<>(exporter.getFinishedSpanItems());
  }

  // --- Raw dump (for debugging, no normalization) ---

  private void dumpRawSpans(List<SpanData> spans) {
    // Sort by start timestamp for a chronological view
    List<SpanData> sorted = new ArrayList<>(spans);
    sorted.sort(Comparator.comparingLong(SpanData::getStartEpochNanos));

    // Build tree from raw data (no normalization)
    Map<String, Map<String, Object>> nodesBySpanId = new HashMap<>();
    List<Map<String, Object>> roots = new ArrayList<>();

    for (SpanData span : sorted) {
      Map<String, Object> node = new LinkedHashMap<>();
      node.put("name", span.getName());
      node.put("kind", span.getKind().name());
      node.put("traceId", span.getSpanContext().getTraceId());
      node.put("spanId", span.getSpanContext().getSpanId());
      if (span.getParentSpanContext().isValid()) {
        node.put("parentSpanId", span.getParentSpanContext().getSpanId());
      }
      node.put("startEpochNanos", span.getStartEpochNanos());
      node.put("endEpochNanos", span.getEndEpochNanos());
      // All attributes
      Map<String, Object> attrs = new LinkedHashMap<>();
      span.getAttributes().forEach((key, value) -> attrs.put(key.getKey(), value));
      if (!attrs.isEmpty()) {
        node.put("attributes", attrs);
      }
      nodesBySpanId.put(span.getSpanContext().getSpanId(), node);
    }

    for (SpanData span : sorted) {
      Map<String, Object> node = nodesBySpanId.get(span.getSpanContext().getSpanId());
      String parentId = span.getParentSpanContext().getSpanId();
      if (span.getParentSpanContext().isValid() && nodesBySpanId.containsKey(parentId)) {
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> children =
            (List<Map<String, Object>>)
                nodesBySpanId.get(parentId).computeIfAbsent("children", k -> new ArrayList<>());
        children.add(node);
      } else {
        roots.add(node);
      }
    }

    String json = Utils.toJSONString(roots, 2);
    Path dumpDir = goldFilePath.getParent();
    Path dumpFile = dumpDir.resolve(testName + "-phase" + currentPhaseIndex + "-dump.json");
    try {
      Files.createDirectories(dumpDir);
      Files.writeString(dumpFile, json);
      System.out.println("Dumped raw traces: " + dumpFile.toAbsolutePath());
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write trace dump", e);
    }
  }

  // --- Tree building ---

  private List<Map<String, Object>> buildTree(List<SpanData> spans) {
    Map<String, Map<String, Object>> nodesBySpanId = new HashMap<>();
    List<Map<String, Object>> roots = new ArrayList<>();

    for (SpanData span : spans) {
      nodesBySpanId.put(span.getSpanContext().getSpanId(), normalize(span));
    }

    for (SpanData span : spans) {
      Map<String, Object> node = nodesBySpanId.get(span.getSpanContext().getSpanId());
      String parentId = span.getParentSpanContext().getSpanId();
      if (span.getParentSpanContext().isValid() && nodesBySpanId.containsKey(parentId)) {
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> children =
            (List<Map<String, Object>>) nodesBySpanId.get(parentId).get("children");
        if (children == null) {
          children = new ArrayList<>();
          nodesBySpanId.get(parentId).put("children", children);
        }
        children.add(node);
      } else {
        roots.add(node);
      }
    }

    sortTree(roots);
    return roots;
  }

  @SuppressWarnings("unchecked")
  private void sortTree(List<Map<String, Object>> nodes) {
    nodes.sort(SPAN_COMPARATOR);
    for (Map<String, Object> node : nodes) {
      List<Map<String, Object>> children = (List<Map<String, Object>>) node.get("children");
      if (children != null && !children.isEmpty()) {
        sortTree(children);
      }
    }
  }

  private Map<String, Object> normalize(SpanData span) {
    // Use LinkedHashMap for consistent key ordering in JSON output
    Map<String, Object> node = new LinkedHashMap<>();
    node.put("name", span.getName());
    if (span.getKind() != SpanKind.INTERNAL) {
      node.put("kind", span.getKind().name());
    }

    String dbInstance = normalizeDbInstance(span.getAttributes().get(TraceUtils.TAG_DB));
    if (dbInstance != null) {
      node.put(TraceUtils.TAG_DB.getKey(), dbInstance);
    }

    String dbType = span.getAttributes().get(TraceUtils.TAG_DB_TYPE);
    if (dbType != null) {
      node.put(TraceUtils.TAG_DB_TYPE.getKey(), dbType);
    }

    String httpMethod = span.getAttributes().get(TraceUtils.TAG_HTTP_METHOD);
    if (httpMethod != null) {
      node.put(TraceUtils.TAG_HTTP_METHOD.getKey(), httpMethod);
    }

    Long statusCode = span.getAttributes().get(TraceUtils.TAG_HTTP_STATUS);
    if (statusCode != null) {
      node.put(TraceUtils.TAG_HTTP_STATUS.getKey(), statusCode.intValue());
    }

    String httpUrl = span.getAttributes().get(TraceUtils.TAG_HTTP_URL);
    if (httpUrl != null) {
      node.put(TraceUtils.TAG_HTTP_URL.getKey(), normalizeUrl(httpUrl));
    }

    String httpParams = span.getAttributes().get(TraceUtils.TAG_HTTP_PARAMS);
    if (httpParams != null) {
      node.put(TraceUtils.TAG_HTTP_PARAMS.getKey(), normalizeParams(httpParams));
    }

    // children will be added later during tree building if needed
    return node;
  }

  private String normalizeUrl(String url) {
    url = URL_HOST_PORT.matcher(url).replaceFirst("http://NORMALIZED/");
    url = REPLICA_SUFFIX.matcher(url).replaceAll("_replica_nN");
    return url;
  }

  private String normalizeParams(String params) {
    // Scrub embedded URLs (distrib.from=http%3A%2F%2F...) that contain ports
    params = ENCODED_URL_IN_PARAMS.matcher(params).replaceAll("http%3A%2F%2FNORMALIZED");
    String[] parts = params.split("&");
    Arrays.sort(parts);
    return String.join("&", parts);
  }

  private String normalizeDbInstance(String dbInstance) {
    if (dbInstance == null) return null;
    return REPLICA_SUFFIX.matcher(dbInstance).replaceAll("_replica_nN");
  }

  // --- Comparison ---

  @SuppressWarnings("unchecked")
  private void comparePhase(int phaseIndex, List<Map<String, Object>> actual) {
    List<Map<String, Object>> phases = (List<Map<String, Object>>) goldFile.get("phases");
    Map<String, Object> phase = phases.get(phaseIndex);
    List<Map<String, Object>> expectedSpans = (List<Map<String, Object>>) phase.get("spans");
    String description = (String) phase.get("description");

    String expectedJson = Utils.toJSONString(expectedSpans, 2);
    String actualJson = Utils.toJSONString(actual, 2);

    if (!expectedJson.equals(actualJson)) {
      Path tempFile = writeTempJson(Utils.toJSONString(actual, 2), phaseIndex);
      String message =
          String.format(
              Locale.getDefault(),
              """

              Trace spans mismatch in phase %d%s

              Expected (gold file):
                %s

              Actual output (written to temp file):
                %s

              To compare:
                diff %s %s

              If intentional, regenerate:
                gradlew :solr:modules:opentelemetry:test --tests TestDistributedTracing.%s "-Ptests.jvmargs=-Dregenerate.golden.files=true" -Ptests.useSecurityManager=false
              """,
              phaseIndex,
              description != null ? " \"" + description + "\"" : "",
              goldFilePath.toAbsolutePath(),
              tempFile.toAbsolutePath(),
              goldFilePath.toAbsolutePath(),
              tempFile.toAbsolutePath(),
              testName);
      Assert.assertEquals(message, expectedJson, actualJson);
    }
  }

  @SuppressWarnings("unchecked")
  private int countSpansRecursive(List<Map<String, Object>> nodes) {
    int count = 0;
    for (Map<String, Object> node : nodes) {
      count++;
      List<Map<String, Object>> children = (List<Map<String, Object>>) node.get("children");
      if (children != null && !children.isEmpty()) {
        count += countSpansRecursive(children);
      }
    }
    return count;
  }

  // --- Gold file I/O ---

  private static Path resolveGoldFilePath(Class<?> testClass, String testMethodName) {
    // Gold files live at: {test-files}/solr/tracing/{TestClassName}/{testMethod}.json
    // SolrTestCaseJ4.getFile() may resolve to the build output; walk up to find src/test-files.
    Path solrDir = SolrTestCaseJ4.getFile("solr");
    Path testFilesDir = solrDir.getParent();
    // If we're in build output (e.g. build/resources/test/solr), find the source equivalent
    String testFilesStr = testFilesDir.toString();
    if (testFilesStr.contains("/build/")) {
      testFilesDir =
          Path.of(testFilesStr.substring(0, testFilesStr.indexOf("/build/")))
              .resolve("src/test-files");
    }
    return testFilesDir
        .resolve("solr/tracing")
        .resolve(testClass.getSimpleName())
        .resolve(testMethodName + ".json");
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> loadGoldFile(Path path) {
    if (!Files.exists(path)) {
      Assert.fail(
          "Gold file not found: "
              + path.toAbsolutePath()
              + "\nRegenerate with: gradlew ... \"-Ptests.jvmargs=-Dregenerate.golden.files=true\" -Ptests.useSecurityManager=false");
    }
    try {
      byte[] bytes = Files.readAllBytes(path);
      return (Map<String, Object>) Utils.fromJSON(bytes);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read gold file: " + path, e);
    }
  }

  private void writeGoldFile() {
    Map<String, Object> output = new LinkedHashMap<>();
    List<Map<String, Object>> phases = new ArrayList<>();
    for (int i = 0; i < recordedPhases.size(); i++) {
      Map<String, Object> phase = new LinkedHashMap<>();
      phase.put("description", "phase " + i);
      phase.put("spans", recordedPhases.get(i));
      phases.add(phase);
    }
    output.put("phases", phases);
    try {
      Files.createDirectories(goldFilePath.getParent());
      String json = Utils.toJSONString(output, 2);
      Files.writeString(goldFilePath, json);
      System.out.println("Regenerated gold file: " + goldFilePath.toAbsolutePath());
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write gold file: " + goldFilePath, e);
    }
  }

  private Path writeTempJson(String json, int phaseIndex) {
    try {
      Path tempFile =
          Path.of(System.getProperty("java.io.tmpdir"), testName + "-phase" + phaseIndex + ".json");
      Files.writeString(tempFile, json);
      return tempFile;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write temp file", e);
    }
  }
}
