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
package org.apache.solr.handler.loader;

import static org.apache.solr.common.params.CommonParams.JSON;

import java.io.BufferedReader;
import java.io.FilterReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.eclipse.jetty.http.MimeTypes;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;

/**
 * Loads documents in <a href="https://ndjson.org/">Newline Delimited JSON</a> (ND-JSON, also known
 * as JSON Lines or JSONL) format; one JSON object per line, each representing a document to add.
 *
 * <p>The format is defined as UTF-8, so a request declaring any other charset is rejected; a
 * request that declares none is read as UTF-8.
 *
 * <p>Documents are parsed and indexed one line at a time, so peak memory depends on the longest
 * line rather than on the size of the input. A single line is bounded by {@link
 * #MAX_LINE_LENGTH_PROP}, defaulting to a fraction of the heap, so that input which is not really
 * newline delimited fails instead of exhausting the heap. Unlike {@link JsonLoader}, update
 * commands such as {@code delete} or {@code commit} are not recognized; use request parameters or a
 * separate request for those. As on the {@code /update/json/docs} path, a nested JSON object is
 * always a child document, so atomic updates are not expressible in this format.
 */
public class NDJsonLoader extends ContentStreamLoader {

  /** The content types that select this loader. */
  public static final Set<String> CONTENT_TYPES =
      Set.of("application/x-ndjson", "application/jsonl", "application/x-jsonlines");

  /** System property setting the characters a single line may span, for all update handlers. */
  public static final String MAX_LINE_LENGTH_PROP = "solr.ndjson.maxLineLength";

  /** Smallest default line budget, so that a tiny heap still accepts ordinary documents. */
  public static final int MIN_DEFAULT_MAX_LINE_LENGTH = 1024 * 1024;

  /**
   * Parsing a line costs roughly this many heap bytes per character, dominated by the doubling of
   * the reader's line buffer plus the parsed document.
   */
  private static final int HEAP_BYTES_PER_CHAR = 8;

  /** Share of the heap a single line may occupy while being parsed. */
  private static final int HEAP_FRACTION = 4;

  private int maxLineLength =
      EnvUtils.getPropertyAsInteger(MAX_LINE_LENGTH_PROP, defaultMaxLineLength());

  /**
   * The line budget to use when nothing is configured: enough characters to occupy {@code 1 /
   * HEAP_FRACTION} of the heap while parsing. This is a backstop against input that is not really
   * newline delimited, not a limit on how large a document may be, so it is deliberately generous.
   */
  public static int defaultMaxLineLength() {
    long fromHeap = Runtime.getRuntime().maxMemory() / HEAP_FRACTION / HEAP_BYTES_PER_CHAR;
    return Math.clamp(fromHeap, MIN_DEFAULT_MAX_LINE_LENGTH, Integer.MAX_VALUE);
  }

  @Override
  public ContentStreamLoader init(SolrParams args) {
    if (args != null) {
      maxLineLength = args.getInt("maxLineLength", maxLineLength);
    }
    return this;
  }

  @Override
  public String getDefaultWT() {
    return JSON;
  }

  @Override
  public void load(
      SolrQueryRequest req,
      SolrQueryResponse rsp,
      ContentStream stream,
      UpdateRequestProcessor processor)
      throws Exception {
    assertUtf8(stream);
    final int commitWithin = req.getParams().getInt(UpdateParams.COMMIT_WITHIN, -1);
    final boolean overwrite = req.getParams().getBool(UpdateParams.OVERWRITE, true);

    long lineNumber = 0;
    // Reused across lines; JSONParser(String) would copy each line into a fresh array instead
    char[] chars = new char[512];
    // Decoded here rather than via stream.getReader(), since the charset is known to be UTF-8
    try (BufferedReader reader =
        new BufferedReader(
            new LineLengthGuard(
                new InputStreamReader(stream.getStream(), StandardCharsets.UTF_8),
                maxLineLength))) {
      String line;
      while ((line = reader.readLine()) != null) {
        lineNumber++;
        if (line.isBlank()) {
          continue;
        }
        int length = line.length();
        if (length > chars.length) {
          chars = new char[length];
        }
        line.getChars(0, length, chars, 0);

        AddUpdateCommand cmd = new AddUpdateCommand(req);
        cmd.commitWithin = commitWithin;
        cmd.overwrite = overwrite;
        cmd.solrDoc = JsonLoader.buildDoc(parseLine(chars, length, lineNumber));
        processor.processAdd(cmd);
      }
    }
  }

  /**
   * NDJSON is defined as UTF-8, so a stream declaring any other charset is rejected rather than
   * silently decoded. A stream that declares no charset is read as UTF-8.
   */
  private static void assertUtf8(ContentStream stream) {
    String charset = MimeTypes.getCharsetFromContentType(stream.getContentType());
    if (charset != null && !StandardCharsets.UTF_8.equals(Charset.forName(charset, null))) {
      throw new SolrException(
          SolrException.ErrorCode.UNSUPPORTED_MEDIA_TYPE,
          "NDJSON must be UTF-8 encoded, but charset=" + charset + " was declared");
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> parseLine(char[] line, int length, long lineNumber) {
    final Object parsed;
    final JSONParser parser;
    try {
      parser = new JSONParser(line, 0, length);
      parsed = ObjectBuilder.getVal(parser);
    } catch (JSONParser.ParseException | IOException e) {
      throw bad(lineNumber, e.getMessage());
    }
    if (!(parsed instanceof Map)) {
      throw bad(lineNumber, "expected a JSON object");
    }
    try {
      if (parser.nextEvent() != JSONParser.EOF) {
        throw bad(lineNumber, "expected exactly one JSON object per line");
      }
    } catch (JSONParser.ParseException | IOException e) {
      throw bad(lineNumber, e.getMessage());
    }
    return (Map<String, Object>) parsed;
  }

  private static SolrException bad(long lineNumber, String detail) {
    return new SolrException(
        SolrException.ErrorCode.BAD_REQUEST,
        "Cannot parse NDJSON at line " + lineNumber + ": " + detail);
  }

  /**
   * Bounds how far the underlying reader may go without a line terminator, so that input which is
   * not in fact newline delimited is rejected instead of being buffered into one huge line.
   */
  private static final class LineLengthGuard extends FilterReader {
    private final int maxLineLength;
    private long sinceTerminator;

    LineLengthGuard(Reader in, int maxLineLength) {
      super(in);
      this.maxLineLength = maxLineLength;
    }

    @Override
    public int read() throws IOException {
      int c = super.read();
      if (c >= 0) {
        sinceTerminator = (c == '\n' || c == '\r') ? 0 : sinceTerminator + 1;
        checkLength();
      }
      return c;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
      int read = super.read(cbuf, off, len);
      int end = off + read;
      int runStart = off;
      for (int i = off; i < end; i++) {
        if (cbuf[i] == '\n' || cbuf[i] == '\r') {
          sinceTerminator += i - runStart;
          checkLength();
          sinceTerminator = 0;
          runStart = i + 1;
        }
      }
      if (read > 0) {
        sinceTerminator += end - runStart;
        checkLength();
      }
      return read;
    }

    private void checkLength() {
      if (sinceTerminator > maxLineLength) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "NDJSON line exceeds maxLineLength of "
                + maxLineLength
                + " characters; is the input really newline delimited?");
      }
    }
  }
}
