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

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.eclipse.jetty.http.MimeTypes;
import org.noggit.JSONParser;
import org.noggit.JSONParser.ParseException;

/**
 * Loads documents in <a href="https://ndjson.org/">Newline Delimited JSON</a> (ND-JSON, also known
 * as JSON Lines or JSONL) format; one JSON object per line, each representing a document to add.
 *
 * <p>Documents are mapped exactly as on the {@code /update/json/docs} path: a nested JSON object is
 * flattened into dotted field names unless the {@code split} parameter declares its path to be a
 * nested document. Update commands such as {@code delete} or {@code commit} are not recognized; use
 * request parameters or a separate request for those.
 *
 * <p>The only differences from that path are the restrictions the format itself implies, all
 * enforced while streaming: the content must be UTF-8, every document sits on a line of its own,
 * and {@code split} must start at the document root, since a line is already one document.
 */
public class NDJsonLoader extends JsonLoader {

  /** The content types that select this loader. */
  public static final Set<String> CONTENT_TYPES =
      Set.of("application/x-ndjson", "application/jsonl", "application/x-jsonlines");

  @Override
  public void load(
      SolrQueryRequest req,
      SolrQueryResponse rsp,
      ContentStream stream,
      UpdateRequestProcessor processor)
      throws Exception {
    assertUtf8(stream);
    super.load(req, rsp, stream, processor);
  }

  @Override
  protected SingleThreadedJsonLoader createLoader(
      SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor processor) {
    return new SingleThreadedNDJsonLoader(req, rsp, processor);
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

  private static class SingleThreadedNDJsonLoader extends SingleThreadedJsonLoader {

    private NDJsonParser ndJsonParser;

    SingleThreadedNDJsonLoader(
        SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor processor) {
      super(req, rsp, processor);
    }

    /** As the base loader, but reporting a malformed document by line rather than by offset. */
    @Override
    public void load(
        SolrQueryRequest req,
        SolrQueryResponse rsp,
        ContentStream stream,
        UpdateRequestProcessor processor)
        throws Exception {
      // The charset is already known to be UTF-8, so the stream is decoded as such
      try (Reader reader = new InputStreamReader(stream.getStream(), StandardCharsets.UTF_8)) {
        processUpdate(reader);
      } catch (ParseException e) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Cannot parse NDJSON at line " + ndJsonParser.getLineNumber() + ": " + e.getMessage());
      }
    }

    /** NDJSON carries documents only, so this is always the document path, never commands. */
    @Override
    void processUpdate(Reader reader) throws IOException {
      SolrParams params = req.getParams();
      if (params.get("srcField") != null) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "srcField is not supported for NDJSON; use the /update/json/docs path instead");
      }
      handleSplitMode(assertSplitStartsAtRoot(params.get("split")), params.getParams("f"), reader);
    }

    @Override
    JSONParser createParser(Reader reader, String srcField) {
      ndJsonParser = new NDJsonParser(reader);
      return ndJsonParser;
    }

    /**
     * Each line is already a document, so the first {@code split} path has to be the root. Further
     * paths are what declares a nested document, and must sit below it.
     */
    private static String assertSplitStartsAtRoot(String split) {
      if (split == null) {
        return "/";
      }
      String[] paths = split.split("\\|");
      if (!"/".equals(paths[0].trim())) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "split must start at the document root for NDJSON, since each line is already one"
                + " document; use split=/|"
                + paths[0].trim()
                + " rather than split="
                + split);
      }
      for (String path : paths) {
        if (path.indexOf('*') >= 0) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST, "split cannot contain wildcards: " + path);
        }
      }
      return split;
    }
  }
}
