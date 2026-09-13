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
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;

/**
 * Loads documents in <a href="https://ndjson.org/">Newline Delimited JSON</a> (ND-JSON, also known
 * as JSON Lines or JSONL) format; one JSON object per line, each representing a document to add.
 *
 * <p>Documents are parsed and indexed one line at a time, so arbitrarily large inputs can be
 * streamed without holding more than a single document in memory. Unlike {@link JsonLoader}, update
 * commands such as {@code delete} or {@code commit} are not recognized; use request parameters or a
 * separate request for those. As on the {@code /update/json/docs} path, a nested JSON object is
 * always a child document, so atomic updates are not expressible in this format.
 */
public class NDJsonLoader extends ContentStreamLoader {

  /** The content types that select this loader. */
  public static final Set<String> CONTENT_TYPES =
      Set.of("application/x-ndjson", "application/jsonl", "application/x-jsonlines");

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
    final int commitWithin = req.getParams().getInt(UpdateParams.COMMIT_WITHIN, -1);
    final boolean overwrite = req.getParams().getBool(UpdateParams.OVERWRITE, true);

    long lineNumber = 0;
    try (BufferedReader reader = new BufferedReader(stream.getReader())) {
      String line;
      while ((line = reader.readLine()) != null) {
        lineNumber++;
        if (line.isBlank()) {
          continue;
        }
        AddUpdateCommand cmd = new AddUpdateCommand(req);
        cmd.commitWithin = commitWithin;
        cmd.overwrite = overwrite;
        cmd.solrDoc = JsonLoader.buildDoc(parseLine(line, lineNumber));
        processor.processAdd(cmd);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> parseLine(String line, long lineNumber) {
    final Object parsed;
    final JSONParser parser;
    try {
      parser = new JSONParser(line);
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
}
