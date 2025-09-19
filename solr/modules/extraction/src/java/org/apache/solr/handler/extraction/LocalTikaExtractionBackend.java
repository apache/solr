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
package org.apache.solr.handler.extraction;

import java.io.InputStream;
import java.util.Locale;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.metadata.HttpHeaders;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaMetadataKeys;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.DefaultParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.PasswordProvider;
import org.apache.tika.parser.html.HtmlMapper;
import org.apache.tika.sax.BodyContentHandler;

/**
 * Extraction backend using local in-process Apache Tika. This encapsulates the previous direct
 * usage of Tika from the loader.
 */
public class LocalTikaExtractionBackend implements ExtractionBackend {
  private final TikaConfig tikaConfig;
  private final ParseContextConfig parseContextConfig;
  private final AutoDetectParser autoDetectParser;

  public LocalTikaExtractionBackend(TikaConfig config, ParseContextConfig parseContextConfig) {
    this.tikaConfig = config;
    this.parseContextConfig = parseContextConfig;
    this.autoDetectParser = new AutoDetectParser(config);
  }

  @Override
  public String name() {
    return "local";
  }

  @Override
  public ExtractionResult extract(InputStream inputStream, ExtractionRequest request)
      throws Exception {
    Parser parser = null;
    if (request.streamType != null) {
      MediaType mt = MediaType.parse(request.streamType.trim().toLowerCase(Locale.ROOT));
      parser = new DefaultParser(tikaConfig.getMediaTypeRegistry()).getParsers().get(mt);
    } else {
      parser = autoDetectParser;
    }
    if (parser == null) {
      throw new IllegalArgumentException("No Tika parser for stream type: " + request.streamType);
    }

    Metadata md = new Metadata();
    if (request.resourceName != null) {
      md.add(TikaMetadataKeys.RESOURCE_NAME_KEY, request.resourceName);
    }
    if (request.contentType != null) {
      md.add(HttpHeaders.CONTENT_TYPE, request.contentType);
    }
    if (request.streamName != null) {
      md.add(ExtractingMetadataConstants.STREAM_NAME, request.streamName);
    }
    if (request.streamSourceInfo != null) {
      md.add(ExtractingMetadataConstants.STREAM_SOURCE_INFO, request.streamSourceInfo);
    }
    if (request.streamSize != null) {
      md.add(ExtractingMetadataConstants.STREAM_SIZE, String.valueOf(request.streamSize));
    }
    if (request.contentType != null) {
      md.add(ExtractingMetadataConstants.STREAM_CONTENT_TYPE, request.contentType);
    }
    if (request.charset != null) {
      md.add(HttpHeaders.CONTENT_ENCODING, request.charset);
    }

    ParseContext context = parseContextConfig.create();
    context.set(Parser.class, parser);
    context.set(HtmlMapper.class, ExtractingDocumentLoader.MostlyPassthroughHtmlMapper.INSTANCE);

    // Password handling: allow passing explicit and map via params in future if needed.
    PasswordProvider epp = new RegexRulesPasswordProvider();
    if (request.resourcePassword != null && epp instanceof RegexRulesPasswordProvider) {
      ((RegexRulesPasswordProvider) epp).setExplicitPassword(request.resourcePassword);
    }
    context.set(PasswordProvider.class, epp);

    BodyContentHandler textHandler = new BodyContentHandler(-1);
    parser.parse(inputStream, textHandler, md, context);

    // copy metadata to neutral container
    ExtractionMetadata outMetadata = new SimpleExtractionMetadata();
    for (String name : md.names()) {
      String[] vals = md.getValues(name);
      if (vals != null) {
        for (String v : vals) {
          outMetadata.add(name, v);
        }
      }
    }
    String content = textHandler.toString();
    return new ExtractionResult(content, outMetadata);
  }
}
