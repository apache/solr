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
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import org.apache.solr.core.SolrCore;
import org.apache.solr.logging.DeprecationLog;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.DefaultParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.PasswordProvider;
import org.apache.tika.parser.html.HtmlMapper;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Extraction backend using local in-process Apache Tika. This encapsulates the previous direct
 * usage of Tika from the loader.
 *
 * @deprecated Will be removed soon, please use the 'tikaserver' extraction backend instead.
 */
@Deprecated(since = "9.10.0")
public class LocalTikaExtractionBackend implements ExtractionBackend {
  private final TikaConfig tikaConfig;
  private final ParseContextConfig parseContextConfig;
  private final AutoDetectParser autoDetectParser;

  // Local HtmlMapper moved from ExtractingDocumentLoader
  private static class MostlyPassthroughHtmlMapper implements HtmlMapper {
    static final HtmlMapper INSTANCE = new MostlyPassthroughHtmlMapper();

    @Override
    public boolean isDiscardElement(String name) {
      return false;
    }

    @Override
    public String mapSafeAttribute(String elementName, String attributeName) {
      return attributeName.toLowerCase(java.util.Locale.ENGLISH);
    }

    @Override
    public String mapSafeElement(String name) {
      String lowerName = name.toLowerCase(java.util.Locale.ROOT);
      return (lowerName.equals("br") || lowerName.equals("body")) ? null : lowerName;
    }
  }

  public LocalTikaExtractionBackend(TikaConfig config, ParseContextConfig parseContextConfig) {
    this.tikaConfig = config;
    this.parseContextConfig = parseContextConfig;
    this.autoDetectParser = new AutoDetectParser(config);
  }

  /**
   * Construct backend by loading TikaConfig based on handler/core configuration without exposing
   * Tika types to the handler.
   */
  public LocalTikaExtractionBackend(
      SolrCore core, String tikaConfigLoc, ParseContextConfig parseContextConfig) throws Exception {
    TikaConfig cfg;
    if (tikaConfigLoc == null) { // default
      ClassLoader classLoader = core.getResourceLoader().getClassLoader();
      try (InputStream is = classLoader.getResourceAsStream("solr-default-tika-config.xml")) {
        cfg = new TikaConfig(is);
      }
    } else {
      Path configFile = Path.of(tikaConfigLoc);
      core.getCoreContainer().assertPathAllowed(configFile);
      if (configFile.isAbsolute()) {
        cfg = new TikaConfig(configFile);
      } else { // in conf/
        try (InputStream is = core.getResourceLoader().openResource(tikaConfigLoc)) {
          cfg = new TikaConfig(is);
        }
      }
    }
    this.tikaConfig = cfg;
    this.parseContextConfig = parseContextConfig;
    this.autoDetectParser = new AutoDetectParser(cfg);
    DeprecationLog.log("Local Tika", "The 'local' extraction backend is deprecated");
  }

  public static final String NAME = "local";

  @Override
  public String name() {
    return NAME;
  }

  private Parser selectParser(ExtractionRequest request) {
    if (request.streamType != null) {
      MediaType mt = MediaType.parse(request.streamType.trim().toLowerCase(Locale.ROOT));
      return new DefaultParser(tikaConfig.getMediaTypeRegistry()).getParsers().get(mt);
    }
    return autoDetectParser;
  }

  private Metadata buildMetadata(ExtractionRequest request) {
    ExtractionMetadata extractionMetadata = buildMetadataFromRequest(request);
    Metadata md = new Metadata();
    for (String name : extractionMetadata.keySet()) {
      List<String> vals = extractionMetadata.get(name);
      if (vals != null) for (String v : vals) md.add(name, v);
    }
    return md;
  }

  private ParseContext buildContext(Parser parser, ExtractionRequest request) {
    ParseContext context = parseContextConfig.create();
    context.set(Parser.class, parser);
    context.set(HtmlMapper.class, MostlyPassthroughHtmlMapper.INSTANCE);
    RegexRulesPasswordProvider pwd = new RegexRulesPasswordProvider();
    if (request.resourcePassword != null) {
      pwd.setExplicitPassword(request.resourcePassword);
    }
    if (request.passwordsMap != null) {
      pwd.setPasswordMap(request.passwordsMap);
    }
    context.set(PasswordProvider.class, pwd);
    return context;
  }

  private static ExtractionMetadata tikaMetadataToExtractionMetadata(Metadata md) {
    ExtractionMetadata out = new ExtractionMetadata();
    for (String name : md.names()) {
      String[] vals = md.getValues(name);
      if (vals != null) for (String v : vals) out.add(name, v);
    }
    return out;
  }

  @Override
  public ExtractionResult extract(InputStream inputStream, ExtractionRequest request)
      throws Exception {
    Parser parser = selectParser(request);
    if (parser == null) {
      throw new IllegalArgumentException("No Tika parser for stream type: " + request.streamType);
    }
    ParseContext context = buildContext(parser, request);
    Metadata md = buildMetadata(request);
    BodyContentHandler textHandler = new BodyContentHandler(-1);
    parser.parse(inputStream, textHandler, md, context);
    return new ExtractionResult(textHandler.toString(), tikaMetadataToExtractionMetadata(md));
  }

  @Override
  public void extractWithSaxHandler(
      InputStream inputStream,
      ExtractionRequest request,
      ExtractionMetadata md,
      DefaultHandler saxContentHandler)
      throws Exception {
    Parser parser = selectParser(request);
    if (parser == null) {
      throw new IllegalArgumentException("No Tika parser for stream type: " + request.streamType);
    }
    ParseContext context = buildContext(parser, request);
    Metadata tikaMetadata = buildMetadata(request);
    parser.parse(inputStream, saxContentHandler, tikaMetadata, context);
    for (String name : tikaMetadata.names()) {
      String[] vals = tikaMetadata.getValues(name);
      if (vals != null) for (String v : vals) md.add(name, v);
    }
  }
}
