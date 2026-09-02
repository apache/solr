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

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.extraction.fromtika.ToTextContentHandler;
import org.apache.solr.handler.extraction.fromtika.ToXMLContentHandler;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.tika.sax.XHTMLContentHandler;
import org.apache.tika.sax.xpath.Matcher;
import org.apache.tika.sax.xpath.MatchingContentHandler;
import org.apache.tika.sax.xpath.XPathParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.helpers.DefaultHandler;

/**
 * The class responsible for loading extracted content into Solr. It will delegate parsing to a
 * {@link ExtractionBackend} and then load the resulting SolrInputDocument into Solr.
 */
public class ExtractingDocumentLoader extends ContentStreamLoader {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** Extract Only supported format */
  public static final String TEXT_FORMAT = "text";

  /** Extract Only supported format. Default */
  public static final String XML_FORMAT = "xml";

  final SolrCore core;
  final UpdateRequestProcessor processor;
  final boolean ignoreTikaException;

  private final AddUpdateCommand templateAdd;

  protected SolrContentHandlerFactory factory;
  protected ExtractionBackend backend;

  public ExtractingDocumentLoader(
      SolrQueryRequest req,
      UpdateRequestProcessor processor,
      SolrContentHandlerFactory factory,
      ExtractionBackend backend) {
    SolrParams params = req.getParams();
    this.core = req.getCore();
    this.processor = processor;

    templateAdd = new AddUpdateCommand(req);
    templateAdd.overwrite = params.getBool(UpdateParams.OVERWRITE, true);
    templateAdd.commitWithin = params.getInt(UpdateParams.COMMIT_WITHIN, -1);

    this.factory = factory;
    this.backend = backend;

    ignoreTikaException = params.getBool(ExtractingParams.IGNORE_TIKA_EXCEPTION, false);
  }

  /** this must be MT safe... may be called concurrently from multiple threads. */
  void doAdd(SolrContentHandler handler, AddUpdateCommand template) throws IOException {
    template.solrDoc = handler.newDocument();
    processor.processAdd(template);
  }

  void addDoc(SolrContentHandler handler) throws IOException {
    templateAdd.clear();
    doAdd(handler, templateAdd);
  }

  @Override
  public void load(
      SolrQueryRequest req,
      SolrQueryResponse rsp,
      ContentStream stream,
      UpdateRequestProcessor processor)
      throws Exception {
    SolrParams params = req.getParams();
    String streamType = params.get(ExtractingParams.STREAM_TYPE, null);
    String resourceName = params.get(ExtractingParams.RESOURCE_NAME, null);

    try (InputStream inputStream = stream.getStream()) {
      String charset = ContentStreamBase.getCharsetFromContentType(stream.getContentType());

      String xpathExpr = params.get(ExtractingParams.XPATH_EXPRESSION);
      boolean extractOnly = params.getBool(ExtractingParams.EXTRACT_ONLY, false);
      // Prefer new parameter name; fall back to legacy name for backward compatibility
      boolean tikaserverRecursive = params.getBool(ExtractingParams.TIKASERVER_RECURSIVE, false);
      String extractFormat =
          params.get(ExtractingParams.EXTRACT_FORMAT, extractOnly ? XML_FORMAT : TEXT_FORMAT);

      // Parse optional passwords file into a map
      LinkedHashMap<Pattern, String> pwMap = null;
      String passwordsFile = params.get(ExtractingParams.PASSWORD_MAP_FILE);
      if (passwordsFile != null) {
        try (InputStream is = core.getResourceLoader().openResource(passwordsFile)) {
          pwMap = RegexRulesPasswordProvider.parseRulesFile(is);
        }
      }

      Integer tikaTimeoutSecs = params.getInt(ExtractingParams.TIKASERVER_TIMEOUT_SECS);
      ExtractionRequest extractionRequest =
          ExtractionRequest.builder()
              .streamType(streamType)
              .resourceName(resourceName)
              .contentType(stream.getContentType())
              .charset(charset)
              .streamName(stream.getName())
              .streamSourceInfo(stream.getSourceInfo())
              .streamSize(stream.getSize())
              .resourcePassword(params.get(ExtractingParams.RESOURCE_PASSWORD, null))
              .passwordsMap(pwMap)
              .extractFormat(extractFormat)
              .tikaServerRecursive(tikaserverRecursive)
              .tikaServerTimeoutSeconds(tikaTimeoutSecs)
              .tikaServerRequestHeaders(Map.of())
              .tikaServerConfigJson(params.get(ExtractingParams.TIKASERVER_CONFIG_JSON))
              .tikaServerChunks(params.getBool(ExtractingParams.TIKASERVER_CHUNKS, false))
              .build();

      if (extractionRequest.tikaServerChunks) {
        try {
          loadChunks(req, params, stream, inputStream, extractionRequest);
        } catch (Exception e) {
          if (ignoreTikaException) {
            if (log.isWarnEnabled())
              log.warn("skip extracting chunks due to {}.", e.getLocalizedMessage(), e);
            return;
          }
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        }
        return;
      }

      boolean captureAttr = params.getBool(ExtractingParams.CAPTURE_ATTRIBUTES, false);
      String[] captureElems = params.getParams(ExtractingParams.CAPTURE_ELEMENTS);
      boolean needsSaxParsing =
          extractOnly
              || xpathExpr != null
              || captureAttr
              || (captureElems != null && captureElems.length > 0)
              || (params.get(ExtractingParams.RESOURCE_PASSWORD) != null)
              || (passwordsFile != null);

      if (extractOnly) {
        try {
          ExtractionMetadata md = backend.buildMetadataFromRequest(extractionRequest);
          String content;
          if (ExtractingDocumentLoader.TEXT_FORMAT.equals(extractionRequest.extractFormat)
              || xpathExpr != null) {
            content =
                extractWithHandler(
                    inputStream, xpathExpr, extractionRequest, md, new ToTextContentHandler());
          } else { // XML format
            content =
                extractWithHandler(
                    inputStream, xpathExpr, extractionRequest, md, new ToXMLContentHandler());
            if (!content.startsWith("<?xml")) {
              content = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + content;
            }
          }

          rsp.add(stream.getName(), content);
          NamedList<String[]> metadataNL = new NamedList<>();
          for (String name : md.keySet()) {
            metadataNL.add(name, md.get(name).toArray(new String[0]));
          }
          rsp.add(stream.getName() + "_metadata", metadataNL);
        } catch (Exception e) {
          if (ignoreTikaException) {
            if (log.isWarnEnabled())
              log.warn("skip extracting text due to {}.", e.getLocalizedMessage(), e);
            return;
          }
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        }
        return;
      }

      if (needsSaxParsing) {
        ExtractionMetadata metadata = backend.buildMetadataFromRequest(extractionRequest);
        SolrContentHandler handler =
            factory.createSolrContentHandler(metadata, params, req.getSchema());
        try {
          backend.extractWithSaxHandler(inputStream, extractionRequest, metadata, handler);
        } catch (Exception e) {
          if (ignoreTikaException) {
            if (log.isWarnEnabled()) {
              log.warn("skip extracting text due to {}.", e.getLocalizedMessage(), e);
              return;
            }
          }
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        }

        addDoc(handler);
        return;
      }

      ExtractionResult result;
      try {
        result = backend.extract(inputStream, extractionRequest);
      } catch (Exception e) {
        if (ignoreTikaException) {
          if (log.isWarnEnabled())
            log.warn("skip extracting text due to {}.", e.getLocalizedMessage(), e);
          return;
        }
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }

      ExtractionMetadata metadata = result.getMetadata();

      SolrContentHandler handler =
          factory.createSolrContentHandler(metadata, params, req.getSchema());
      handler.appendToContent(result.getContent());
      addDoc(handler);
    }
  }

  /**
   * Indexes one Solr document per Tika 4.x {@code tk:chunks} entry instead of one document for the
   * whole source file. See {@link ExtractingParams#TIKASERVER_CHUNKS}.
   */
  private void loadChunks(
      SolrQueryRequest req,
      SolrParams params,
      ContentStream stream,
      InputStream inputStream,
      ExtractionRequest extractionRequest)
      throws Exception {
    if (!(backend instanceof TikaServerExtractionBackend tikaServerBackend)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          ExtractingParams.TIKASERVER_CHUNKS
              + "=true requires the "
              + TikaServerExtractionBackend.NAME
              + " extraction backend");
    }
    List<TikaServerExtractionBackend.Chunk> chunks =
        tikaServerBackend.extractChunks(inputStream, extractionRequest);

    String contentField = params.get(ExtractingParams.TIKASERVER_CHUNKS_CONTENT_FIELD, "content");
    String vectorField = params.get(ExtractingParams.TIKASERVER_CHUNKS_VECTOR_FIELD, "vector");
    String parentField =
        params.get(ExtractingParams.TIKASERVER_CHUNKS_PARENT_FIELD, "chunk_parent_id");

    String uniqueKeyField =
        req.getSchema().getUniqueKeyField() != null
            ? req.getSchema().getUniqueKeyField().getName()
            : "id";

    String parentId = params.get(ExtractingParams.LITERALS_PREFIX + uniqueKeyField);
    if (parentId == null) parentId = params.get(ExtractingParams.RESOURCE_NAME);
    if (parentId == null) parentId = stream.getName();
    if (parentId == null) parentId = UUID.randomUUID().toString();

    int n = 0;
    for (TikaServerExtractionBackend.Chunk chunk : chunks) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.setField(uniqueKeyField, parentId + "-chunk-" + n);
      doc.setField(contentField, chunk.text);
      doc.setField(vectorField, chunk.vector);
      doc.setField(parentField, parentId);

      Iterator<String> paramNames = params.getParameterNamesIterator();
      while (paramNames.hasNext()) {
        String pname = paramNames.next();
        if (!pname.startsWith(ExtractingParams.LITERALS_PREFIX)) continue;
        String name = pname.substring(ExtractingParams.LITERALS_PREFIX.length());
        if (name.equals(uniqueKeyField)) continue;
        for (String v : params.getParams(pname)) {
          doc.addField(name, v);
        }
      }

      templateAdd.clear();
      templateAdd.solrDoc = doc;
      processor.processAdd(templateAdd);
      n++;
    }
  }

  /*
   * Extracts content from the given input stream using an optional XPath expression
   * and a SAX content handler. The extraction process may filter content based on
   * the XPath expression, if provided.
   */
  private String extractWithHandler(
      InputStream inputStream,
      String xpathExpr,
      ExtractionRequest extractionRequest,
      ExtractionMetadata md,
      DefaultHandler ch)
      throws Exception {
    if (xpathExpr != null) {
      XPathParser xparser = new XPathParser("xhtml", XHTMLContentHandler.XHTML);
      Matcher matcher = xparser.parse(xpathExpr);
      ch = new MatchingContentHandler(ch, matcher);
    }
    backend.extractWithSaxHandler(inputStream, extractionRequest, md, ch);
    return ch.toString();
  }
}
