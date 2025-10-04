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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.helpers.DefaultHandler;

/** The class responsible for loading extracted content into Solr. */
public class ExtractingDocumentLoader extends ContentStreamLoader {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** Extract Only supported format */
  public static final String TEXT_FORMAT = "text";

  /** Extract Only supported format. Default */
  public static final String XML_FORMAT = "xml";

  final SolrCore core;
  final SolrParams params;
  final UpdateRequestProcessor processor;
  final boolean ignoreTikaException;
  final boolean backCompat;

  private final AddUpdateCommand templateAdd;

  protected SolrContentHandlerFactory factory;
  protected ExtractionBackend backend;

  public ExtractingDocumentLoader(
      SolrQueryRequest req,
      UpdateRequestProcessor processor,
      SolrContentHandlerFactory factory,
      ExtractionBackend backend) {
    this.params = req.getParams();
    this.core = req.getCore();
    this.processor = processor;
    this.backCompat = params.getBool(ExtractingParams.BACK_COMPATIBILITY, true);

    templateAdd = new AddUpdateCommand(req);
    templateAdd.overwrite = params.getBool(UpdateParams.OVERWRITE, true);
    templateAdd.commitWithin = params.getInt(UpdateParams.COMMIT_WITHIN, -1);
    templateAdd.overwrite = params.getBool(UpdateParams.OVERWRITE, true);

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
    String streamType = req.getParams().get(ExtractingParams.STREAM_TYPE, null);
    String resourceName = req.getParams().get(ExtractingParams.RESOURCE_NAME, null);

    try (InputStream inputStream = stream.getStream()) {
      String charset = ContentStreamBase.getCharsetFromContentType(stream.getContentType());

      String xpathExpr = params.get(ExtractingParams.XPATH_EXPRESSION);
      boolean extractOnly = params.getBool(ExtractingParams.EXTRACT_ONLY, false);
      boolean recursive = params.getBool(ExtractingParams.RECURSIVE, false);
      String extractFormat =
          params.get(ExtractingParams.EXTRACT_FORMAT, extractOnly ? XML_FORMAT : TEXT_FORMAT);

      // Parse optional passwords file into a map (keeps Tika usages out of this class)
      LinkedHashMap<Pattern, String> pwMap = null;
      String passwordsFile = params.get("passwordsFile");
      if (passwordsFile != null) {
        try (java.io.InputStream is = core.getResourceLoader().openResource(passwordsFile)) {
          pwMap = RegexRulesPasswordProvider.parseRulesFile(is);
        }
      }

      ExtractionRequest extractionRequest =
          new ExtractionRequest(
              streamType,
              resourceName,
              stream.getContentType(),
              charset,
              stream.getName(),
              stream.getSourceInfo(),
              stream.getSize(),
              params.get(ExtractingParams.RESOURCE_PASSWORD, null),
              pwMap,
              extractFormat,
              recursive,
              Collections.emptyMap());

      boolean captureAttr = params.getBool(ExtractingParams.CAPTURE_ATTRIBUTES, false);
      String[] captureElems = params.getParams(ExtractingParams.CAPTURE_ELEMENTS);
      boolean needLegacySax =
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
            org.apache.tika.sax.ToTextContentHandler textHandler =
                new org.apache.tika.sax.ToTextContentHandler();
            DefaultHandler ch = textHandler;
            if (xpathExpr != null) {
              org.apache.tika.sax.xpath.XPathParser xparser =
                  new org.apache.tika.sax.xpath.XPathParser(
                      "xhtml", org.apache.tika.sax.XHTMLContentHandler.XHTML);
              org.apache.tika.sax.xpath.Matcher matcher = xparser.parse(xpathExpr);
              ch = new org.apache.tika.sax.xpath.MatchingContentHandler(textHandler, matcher);
            }
            backend.extractWithSaxHandler(inputStream, extractionRequest, md, ch);
            content = textHandler.toString();
          } else { // XML format
            org.apache.tika.sax.ToXMLContentHandler toXml =
                new org.apache.tika.sax.ToXMLContentHandler();
            DefaultHandler ch = toXml;
            if (xpathExpr != null) {
              org.apache.tika.sax.xpath.XPathParser xparser =
                  new org.apache.tika.sax.xpath.XPathParser(
                      "xhtml", org.apache.tika.sax.XHTMLContentHandler.XHTML);
              org.apache.tika.sax.xpath.Matcher matcher = xparser.parse(xpathExpr);
              ch = new org.apache.tika.sax.xpath.MatchingContentHandler(toXml, matcher);
            }
            backend.extractWithSaxHandler(inputStream, extractionRequest, md, ch);
            content = toXml.toString();
            if (!content.startsWith("<?xml")) {
              content = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + content;
            }
          }

          appendBackCompatTikaMetadata(md);

          // Write content
          rsp.add(stream.getName(), content);
          // Write metadata
          NamedList<String[]> metadataNL = new NamedList<>();
          for (String name : md.names()) {
            metadataNL.add(name, md.getValues(name));
          }
          rsp.add(stream.getName() + "_metadata", metadataNL);
        } catch (UnsupportedOperationException uoe) {
          // For backends that don't support xpath
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "XPath filtering is not supported by backend '" + backend.name() + "'.");
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

      if (needLegacySax) {
        // Indexing with capture/xpath/etc: delegate SAX parse to backend
        ExtractionMetadata metadata = backend.buildMetadataFromRequest(extractionRequest);
        SolrContentHandler handler =
            factory.createSolrContentHandler(metadata, params, req.getSchema());
        try {
          backend.extractWithSaxHandler(inputStream, extractionRequest, metadata, handler);
        } catch (UnsupportedOperationException uoe) {
          // For backends that don't support parseToSolrContentHandler
          if (log.isWarnEnabled()) {
            log.warn("skip extracting text since tika backend does not yet support this option");
          }
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "The requested operation is not supported by backend '" + backend.name() + "'.");
        } catch (Exception e) {
          if (ignoreTikaException) {
            if (log.isWarnEnabled()) {
              log.warn("skip extracting text due to {}.", e.getLocalizedMessage(), e);
              return;
            }
          }
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        }
        appendBackCompatTikaMetadata(handler.metadata);

        addDoc(handler);
        return;
      }

      // Default simple backend-neutral path
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

      appendBackCompatTikaMetadata(metadata);

      String content = result.getContent();

      SolrContentHandler handler =
          factory.createSolrContentHandler(metadata, params, req.getSchema());
      handler.appendToContent(content);
      addDoc(handler);
    }
  }

  private final Map<String, String> fieldMappings = new LinkedHashMap<>();

  {
    fieldMappings.put("dc:title", "title");
    fieldMappings.put("dc:creator", "author");
    fieldMappings.put("dc:description", "description");
    fieldMappings.put("dc:subject", "subject");
    fieldMappings.put("dc:language", "language");
    fieldMappings.put("dc:publisher", "publisher");
    fieldMappings.put("dcterms:created", "created");
    fieldMappings.put("dcterms:modified", "modified");
    fieldMappings.put("meta:author", "Author");
    fieldMappings.put("meta:creation-date", "Creation-Date");
    fieldMappings.put("meta:save-date", "Last-Save-Date");
    fieldMappings.put("meta:keyword", "Keywords");
    fieldMappings.put("pdf:docinfo:keywords", "Keywords");
  }

  private void appendBackCompatTikaMetadata(ExtractionMetadata md) {
    if (!backCompat) {
      return;
    }

    for (Map.Entry<String, String> mapping : fieldMappings.entrySet()) {
      String sourceField = mapping.getKey();
      String targetField = mapping.getValue();
      if (md.get(sourceField) != null && md.get(targetField) == null) {
        md.addValues(targetField, md.getValues(sourceField));
      }
    }
  }
}
