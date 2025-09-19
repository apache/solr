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
import java.util.Locale;
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
import org.apache.tika.config.TikaConfig;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.html.HtmlMapper;
import org.apache.tika.sax.XHTMLContentHandler;
import org.apache.tika.sax.xpath.XPathParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The class responsible for loading extracted content into Solr. */
public class ExtractingDocumentLoader extends ContentStreamLoader {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** Extract Only supported format */
  public static final String TEXT_FORMAT = "text";

  /** Extract Only supported format. Default */
  public static final String XML_FORMAT = "xml";

  /** XHTML XPath parser. */
  private static final XPathParser PARSER = new XPathParser("xhtml", XHTMLContentHandler.XHTML);

  final SolrCore core;
  final SolrParams params;
  final UpdateRequestProcessor processor;
  final boolean ignoreTikaException;
  protected AutoDetectParser autoDetectParser;

  private final AddUpdateCommand templateAdd;

  protected TikaConfig config;
  protected ParseContextConfig parseContextConfig;
  protected SolrContentHandlerFactory factory;
  protected ExtractionBackend backend;

  public ExtractingDocumentLoader(
      SolrQueryRequest req,
      UpdateRequestProcessor processor,
      TikaConfig config,
      ParseContextConfig parseContextConfig,
      SolrContentHandlerFactory factory,
      ExtractionBackend backend) {
    this.params = req.getParams();
    this.core = req.getCore();
    this.config = config;
    this.parseContextConfig = parseContextConfig;
    this.processor = processor;

    templateAdd = new AddUpdateCommand(req);
    templateAdd.overwrite = params.getBool(UpdateParams.OVERWRITE, true);
    templateAdd.commitWithin = params.getInt(UpdateParams.COMMIT_WITHIN, -1);

    // this is lightweight
    autoDetectParser = new AutoDetectParser(config);
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
    // If you specify the resource name (the filename, roughly) with this parameter,
    // some backends can make use of it in guessing the appropriate MIME type:
    String resourceName = req.getParams().get(ExtractingParams.RESOURCE_NAME, null);

    try (InputStream inputStream = stream.getStream()) {
      // HtmlParser and TXTParser regard Metadata.CONTENT_ENCODING in metadata
      String charset = ContentStreamBase.getCharsetFromContentType(stream.getContentType());

      String xpathExpr = params.get(ExtractingParams.XPATH_EXPRESSION);
      boolean extractOnly = params.getBool(ExtractingParams.EXTRACT_ONLY, false);

      ExtractionRequest extractionRequest =
          new ExtractionRequest(
              streamType,
              resourceName,
              stream.getContentType(),
              charset,
              stream.getName(),
              stream.getSourceInfo(),
              stream.getSize(),
              params.get(ExtractingParams.RESOURCE_PASSWORD, null));

      // Determine if we must use the legacy SAX/XHTML pipeline (needed for
      // capture/xpath/extractOnly)
      boolean captureAttr = params.getBool(ExtractingParams.CAPTURE_ATTRIBUTES, false);
      String[] captureElems = params.getParams(ExtractingParams.CAPTURE_ELEMENTS);
      boolean needLegacySax =
          extractOnly
              || xpathExpr != null
              || captureAttr
              || (captureElems != null && captureElems.length > 0)
              || (params.get(ExtractingParams.RESOURCE_PASSWORD) != null);

      if (backend instanceof LocalTikaExtractionBackend) {
        // Use in-process Tika and SAX pipeline to preserve legacy behavior & test expectations
        org.apache.tika.metadata.Metadata md = new org.apache.tika.metadata.Metadata();
        if (resourceName != null) {
          md.add(org.apache.tika.metadata.TikaMetadataKeys.RESOURCE_NAME_KEY, resourceName);
        }
        if (stream.getContentType() != null) {
          md.add(org.apache.tika.metadata.HttpHeaders.CONTENT_TYPE, stream.getContentType());
          md.add(ExtractingMetadataConstants.STREAM_CONTENT_TYPE, stream.getContentType());
        }
        if (charset != null) {
          md.add(org.apache.tika.metadata.HttpHeaders.CONTENT_ENCODING, charset);
        }
        if (stream.getName() != null) {
          md.add(ExtractingMetadataConstants.STREAM_NAME, stream.getName());
        }
        if (stream.getSourceInfo() != null) {
          md.add(ExtractingMetadataConstants.STREAM_SOURCE_INFO, stream.getSourceInfo());
        }
        if (stream.getSize() != null) {
          md.add(ExtractingMetadataConstants.STREAM_SIZE, String.valueOf(stream.getSize()));
        }

        org.apache.tika.parser.Parser parser;
        if (streamType != null) {
          org.apache.tika.mime.MediaType mt =
              org.apache.tika.mime.MediaType.parse(streamType.trim().toLowerCase(Locale.ROOT));
          parser =
              new org.apache.tika.parser.DefaultParser(config.getMediaTypeRegistry())
                  .getParsers()
                  .get(mt);
        } else {
          parser = autoDetectParser;
        }
        if (parser == null) {
          throw new IllegalArgumentException("No Tika parser for stream type: " + streamType);
        }

        org.apache.tika.parser.ParseContext context = parseContextConfig.create();
        context.set(org.apache.tika.parser.Parser.class, parser);
        context.set(
            org.apache.tika.parser.html.HtmlMapper.class, MostlyPassthroughHtmlMapper.INSTANCE);
        RegexRulesPasswordProvider pwd = new RegexRulesPasswordProvider();
        String explicitPwd = params.get(ExtractingParams.RESOURCE_PASSWORD);
        if (explicitPwd != null) pwd.setExplicitPassword(explicitPwd);
        String passwordsFile = params.get("passwordsFile");
        if (passwordsFile != null) {
          try (java.io.InputStream is = core.getResourceLoader().openResource(passwordsFile)) {
            pwd.parse(is);
          }
        }
        context.set(org.apache.tika.parser.PasswordProvider.class, pwd);

        if (extractOnly) {
          String extractFormat = params.get(ExtractingParams.EXTRACT_FORMAT, XML_FORMAT);

          if (xpathExpr != null) {
            // Always return text when xpath is provided, matching legacy behavior
            org.apache.tika.sax.ToTextContentHandler textHandler =
                new org.apache.tika.sax.ToTextContentHandler();
            org.apache.tika.sax.xpath.Matcher matcher = PARSER.parse(xpathExpr);
            org.xml.sax.ContentHandler ch =
                new org.apache.tika.sax.xpath.MatchingContentHandler(textHandler, matcher);
            try {
              parser.parse(inputStream, ch, md, context);
            } catch (Exception e) {
              if (ignoreTikaException) {
                if (log.isWarnEnabled())
                  log.warn("skip extracting text due to {}.", e.getLocalizedMessage(), e);
                return;
              } else {
                throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
              }
            }
            rsp.add(stream.getName(), textHandler.toString());

          } else if (XML_FORMAT.equals(extractFormat)) {
            org.apache.tika.sax.ToXMLContentHandler toXml =
                new org.apache.tika.sax.ToXMLContentHandler();
            org.xml.sax.ContentHandler ch = toXml;
            if (xpathExpr != null) {
              org.apache.tika.sax.xpath.Matcher matcher = PARSER.parse(xpathExpr);
              ch = new org.apache.tika.sax.xpath.MatchingContentHandler(toXml, matcher);
            }
            try {
              parser.parse(inputStream, ch, md, context);
            } catch (Exception e) {
              if (ignoreTikaException) {
                if (log.isWarnEnabled())
                  log.warn("skip extracting text due to {}.", e.getLocalizedMessage(), e);
                return;
              } else {
                throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
              }
            }
            String xml = toXml.toString();
            if (!xml.startsWith("<?xml")) {
              xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + xml;
            }
            rsp.add(stream.getName(), xml);
          } else { // TEXT_FORMAT
            org.apache.tika.sax.ToTextContentHandler textHandler =
                new org.apache.tika.sax.ToTextContentHandler();
            try {
              if (xpathExpr != null) {
                org.apache.tika.sax.xpath.Matcher matcher = PARSER.parse(xpathExpr);
                org.xml.sax.ContentHandler ch =
                    new org.apache.tika.sax.xpath.MatchingContentHandler(textHandler, matcher);
                parser.parse(inputStream, ch, md, context);
              } else {
                parser.parse(inputStream, textHandler, md, context);
              }
            } catch (Exception e) {
              if (ignoreTikaException) {
                if (log.isWarnEnabled())
                  log.warn("skip extracting text due to {}.", e.getLocalizedMessage(), e);
                return;
              } else {
                throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
              }
            }
            rsp.add(stream.getName(), textHandler.toString());
          }

          // Add metadata to the response
          NamedList<String[]> metadataNL = new NamedList<>();
          for (String name : md.names()) {
            String[] vals = md.getValues(name);
            metadataNL.add(name, vals);
          }
          rsp.add(stream.getName() + "_metadata", metadataNL);
        } else {
          // Indexing with capture/captureAttr etc.
          SimpleExtractionMetadata neutral = new SimpleExtractionMetadata();
          SolrContentHandler handler =
              factory.createSolrContentHandler(neutral, params, req.getSchema());
          try {
            parser.parse(inputStream, handler, md, context);
          } catch (Exception e) {
            if (ignoreTikaException) {
              if (log.isWarnEnabled())
                log.warn("skip extracting text due to {}.", e.getLocalizedMessage(), e);
              // Index a document with literals only (no extracted content/metadata)
              addDoc(handler);
              return;
            } else {
              throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
            }
          }
          // After parsing, transfer metadata into neutral and index
          for (String name : md.names()) {
            String[] vals = md.getValues(name);
            if (vals != null) {
              for (String v : vals) neutral.add(name, v);
            }
          }
          addDoc(handler);
        }
      } else {
        // Default backend-neutral path
        ExtractionResult result;
        try {
          result = backend.extract(inputStream, extractionRequest);
        } catch (Exception e) {
          if (ignoreTikaException) {
            if (log.isWarnEnabled()) {
              log.warn("skip extracting text due to {}.", e.getLocalizedMessage(), e);
            }
            // Index a document with literals only (no extracted content/metadata)
            SolrContentHandler handler =
                factory.createSolrContentHandler(
                    new SimpleExtractionMetadata(), params, req.getSchema());
            addDoc(handler);
            return;
          } else {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
          }
        }

        ExtractionMetadata metadata = result.getMetadata();
        String content = result.getContent();

        if (extractOnly == false) {
          SolrContentHandler handler =
              factory.createSolrContentHandler(metadata, params, req.getSchema());
          handler.appendToContent(content);
          addDoc(handler);
        } else {
          if (xpathExpr != null) {
            throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST,
                "XPath filtering is not supported with the backend-neutral extraction API.");
          }
          String extractFormat = params.get(ExtractingParams.EXTRACT_FORMAT, "xml");
          String out;
          if (extractFormat.equals(TEXT_FORMAT)) {
            out = content != null ? content : "";
          } else {
            // wrap content in basic XML with CDATA to avoid escaping
            String safe = content == null ? "" : content.replace("]]>", "]]]]>\u003c![CDATA[>");
            out = "<body><![CDATA[" + safe + "]]></body>";
          }
          rsp.add(stream.getName(), out);
          String[] names = metadata.names();
          NamedList<String[]> metadataNL = new NamedList<>();
          for (int i = 0; i < names.length; i++) {
            String[] vals = metadata.getValues(names[i]);
            metadataNL.add(names[i], vals);
          }
          rsp.add(stream.getName() + "_metadata", metadataNL);
        }
      }
    }
  }

  public static class MostlyPassthroughHtmlMapper implements HtmlMapper {
    public static final HtmlMapper INSTANCE = new MostlyPassthroughHtmlMapper();

    /**
     * Keep all elements and their content.
     *
     * <p>Apparently &lt;SCRIPT&gt; and &lt;STYLE&gt; elements are blocked elsewhere
     */
    @Override
    public boolean isDiscardElement(String name) {
      return false;
    }

    /** Lowercases the attribute name */
    @Override
    public String mapSafeAttribute(String elementName, String attributeName) {
      return attributeName.toLowerCase(Locale.ENGLISH);
    }

    /**
     * Lowercases the element name, but returns null for &lt;BR&gt;, which suppresses the
     * start-element event for lt;BR&gt; tags. This also suppresses the &lt;BODY&gt; tags because
     * those are handled internally by Tika's XHTMLContentHandler.
     */
    @Override
    public String mapSafeElement(String name) {
      String lowerName = name.toLowerCase(Locale.ROOT);
      return (lowerName.equals("br") || lowerName.equals("body")) ? null : lowerName;
    }
  }
}
