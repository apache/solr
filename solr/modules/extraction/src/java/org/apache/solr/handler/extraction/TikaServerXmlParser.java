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
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.apache.solr.common.SolrException;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class TikaServerXmlParser {
  private final SAXParser saxParser;

  public TikaServerXmlParser() {
    SAXParserFactory factory = SAXParserFactory.newInstance();
    factory.setNamespaceAware(true);
    try {
      factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
      factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
      factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
    } catch (Throwable ignore) {
      // Some parsers may not support all features; ignore
    }
    try {
      saxParser = factory.newSAXParser();
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  /**
   * Parses response in XML format from Tika Server /tika endpoint. The result is that the metadata
   * object is populated and the content handler is called with extracted text.
   */
  public void parse(InputStream inputStream, ContentHandler handler, ExtractionMetadata metadata)
      throws IOException, SAXException {
    DefaultHandler myHandler = new TikaXmlResponseSaxContentHandler(handler, metadata);
    InputStream sanitizedStream = XmlSanitizingReader.sanitize(inputStream);
    saxParser.parse(sanitizedStream, myHandler);
  }

  /** Custom SAX handler that will extract meta tags from the tika xml and delegate */
  static class TikaXmlResponseSaxContentHandler extends DefaultHandler {
    private final ContentHandler delegate;
    private final ExtractionMetadata metadata;
    private boolean inHead = false;

    public TikaXmlResponseSaxContentHandler(ContentHandler delegate, ExtractionMetadata metadata) {
      this.delegate = delegate;
      this.metadata = metadata;
    }

    @Override
    public void startDocument() throws SAXException {
      if (delegate != null) delegate.startDocument();
    }

    @Override
    public void endDocument() throws SAXException {
      if (delegate != null) delegate.endDocument();
    }

    @Override
    public void startElement(
        String uri, String localName, String qName, org.xml.sax.Attributes attributes)
        throws SAXException {
      String ln = localName != null && !localName.isEmpty() ? localName : qName;
      if ("head".equalsIgnoreCase(ln)) {
        inHead = true;
      } else if (inHead && "meta".equalsIgnoreCase(ln) && attributes != null) {
        String name = attributes.getValue("name");
        String content = attributes.getValue("content");
        if (name != null && content != null) {
          metadata.add(name, content);
        }
      }
      if (delegate != null) delegate.startElement(uri, localName, qName, attributes);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
      String ln = localName != null && !localName.isEmpty() ? localName : qName;
      if ("head".equalsIgnoreCase(ln)) {
        inHead = false;
      }
      if (delegate != null) delegate.endElement(uri, localName, qName);
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
      if (delegate != null) delegate.characters(ch, start, length);
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
      if (delegate != null) delegate.ignorableWhitespace(ch, start, length);
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
      if (delegate != null) delegate.startPrefixMapping(prefix, uri);
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
      if (delegate != null) delegate.endPrefixMapping(prefix);
    }
  }
}
