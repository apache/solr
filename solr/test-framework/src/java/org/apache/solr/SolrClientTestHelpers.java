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
package org.apache.solr;

import java.io.StringWriter;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.InputStreamResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.XMLResponseParser;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.BaseTestHarness;

/**
 * Helper methods for using assertQ/req style testing with SolrClient-based tests. This bridges the
 * gap between TestHarness-style tests and EmbeddedSolrServerTestRule tests.
 *
 * <p>Example usage:
 *
 * <pre>
 * import static org.apache.solr.SolrClientTestHelpers.assertQClient;
 * import static org.apache.solr.SolrClientTestHelpers.params;
 *
 * SolrClient client = solrTestRule.getSolrClient();
 * assertQClient(client, params("q", "*:*"),
 *               "/response/lst[@name='responseHeader']/int[@name='status'][.='0']");
 * </pre>
 */
public class SolrClientTestHelpers {

  /**
   * Creates a SolrParams object from key-value pairs, similar to req() in SolrTestCaseJ4. Use this
   * with assertQClient(SolrClient, SolrParams, xpaths...).
   *
   * @param params alternating key-value pairs for query parameters
   * @return SolrParams object
   */
  public static SolrParams params(String... params) {
    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    for (int i = 0; i < params.length; i += 2) {
      if (i + 1 < params.length) {
        solrParams.add(params[i], params[i + 1]);
      }
    }
    return solrParams;
  }

  public static GenericSolrRequest req3(String... q) {
    ModifiableSolrParams params = new ModifiableSolrParams();

    if (q.length == 1) {
      params.set("q", q);
    }

    params.set("wt", "xml");

    params.set("indent", params.get("indent", "off"));

    var req =
        new GenericSolrRequest(
            SolrRequest.METHOD.GET, "/select", params // .add("indent", "true")
            );
    // Using the "smart" solr parsers won't work, because they decode into Solr objects.
    // When trying to re-write into JSON, the JSONWriter doesn't have the right info to print it
    // correctly.
    // All we want to do is pass the JSON response to the user, so do that.
    req.setResponseParser(new XMLResponseParser());

    return req;
  }

  public static QueryRequest req2(String... q) {
    ModifiableSolrParams params = new ModifiableSolrParams();

    if (q.length == 1) {
      params.set("q", q);
    }
    if (q.length % 2 != 0) {
      throw new RuntimeException(
          "The length of the string array (query arguments) needs to be even");
    }
    for (int i = 0; i < q.length; i += 2) {
      params.set(q[i], q[i + 1]);
    }

    params.set("wt", "xml");
    params.set("indent", params.get("indent", "off"));

    QueryRequest req = new QueryRequest(params);
    String path = params.get("qt");
    if (path != null) {
      req.setPath(path);
    }
    req.setResponseParser(new InputStreamResponseParser("xml"));
    return req;
  }

  public static ModifiableSolrParams params2(String... params) {
    if (params.length % 2 != 0) throw new RuntimeException("Params length should be even");
    ModifiableSolrParams msp = new ModifiableSolrParams();
    for (int i = 0; i < params.length; i += 2) {
      msp.add(params[i], params[i + 1]);
    }
    return msp;
  }

  /**
   * Executes a query using SolrClient with a custom message and validates against XPath
   * expressions.
   *
   * @param message custom error message prefix
   * @param client the SolrClient to execute the query against
   * @param params the query parameters
   * @param tests XPath expressions to validate against the response
   */
  public static void assertQ2(
      String message, SolrClient client, SolrParams params, String... tests) {
    try {
      // Ensure we request XML format for XPath validation
      ModifiableSolrParams xmlParams = new ModifiableSolrParams(params);
      if (xmlParams.get("wt") == null) {
        xmlParams.set("wt", "xml");
      }
      xmlParams.set("indent", xmlParams.get("indent", "off"));

      // Execute the query
      QueryRequest req = new QueryRequest(xmlParams);
      QueryResponse rsp = req.process(client);

      // Convert response to XML for XPath validation
      String xml = toXML(rsp.getResponse());

      // Validate using BaseTestHarness XPath validation
      String results = BaseTestHarness.validateXPath(xml, tests);

      if (results != null) {
        String msg =
            (message == null ? "" : message + " ")
                + "REQUEST FAILED: xpath="
                + results
                + "\n\txml response was: "
                + xml
                + "\n\trequest was: "
                + params;
        throw new RuntimeException(msg);
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Error executing query with SolrClient: " + params, e);
    }
  }

  /**
   * Converts a NamedList response to XML string for XPath validation. This recreates the XML format
   * that Solr produces.
   *
   * @param response the NamedList response to convert
   * @return XML string representation
   */
  private static String toXML(NamedList<Object> response) {
    try {
      StringWriter writer = new StringWriter();

      // Write XML manually since we don't have a full XMLWriter context
      writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
      writeNamedListAsXML(writer, response, "response", 0);

      return writer.toString();
    } catch (Exception e) {
      throw new RuntimeException("Error converting response to XML", e);
    }
  }

  /** Recursively writes a NamedList as XML. */
  private static void writeNamedListAsXML(
      StringWriter writer, NamedList<?> nl, String rootName, int indent) {
    String indentStr = "  ".repeat(indent);

    if (rootName != null) {
      writer.write(indentStr + "<" + rootName + ">\n");
    }

    for (int i = 0; i < nl.size(); i++) {
      String name = nl.getName(i);
      Object value = nl.getVal(i);
      writeValueAsXML(writer, name, value, indent + 1);
    }

    if (rootName != null) {
      writer.write(indentStr + "</" + rootName + ">\n");
    }
  }

  /** Writes a value as XML. */
  private static void writeValueAsXML(StringWriter writer, String name, Object value, int indent) {
    String indentStr = "  ".repeat(indent);

    if (value == null) {
      writer.write(indentStr + "<null name=\"" + xmlEscape(name) + "\"/>\n");
    } else if (value instanceof NamedList) {
      writer.write(indentStr + "<lst name=\"" + xmlEscape(name) + "\">\n");
      NamedList<?> nl = (NamedList<?>) value;
      for (int i = 0; i < nl.size(); i++) {
        writeValueAsXML(writer, nl.getName(i), nl.getVal(i), indent + 1);
      }
      writer.write(indentStr + "</lst>\n");
    } else if (value instanceof Iterable) {
      writer.write(indentStr + "<arr name=\"" + xmlEscape(name) + "\">\n");
      for (Object item : (Iterable<?>) value) {
        writeValueAsXML(writer, null, item, indent + 1);
      }
      writer.write(indentStr + "</arr>\n");
    } else {
      String type = getXMLType(value);
      if (name != null) {
        writer.write(
            indentStr
                + "<"
                + type
                + " name=\""
                + xmlEscape(name)
                + "\">"
                + xmlEscape(String.valueOf(value))
                + "</"
                + type
                + ">\n");
      } else {
        writer.write(
            indentStr + "<" + type + ">" + xmlEscape(String.valueOf(value)) + "</" + type + ">\n");
      }
    }
  }

  /** Determines XML type tag for a value. */
  private static String getXMLType(Object value) {
    if (value instanceof Integer || value instanceof Long) {
      return "int";
    } else if (value instanceof Float || value instanceof Double) {
      return "float";
    } else if (value instanceof Boolean) {
      return "bool";
    } else if (value instanceof java.util.Date) {
      return "date";
    } else {
      return "str";
    }
  }

  /** Escapes XML special characters. */
  private static String xmlEscape(String str) {
    if (str == null) return "";
    return str.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\"", "&quot;")
        .replace("'", "&apos;");
  }
}
