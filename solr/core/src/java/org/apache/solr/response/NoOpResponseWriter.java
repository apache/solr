package org.apache.solr.response;

import java.io.IOException;
import java.io.Writer;
import org.apache.solr.request.SolrQueryRequest;

public class NoOpResponseWriter implements TextQueryResponseWriter {
  static String MESSAGE = "no operation response writer";

  @Override
  public void write(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    writer.write(MESSAGE);
  }

  @Override
  public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
    return QueryResponseWriter.CONTENT_TYPE_TEXT_UTF8;
  }
}
