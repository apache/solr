package org.apache.solr.response;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import org.junit.Test;

public class NoOpResponseWriterTest {

  @Test
  public void testWrite() throws IOException {
    NoOpResponseWriter writer = new NoOpResponseWriter();

    Writer stringWriter = new StringWriter();

    writer.write(stringWriter, null, null);

    assertEquals(NoOpResponseWriter.MESSAGE, stringWriter.toString());
  }

  @Test
  public void testGetContentType() {
    NoOpResponseWriter writer = new NoOpResponseWriter();

    String contentType = writer.getContentType(null, null);
    assertEquals(QueryResponseWriter.CONTENT_TYPE_TEXT_UTF8, contentType);
  }
}
