package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;

/** ResponseParser for JsonMaps. */
public class JsonMapResponseParser extends ResponseParser {
  @Override
  public String getWriterType() {
    return "json";
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public NamedList<Object> processResponse(InputStream body, String encoding) {
    @SuppressWarnings({"rawtypes"})
    Map map = null;
    try (InputStreamReader reader =
        new InputStreamReader(body, encoding == null ? "UTF-8" : encoding)) {
      ObjectBuilder builder = new ObjectBuilder(new JSONParser(reader));
      map = (Map) builder.getObject();
    } catch (IOException | JSONParser.ParseException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "JSON parsing error", e);
    }
    NamedList<Object> list = new NamedList<>();
    list.addAll(map);
    return list;
  }

  @Override
  public NamedList<Object> processResponse(Reader reader) {
    throw new RuntimeException("Cannot handle character stream");
  }

  @Override
  public String getContentType() {
    return "application/json";
  }

  @Override
  public String getVersion() {
    return "2.2";
  }
}
