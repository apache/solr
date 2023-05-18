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
package org.apache.solr.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.loader.CborStream;

public class TestCborSz extends SolrTestCaseJ4 {

  @SuppressWarnings("unchecked")
  public void test() throws Exception {
    Path filmsJson = new File(ExternalPaths.SOURCE_HOME, "example/films/films.json").toPath();

    System.out.println(filmsJson);
    long sz = Files.size(filmsJson);
    System.out.println("json_sz : " + sz);

    List<Object> films = null;
    try (InputStream is = Files.newInputStream(filmsJson)) {
      films = (List<Object>) Utils.fromJSON(is);
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    new JavaBinCodec().marshal(Map.of("films", films), baos);

    System.out.println("sz_javabin : " + baos.toByteArray().length);

    try (InputStream is = Files.newInputStream(filmsJson)) {
      ObjectMapper jsonMapper = new ObjectMapper(new JsonFactory());

      // Read JSON file as a JsonNode
      JsonNode jsonNode = jsonMapper.readTree(is);
      // Create a CBOR ObjectMapper
      CBORFactory jf = new CBORFactory();
      ObjectMapper cborMapper = new ObjectMapper(jf);
      baos = new ByteArrayOutputStream();
      JsonGenerator jsonGenerator = cborMapper.createGenerator(baos);

      jsonGenerator.writeTree(jsonNode);
      jsonGenerator.close();
      byte[] bytes = baos.toByteArray();
      System.out.println("sz_cbor : " + bytes.length);

      LongAdder docsSz = new LongAdder();
      new CborStream(
              null,
              (document) -> {
                docsSz.increment();
              })
          .stream(new ByteArrayInputStream(bytes));
      assertEquals(films.size(), docsSz.intValue());
    }
  }
}
