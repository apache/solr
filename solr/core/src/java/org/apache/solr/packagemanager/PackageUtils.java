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
package org.apache.solr.packagemanager;

import static org.apache.solr.client.solrj.util.SolrIdentifierValidator.validateCollectionName;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.JsonMapResponseParser;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.BlobRepository;
import org.apache.solr.filestore.DistribFileStore;
import org.apache.solr.filestore.FileStoreAPI;
import org.apache.solr.packagemanager.SolrPackage.Manifest;
import org.apache.solr.util.SolrJacksonAnnotationInspector;

public class PackageUtils {

  /** Represents a version which denotes the latest version available at the moment. */
  public static String LATEST = "latest";

  public static String PACKAGE_PATH = "/api/cluster/package";
  public static String CLUSTER_PLUGINS_PATH = "/api/cluster/plugin";
  public static String REPOSITORIES_ZK_PATH = "/repositories.json";
  public static String CLUSTERPROPS_PATH = "/api/cluster/zk/data/clusterprops.json";

  public static Configuration jsonPathConfiguration() {
    MappingProvider provider = new JacksonMappingProvider();
    JsonProvider jsonProvider = new JacksonJsonProvider();
    Configuration c =
        Configuration.builder()
            .jsonProvider(jsonProvider)
            .mappingProvider(provider)
            .options(com.jayway.jsonpath.Option.REQUIRE_PROPERTIES)
            .build();
    return c;
  }

  public static ObjectMapper getMapper() {
    return new ObjectMapper().setAnnotationIntrospector(new SolrJacksonAnnotationInspector());
  }

  /**
   * Uploads a file to the package store / file store of Solr.
   *
   * @param client A Solr client
   * @param buffer File contents
   * @param name Name of the file as it will appear in the file store (can be hierarchical)
   * @param sig Signature digest (public key should be separately uploaded to ZK)
   */
  public static void postFile(SolrClient client, ByteBuffer buffer, String name, String sig)
      throws SolrServerException, IOException {
    String resource = "/api/cluster/files" + name;
    ModifiableSolrParams params = new ModifiableSolrParams();
    if (sig != null) {
      params.add("sig", sig);
    }
    GenericSolrRequest request =
        new GenericSolrRequest(SolrRequest.METHOD.PUT, resource, params) {
          @Override
          public RequestWriter.ContentWriter getContentWriter(String expectedType) {
            return new RequestWriter.ContentWriter() {
              public final ByteBuffer payload = buffer;

              @Override
              public void write(OutputStream os) throws IOException {
                if (payload == null) return;
                Channels.newChannel(os).write(payload);
              }

              @Override
              public String getContentType() {
                return "application/octet-stream";
              }
            };
          }
        };
    NamedList<Object> rsp = client.request(request);
    if (!name.equals(rsp.get(CommonParams.FILE))) {
      throw new SolrException(
          ErrorCode.BAD_REQUEST,
          "Mismatch in file uploaded. Uploaded: "
              + rsp.get(CommonParams.FILE)
              + ", Original: "
              + name);
    }
  }

  /** Download JSON from a Solr url and deserialize into klass. */
  public static <T> T getJson(SolrClient client, String path, Class<T> klass) {
    try {
      return getMapper()
          .readValue(getJsonStringFromUrl(client, path, new ModifiableSolrParams()), klass);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Search through the list of jar files for a given file. Returns string of the file contents or
   * null if file wasn't found. This is suitable for looking for manifest or property files within
   * pre-downloaded jar files. Please note that the first instance of the file found is returned.
   */
  public static String getFileFromJarsAsString(List<Path> jars, String filename) {
    for (Path jarfile : jars) {
      try (ZipFile zipFile = new ZipFile(jarfile.toFile())) {
        ZipEntry entry = zipFile.getEntry(filename);
        if (entry == null) continue;
        return new String(zipFile.getInputStream(entry).readAllBytes(), StandardCharsets.UTF_8);
      } catch (Exception ex) {
        throw new SolrException(ErrorCode.BAD_REQUEST, ex);
      }
    }
    return null;
  }

  /** Returns JSON string from a given Solr URL */
  public static String getJsonStringFromUrl(SolrClient client, String path, SolrParams params) {
    try {
      GenericSolrRequest request = new GenericSolrRequest(SolrRequest.METHOD.GET, path, params);
      request.setResponseParser(new JsonMapResponseParser());
      NamedList<Object> response = client.request(request);
      return response.jsonStr();
    } catch (IOException | SolrServerException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Fetches a manifest file from the File Store / Package Store. A SHA512 check is enforced after
   * fetching.
   */
  public static Manifest fetchManifest(
      SolrClient solrClient, String manifestFilePath, String expectedSHA512)
      throws IOException, SolrServerException {
    GenericSolrRequest request =
        new GenericSolrRequest(SolrRequest.METHOD.GET, "/api/node/files" + manifestFilePath);
    request.setResponseParser(new JsonMapResponseParser());
    NamedList<Object> response = solrClient.request(request);
    String manifestJson = (String) response.get("response");
    String calculatedSHA512 =
        BlobRepository.sha512Digest(ByteBuffer.wrap(manifestJson.getBytes(StandardCharsets.UTF_8)));
    if (expectedSHA512.equals(calculatedSHA512) == false) {
      throw new SolrException(
          ErrorCode.UNAUTHORIZED,
          "The manifest SHA512 doesn't match expected SHA512. Possible unauthorized manipulation. "
              + "Expected: "
              + expectedSHA512
              + ", calculated: "
              + calculatedSHA512
              + ", manifest location: "
              + manifestFilePath);
    }
    Manifest manifest = getMapper().readValue(manifestJson, Manifest.class);
    return manifest;
  }

  /**
   * Replace a templatized string with parameter substituted string. First applies the overrides,
   * then defaults and then systemParams.
   */
  public static String resolve(
      String str,
      Map<String, String> defaults,
      Map<String, String> overrides,
      Map<String, String> systemParams) {
    // TODO: Should perhaps use Matchers etc. instead of this clumsy replaceAll().

    if (str == null) return null;
    if (defaults != null) {
      for (Map.Entry<String, String> entry : defaults.entrySet()) {
        String param = entry.getKey();
        str =
            str.replace(
                "${" + param + "}",
                overrides.containsKey(param) ? overrides.get(param) : entry.getValue());
      }
    }
    for (Map.Entry<String, String> entry : overrides.entrySet()) {
      str = str.replace("${" + entry.getKey() + "}", entry.getValue());
    }
    for (Map.Entry<String, String> entry : systemParams.entrySet()) {
      str = str.replace("${" + entry.getKey() + "}", entry.getValue());
    }
    return str;
  }

  public static String BLACK = "\u001B[30m";
  public static String RED = "\u001B[31m";
  public static String GREEN = "\u001B[32m";
  public static String YELLOW = "\u001B[33m";
  public static String BLUE = "\u001B[34m";
  public static String PURPLE = "\u001B[35m";
  public static String CYAN = "\u001B[36m";
  public static String WHITE = "\u001B[37m";

  /** Console print using green color */
  public static void printGreen(Object message) {
    PackageUtils.print(PackageUtils.GREEN, message);
  }

  /** Console print using red color */
  public static void printRed(Object message) {
    PackageUtils.print(PackageUtils.RED, message);
  }

  public static void print(Object message) {
    print(null, message);
  }

  @SuppressForbidden(
      reason = "Need to use System.out.println() instead of log4j/slf4j for cleaner output")
  public static void print(String color, Object message) {
    String RESET = "\u001B[0m";

    if (color != null) {
      System.out.println(color + String.valueOf(message) + RESET);
    } else {
      System.out.println(message);
    }
  }

  public static String[] validateCollections(String collections[]) {
    for (String c : collections) {
      validateCollectionName(c);
    }
    return collections;
  }

  public static String getCollectionParamsPath(String collection) {
    return "/api/collections/" + collection + "/config/params";
  }

  public static void uploadKey(byte[] bytes, String path, Path home) throws IOException {
    FileStoreAPI.MetaData meta = FileStoreAPI._createJsonMetaData(bytes, null);
    DistribFileStore._persistToFile(
        home, path, ByteBuffer.wrap(bytes), ByteBuffer.wrap(Utils.toJSON(meta)));
  }
}
