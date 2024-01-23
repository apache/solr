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

package org.apache.solr.filestore;

import static org.apache.solr.common.util.Utils.JAVABINCONSUMER;
import static org.apache.solr.core.TestSolrConfigHandler.getFileContent;
import static org.hamcrest.CoreMatchers.containsString;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Predicate;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.client.methods.HttpDelete;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteExecutionException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.NavigableObject;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.packagemanager.PackageUtils;
import org.apache.solr.util.LogLevel;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LogLevel(
    "org.apache.solr.filestore.FileStoreAPI=DEBUG;org.apache.solr.filestore.DistribFileStore=DEBUG")
public class TestDistribFileStore extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Before
  public void setup() {
    System.setProperty("enable.packages", "true");
  }

  @After
  public void teardown() {
    System.clearProperty("enable.packages");
  }

  @Test
  public void testFileStoreManagement() throws Exception {
    MiniSolrCloudCluster cluster =
        configureCluster(4)
            .withJettyConfig(jetty -> jetty.enableV2(true))
            .addConfig("conf", configset("cloud-minimal"))
            .configure();
    try {

      byte[] derFile = readFile("cryptokeys/pub_key512.der");
      uploadKey(derFile, FileStoreAPI.KEYS_DIR + "/pub_key512.der", cluster);

      try {
        postFile(
            cluster.getSolrClient(),
            getFileContent("runtimecode/runtimelibs.jar.bin"),
            "/package/mypkg/v1.0/runtimelibs.jar",
            "j+Rflxi64tXdqosIhbusqi6GTwZq8znunC/dzwcWW0/dHlFGKDurOaE1Nz9FSPJuXbHkVLj638yZ0Lp1ssnoYA==");
        fail("should have failed because of wrong signature ");
      } catch (RemoteExecutionException e) {
        MatcherAssert.assertThat(e.getMessage(), containsString("Signature does not match"));
      }

      postFile(
          cluster.getSolrClient(),
          getFileContent("runtimecode/runtimelibs.jar.bin"),
          "/package/mypkg/v1.0/runtimelibs.jar",
          "L3q/qIGs4NaF6JiO0ZkMUFa88j0OmYc+I6O7BOdNuMct/xoZ4h73aZHZGc0+nmI1f/U3bOlMPINlSOM6LK3JpQ==");

      NavigableObject rsp =
          postFile(
              cluster.getSolrClient(),
              getFileContent("runtimecode/runtimelibs.jar.bin"),
              "/package/mypkg/v1.0/runtimelibs.jar",
              "L3q/qIGs4NaF6JiO0ZkMUFa88j0OmYc+I6O7BOdNuMct/xoZ4h73aZHZGc0+nmI1f/U3bOlMPINlSOM6LK3JpQ==");

      assertTrue(rsp._getStr("message", "").contains("File with same metadata exists "));

      assertResponseValues(
          10,
          cluster.getSolrClient(),
          new V2Request.Builder("/node/files/package/mypkg/v1.0")
              .withMethod(SolrRequest.METHOD.GET)
              .build(),
          Map.of(
              ":files:/package/mypkg/v1.0[0]:name", "runtimelibs.jar",
              ":files:/package/mypkg/v1.0[0]:sha512",
                  "d01b51de67ae1680a84a813983b1de3b592fc32f1a22b662fc9057da5953abd1b72476388ba342cad21671cd0b805503c78ab9075ff2f3951fdf75fa16981420",
              ":files:/package/mypkg/v1.0[0]:sig[0]",
                  "L3q/qIGs4NaF6JiO0ZkMUFa88j0OmYc+I6O7BOdNuMct/xoZ4h73aZHZGc0+nmI1f/U3bOlMPINlSOM6LK3JpQ=="));

      assertResponseValues(
          10,
          cluster.getSolrClient(),
          new V2Request.Builder("/node/files/package/mypkg")
              .withMethod(SolrRequest.METHOD.GET)
              .build(),
          Map.of(
              ":files:/package/mypkg[0]:name", "v1.0",
              ":files:/package/mypkg[0]:dir", "true"));

      Map<String, Object> expected =
          Map.of(
              ":files:/package/mypkg/v1.0/runtimelibs.jar:name", "runtimelibs.jar",
              ":files:/package/mypkg/v1.0/runtimelibs.jar:sha512",
                  "d01b51de67ae1680a84a813983b1de3b592fc32f1a22b662fc9057da5953abd1b72476388ba342cad21671cd0b805503c78ab9075ff2f3951fdf75fa16981420");
      checkAllNodesForFile(cluster, "/package/mypkg/v1.0/runtimelibs.jar", expected, true);
      postFile(
          cluster.getSolrClient(),
          getFileContent("runtimecode/runtimelibs_v2.jar.bin"),
          "/package/mypkg/v1.0/runtimelibs_v2.jar",
          null);
      expected =
          Map.of(
              ":files:/package/mypkg/v1.0/runtimelibs_v2.jar:name",
              "runtimelibs_v2.jar",
              ":files:/package/mypkg/v1.0/runtimelibs_v2.jar:sha512",
              "bc5ce45ad281b6a08fb7e529b1eb475040076834816570902acb6ebdd809410e31006efdeaa7f78a6c35574f3504963f5f7e4d92247d0eb4db3fc9abdda5d417");
      checkAllNodesForFile(cluster, "/package/mypkg/v1.0/runtimelibs_v2.jar", expected, false);
      expected =
          Map.of(
              ":files:/package/mypkg/v1.0",
              (Predicate<Object>)
                  o -> {
                    List<?> l = (List<?>) o;
                    assertEquals(2, l.size());
                    Set<String> expectedKeys = Set.of("runtimelibs_v2.jar", "runtimelibs.jar");
                    for (Object file : l) {
                      if (!expectedKeys.contains(Utils.getObjectByPath(file, true, "name")))
                        return false;
                    }
                    return true;
                  });
      for (JettySolrRunner jettySolrRunner : cluster.getJettySolrRunners()) {
        String baseUrl = jettySolrRunner.getBaseUrl().toString().replace("/solr", "/api");
        String url = baseUrl + "/node/files/package/mypkg/v1.0?wt=javabin";
        assertResponseValues(10, new Fetcher(url, jettySolrRunner), expected);
      }
      // Delete Jars
      DistribFileStore.deleteZKFileEntry(
          cluster.getZkClient(), "/package/mypkg/v1.0/runtimelibs.jar");
      JettySolrRunner j = cluster.getRandomJetty(random());
      String path = j.getBaseURLV2() + "/cluster/files" + "/package/mypkg/v1.0/runtimelibs.jar";
      HttpDelete del = new HttpDelete(path);
      try (HttpSolrClient cl = (HttpSolrClient) j.newClient()) {
        Utils.executeHttpMethod(cl.getHttpClient(), path, Utils.JSONCONSUMER, del);
      }
      expected = Collections.singletonMap(":files:/package/mypkg/v1.0/runtimelibs.jar", null);
      checkAllNodesForFile(cluster, "/package/mypkg/v1.0/runtimelibs.jar", expected, false);
    } finally {
      cluster.shutdown();
    }
  }

  public static void checkAllNodesForFile(
      MiniSolrCloudCluster cluster,
      String path,
      Map<String, Object> expected,
      boolean verifyContent)
      throws Exception {
    for (JettySolrRunner jettySolrRunner : cluster.getJettySolrRunners()) {
      String baseUrl = jettySolrRunner.getBaseUrl().toString().replace("/solr", "/api");
      String url = baseUrl + "/node/files" + path + "?wt=javabin&meta=true";
      assertResponseValues(10, new Fetcher(url, jettySolrRunner), expected);

      if (verifyContent) {
        try (HttpSolrClient solrClient = (HttpSolrClient) jettySolrRunner.newClient()) {
          ByteBuffer buf =
              Utils.executeGET(
                  solrClient.getHttpClient(),
                  baseUrl + "/node/files" + path,
                  Utils.newBytesConsumer(Integer.MAX_VALUE));
          assertEquals(
              "d01b51de67ae1680a84a813983b1de3b592fc32f1a22b662fc9057da5953abd1b72476388ba342cad21671cd0b805503c78ab9075ff2f3951fdf75fa16981420",
              DigestUtils.sha512Hex(new ByteBufferInputStream(buf)));
        }
      }
    }
  }

  public static class Fetcher implements Callable<NavigableObject> {
    String url;
    JettySolrRunner jetty;

    public Fetcher(String s, JettySolrRunner jettySolrRunner) {
      this.url = s;
      this.jetty = jettySolrRunner;
    }

    @Override
    public NavigableObject call() throws Exception {
      try (HttpSolrClient solrClient = (HttpSolrClient) jetty.newClient()) {
        return (NavigableObject)
            Utils.executeGET(solrClient.getHttpClient(), this.url, JAVABINCONSUMER);
      }
    }

    @Override
    public String toString() {
      return url;
    }
  }

  public static <T extends NavigableObject> T assertResponseValues(
      Callable<T> callable, Map<String, Object> vals) throws Exception {
    return assertResponseValues(1, callable, vals);
  }

  public static NavigableObject assertResponseValues(
      int repeats, SolrClient client, SolrRequest<?> req, Map<String, Object> vals)
      throws Exception {
    Callable<NavigableObject> callable = () -> req.process(client);

    return assertResponseValues(repeats, callable, vals);
  }

  /**
   * Evaluate the given predicates or objects against the given values, obtained by running a given
   * callable. The values to verify are either predicates to evaluate directly, or strings to
   * compare for equality.
   *
   * @param repeats how many attempts to make with the Callable
   * @param callable the code to execute getting a result
   * @param vals the values to check in the result, this is a map of paths to predicates or values
   * @return the final passing result of the callable
   * @throws Exception if the callable throws an Exception, or on interrupt between retries
   */
  @SuppressWarnings({"unchecked"})
  public static <T extends NavigableObject> T assertResponseValues(
      int repeats, Callable<T> callable, Map<String, Object> vals) throws Exception {
    T rsp = null;

    for (int i = 0; i < repeats; i++) {
      if (i > 0) {
        Thread.sleep(100);
      }
      try {
        rsp = callable.call();
      } catch (Exception e) {
        if (i >= repeats - 1) throw e;
        continue;
      }
      boolean passed = true;
      for (Map.Entry<String, Object> entry : vals.entrySet()) {
        String k = entry.getKey();
        List<String> key = StrUtils.split(k, '/');

        Object val = entry.getValue();
        // TODO: This map should just be <String,Predicate> and we should instead provide a static
        // eq() method for callers
        Predicate<Object> p =
            val instanceof Predicate
                ? (Predicate<Object>) val
                : o -> {
                  String v = o == null ? null : o.toString();
                  return Objects.equals(val, v);
                };
        Object actual = rsp._get(key, null);
        // Important: check all the values, not just the first one
        passed = passed && p.test(actual);
        if (!passed && i >= repeats - 1) {
          String description = Utils.toJSONString(rsp);
          if (rsp instanceof SimpleSolrResponse) {
            description = ((SimpleSolrResponse) rsp).getResponse().jsonStr();
          }
          // we know these are unequal but call assert instead of fail() because it gives a better
          // error message
          assertEquals(
              "Failed on path " + key + " of " + description + "after attempt #" + (i + 1),
              val,
              Utils.toJSONString(actual));
        }
      }
      if (passed) {
        break;
      }
    }
    return rsp;
  }

  public static void uploadKey(byte[] bytes, String path, MiniSolrCloudCluster cluster)
      throws Exception {
    JettySolrRunner jetty = cluster.getRandomJetty(random());
    try (HttpSolrClient client = (HttpSolrClient) jetty.newClient()) {
      PackageUtils.uploadKey(bytes, path, Paths.get(jetty.getCoreContainer().getSolrHome()));
      String url = jetty.getBaseURLV2() + "/node/files" + path + "?sync=true";
      Object resp = Utils.executeGET(client.getHttpClient(), url, null);
      log.info("sync resp: {} was {}", url, resp);
    }
    checkAllNodesForFile(
        cluster,
        path,
        Map.of(":files:" + path + ":name", (Predicate<Object>) Objects::nonNull),
        false);
  }

  public static NavigableObject postFile(
      SolrClient client, ByteBuffer buffer, String name, String sig)
      throws SolrServerException, IOException {
    String resource = "/cluster/files" + name;
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("sig", sig);
    V2Response rsp =
        new V2Request.Builder(resource)
            .withMethod(SolrRequest.METHOD.PUT)
            .withPayload(buffer)
            .forceV2(true)
            .withMimeType("application/octet-stream")
            .withParams(params)
            .build()
            .process(client);
    assertEquals(name, rsp.getResponse().get(CommonParams.FILE));
    return rsp;
  }

  /**
   * Read and return the contents of the file-like resource
   *
   * @param fname the name of the resource to read
   * @return the bytes of the resource
   * @throws IOException if there is an I/O error reading the contents
   */
  public static byte[] readFile(String fname) throws IOException {
    try (InputStream is = TestDistribFileStore.class.getClassLoader().getResourceAsStream(fname)) {
      return is.readAllBytes();
    }
  }
}
