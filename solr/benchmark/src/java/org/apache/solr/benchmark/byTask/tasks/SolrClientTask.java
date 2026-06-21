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
package org.apache.solr.benchmark.byTask.tasks;

import com.google.common.io.Files;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.tasks.PerfTask;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.solr.benchmark.byTask.util.StreamEater;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.util.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class SolrClientTask extends PerfTask {

  private Config config;
  private PerfRunData runData;
  private String clientName;

  public SolrClientTask(PerfRunData runData) {
    super(runData);
    this.config = runData.getConfig();
    this.runData = runData;
  }

  @Override
  protected String getLogMessage(int recsCount) {
    return "init solr bench done";
  }

  @Override
  public int doLogic() throws Exception {
    String solrServerUrl = config.get("solr.url", null);
    if (solrServerUrl == null) {
      solrServerUrl = "http://127.0.0.1:8901/solr";
    }

    String solrCollection = config.get("solr.collection", "gettingstarted");

    String solrServerClass = config.get("solr." + clientName + ".clientimpl", "Http2SolrClient");

    SolrClient solrClient = null;
    SolrClient solrClientAdmin = null;
    if (solrServerClass != null) {
      System.out.println("server class is:" + solrServerClass);

      if (solrServerClass.equals("Http2SolrClient")) {
        System.out.println("------------> new Http2SolrClient with URL:"
                + solrServerUrl + "/" + solrCollection);
        solrClient = new Http2SolrClient.Builder(solrServerUrl + "/" + solrCollection).idleTimeout((int) TimeUnit.MINUTES.toMillis(10)).build();
        solrClientAdmin = new Http2SolrClient.Builder(solrServerUrl).idleTimeout((int) TimeUnit.MINUTES.toMillis(10)).build();

      } if (solrServerClass.equals("HttpSolrClient")) {
        System.out.println("------------> new HttpSolrClient with URL:"
            + solrServerUrl + "/" + solrCollection);
        solrClient = new HttpSolrClient.Builder(solrServerUrl + "/" + solrCollection).withSocketTimeout((int) TimeUnit.MINUTES.toMillis(10)).build();
        solrClientAdmin = new HttpSolrClient.Builder(solrServerUrl).withSocketTimeout((int) TimeUnit.MINUTES.toMillis(10)).build();

      } if (solrServerClass.equals("CloudHttp2SolrClient")) {
        String zkHost = config.get("solr.zkhost", null).trim();
        if (zkHost == null) {
          throw new RuntimeException(
              "CloudHttp2SolrClient is used with no solr.zkhost specified");
        }
        zkHost = zkHost.replaceAll("\\|", ":");

        String chroot = Files.asCharSource(new File(System.getProperty("solr.checkout") + "/solr/cloud-dev/bench/chroot.txt"), Charset.forName("UTF-8")).read().trim();

        System.out.println("------------> new CloudHttp2SolrClient with ZkHost:"
            + zkHost + " chroot:" + chroot);
        solrClient = new CloudHttp2SolrClient.Builder(Collections.singletonList(zkHost), Optional.of(chroot)).build();
        ((CloudHttp2SolrClient)solrClient).setDefaultCollection(solrCollection);
        ((CloudHttp2SolrClient)solrClient).connect();
        solrClientAdmin = new CloudHttp2SolrClient.Builder(Collections.singletonList(zkHost), Optional.of(chroot)).build();
        ((CloudHttp2SolrClient)solrClientAdmin).setDefaultCollection(solrCollection);
        ((CloudHttp2SolrClient)solrClientAdmin).connect();
      }


      if (solrClient == null) {
        throw new RuntimeException("Could not understand solr client config:"
                + solrServerClass);

      }
    }

    SolrClient client = (SolrClient) runData.getPerfObject("solr.client");
    IOUtils.closeQuietly(client);
    client = (SolrClient) runData.getPerfObject("solr.admin.client");
    IOUtils.closeQuietly(client);

    runData.setPerfObject("solr.client", solrClient);
    runData.setPerfObject("solr.admin.client", solrClientAdmin);

    return 1;
  }

  @Override
  public void close() {
    IOUtils.closeQuietly((SolrClient) runData.getPerfObject("solr.client"));
    IOUtils.closeQuietly((SolrClient) runData.getPerfObject("solr.admin.client"));
  }

  /**
   * Set the params (docSize only)
   * @param params docSize, or 0 for no limit.
   */
  @Override
  public void setParams(String params) {
    super.setParams(params);
    this.clientName = params;
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.tasks.PerfTask#supportsParams()
   */
  @Override
  public boolean supportsParams() {
    return true;
  }

  public static void runCmd(List<String> cmds, String workingDir, boolean log, boolean wait) throws IOException,
          InterruptedException {

    if (log) System.out.println(cmds);

    ProcessBuilder pb = new ProcessBuilder(cmds);
    if (workingDir != null) {
      pb.directory(new File(workingDir));
    }

    Process p = pb.start();

    OutputStream stdout = null;
    OutputStream stderr = null;

    if (log) {
      stdout = System.out;
      stderr = System.err;
    }

    StreamEater se = new StreamEater(p.getInputStream(), stdout);
    se.start();
    StreamEater se2 = new StreamEater(p.getErrorStream(), stderr);
    se2.start();

    if (wait) {
      p.waitFor();
    }
  }

}
