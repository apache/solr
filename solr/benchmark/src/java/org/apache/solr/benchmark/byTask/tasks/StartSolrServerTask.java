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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.tasks.PerfTask;
import org.apache.lucene.benchmark.byTask.utils.Config;

public class StartSolrServerTask extends PerfTask {
  
  private String xmx;
  private boolean log = false;
  private boolean enabled = true;

  private String jvms = "4";
  
  public StartSolrServerTask(PerfRunData runData) {
    super(runData);
    System.out.println("Init StartSolrServerTask");
    Config config = runData.getConfig();
    //enabled = config.get("solr.url", null) == null;
  }
  
  @Override
  public void setup() throws Exception {
    super.setup();
    this.xmx = getRunData().getConfig().get("solr.internal.server.xmx", "512M");
  }
  
  @Override
  public void tearDown() throws Exception {

  }
  
  @Override
  protected String getLogMessage(int recsCount) {
    return "started solr server";
  }
  
  @Override
  public int doLogic() throws Exception {
    if (enabled) {
      startSolrExample(jvms, log);
      return 1;
    }
    return 0;
  }
  
  private static void startSolrExample(String jvms, boolean log) throws IOException,
      InterruptedException, TimeoutException {
    System.setProperty("solrcheckout", "/data2/lucene-solr");
    System.out.println("solrcheckout:" + System.getProperty("solrcheckout"));
    File benchFolder = new File("bench");
    List<String> cmd = new ArrayList<String>();
    cmd.add("bash");
    cmd.add(System.getProperty("solrcheckout") + "/solr/cloud-dev/cloud.sh");
    if (!benchFolder.exists()) {
      cmd.add("new");
    } else {
      cmd.add("start");
    }
   // cmd.add("-r");
    cmd.add("-n");
    cmd.add("4");
    cmd.add("-m");
    cmd.add("4g");
    cmd.add("-w");
    cmd.add(System.getProperty("solrcheckout"));
    cmd.add("bench");
//    /cmd.add("cloud.sh new -r -n 4 -m 4g bench");


//    cmd.add(jvms);
//    cmd.add(System.getProperty("solr.checkout"));
//    cmd.add(System.getProperty("remoteBench"));
//    cmd.add(System.getProperty("solr.user"));
//    cmd.add(System.getProperty("solr.host"));
    // javaopts last

   // System.out.println("props:" + System.getProperties());
    System.out.println("startSolrServer:" + cmd);

    SolrClientTask.runCmd(cmd, null, log, true);

    //bash cloud.sh new -r -n $jvmcount -m 4g -w ${lucenecheckout}  -a "${javaopts}" bench
  }

  @Override
  public void setParams(String params) {
    super.setParams(params);
    System.out.println("params:" + params);
    if (params.equalsIgnoreCase("log")) {
      this.log = true;
      System.out.println("------------> logging to stdout with new SolrServer");
    }

    this.params = params;
    String [] splits = params.split(",");
    for (int i = 0; i < splits.length; i++) {
      if (splits[i].equals("log") == true){
        log = true;
      } else if (splits[i].startsWith("jvms[") == true){
        jvms = splits[i].substring("jvms[".length(),splits[i].length() - 1);
      }
    }
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.benchmark.byTask.tasks.PerfTask#supportsParams()
   */
  @Override
  public boolean supportsParams() {
    return true;
  }
  
}
