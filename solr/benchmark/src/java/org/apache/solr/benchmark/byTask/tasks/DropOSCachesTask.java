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

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.tasks.PerfTask;
import org.apache.lucene.benchmark.byTask.utils.Config;

import java.util.ArrayList;
import java.util.List;

public class DropOSCachesTask extends PerfTask {


  public DropOSCachesTask(PerfRunData runData) {
    super(runData);
    Config config = runData.getConfig();
  }
  
  @Override
  public void setup() throws Exception {
    super.setup();
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
    List<String> cmd = new ArrayList<String>();
    cmd.add("sudo");
    cmd.add("/home/mm/drop-caches/drop-caches");
    // javaopts last

    System.out.println("linux drop-caches cmd:" + cmd);

    SolrClientTask.runCmd(cmd, System.getProperty("remoteBench"), false, true);

    return 1;
  }


  @Override
  public void setParams(String params) {
    super.setParams(params);
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.benchmark.byTask.tasks.PerfTask#supportsParams()
   */
  @Override
  public boolean supportsParams() {
    return false;
  }
  
}
