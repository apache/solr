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

package org.apache.solr.cli;

import java.util.Collections;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.client.api.util.SolrVersion;

public class VersionTool extends ToolBase {

  @Override
  public String getName() {
    return "version";
  }

  @Override
  public List<Option> getOptions() {
    return Collections.emptyList();
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    CLIO.out("Solr version is: " + SolrVersion.LATEST);
  }
}
