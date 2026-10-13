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

import org.apache.solr.logging.DeprecationLog;

/**
 * The old top-level {@code snapshot-list} spelling of {@code bin/solr snapshot list}, kept so that
 * existing scripts keep working. It is hidden from help and the reference guide.
 *
 * @deprecated Use {@code bin/solr snapshot list}; this spelling is removed in Solr 11.
 */
@Deprecated(since = "10.2")
@SuppressWarnings("UnnecessarilyFullyQualified")
@picocli.CommandLine.Command(
    name = "snapshot-list",
    hidden = true,
    description = "Deprecated; use 'snapshot list'.")
public class SnapshotListShim extends SnapshotListTool {

  @Override
  public int callTool() throws Exception {
    DeprecationLog.log(
        "cli.snapshot-list",
        "'bin/solr snapshot-list' is deprecated and will be removed in Solr 11; use 'bin/solr snapshot list'.");
    return super.callTool();
  }
}
