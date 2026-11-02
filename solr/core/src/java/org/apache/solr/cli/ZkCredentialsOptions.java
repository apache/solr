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

import java.nio.file.Path;
import picocli.CommandLine;

/** Authentication and TLS options for commands that connect directly to ZooKeeper. */
public final class ZkCredentialsOptions {
  @CommandLine.Option(
      names = "--zk-credentials",
      paramLabel = "username:password",
      description = "ZooKeeper digest credentials, separate from HTTP --credentials.")
  String credentials;

  @CommandLine.Option(
      names = "--zk-tls",
      description = "Use TLS for the ZooKeeper connection and verify the server certificate.")
  boolean tls;

  @CommandLine.Option(
      names = "--zk-tls-ca-file",
      paramLabel = "file",
      description = "PEM certificates to trust for ZooKeeper; also enables TLS.")
  Path caFile;

  void configureTls() {
    CLIUtils.configureZkTls(tls, caFile);
  }
}
