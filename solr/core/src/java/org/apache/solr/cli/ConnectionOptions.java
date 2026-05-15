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

/**
 * Picocli ArgGroup for mutually-exclusive Solr URL / ZooKeeper connection options.
 *
 * <p>Use as the type of an {@code @ArgGroup(exclusive = true, multiplicity = "0..1")} field to
 * ensure the user provides at most one of {@code --solr-url} or {@code --zk-host}.
 */
class ConnectionOptions {
  @picocli.CommandLine.Option(
      names = {"-s", "--solr-url"},
      description =
          "Base Solr URL, which can be used to determine the zk-host if that's not known.")
  String solrUrl;

  @picocli.CommandLine.Option(
      names = {"-z", "--zk-host"},
      description =
          "Zookeeper connection string; unnecessary if ZK_HOST is defined in solr.in.sh; otherwise, defaults to "
              + CommonCLIOptions.DefaultValues.ZK_HOST
              + ".")
  String zkHost;
}
