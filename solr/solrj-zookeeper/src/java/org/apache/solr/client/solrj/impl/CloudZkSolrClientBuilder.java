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

package org.apache.solr.client.solrj.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.zookeeper.client.ConnectStringParser;

/**
 * A convenience class to more easily build new {@link CloudSolrClient}s to be used with ZooKeeper
 * connections.
 */
public class CloudZkSolrClientBuilder {

  /**
   * Start constructing a new {@link CloudSolrClient} that will use ZooKeeper to discover Solr state
   * and connections.
   *
   * @param zkConnectionString A full ZooKeeper connection string
   * @return A {@link CloudSolrClient.Builder} that can be further customized
   */
  public static CloudSolrClient.Builder from(String zkConnectionString) {
    ConnectStringParser parser = new ConnectStringParser(zkConnectionString);
    return new CloudSolrClient.Builder(
        parser.getServerAddresses().stream()
            .map(address -> address.getHostString() + ":" + address.getPort())
            .collect(Collectors.toList()),
        Optional.ofNullable(parser.getChrootPath()));
  }

  /**
   * Start constructing a new {@link CloudSolrClient} that will use ZooKeeper to discover Solr state
   * and connections.
   *
   * <p>This method does not support a ZooKeeper ChRoot
   *
   * @param zkHosts A list of ZooKeeper hosts to connect to
   * @return A {@link CloudSolrClient.Builder} that can be further customized
   */
  public static CloudSolrClient.Builder from(String... zkHosts) {
    return new CloudSolrClient.Builder(Arrays.asList(zkHosts), Optional.empty());
  }

  /**
   * Start constructing a new {@link CloudSolrClient} that will use ZooKeeper to discover Solr state
   * and connections.
   *
   * @param zkHosts A list of ZooKeeper hosts to connect to.
   * @param chRoot An optional chRoot to use when connecting with ZooKeeper. This should be set
   *     identically to the chRoot your Solr Cloud is using. Set this to null if you do not need a
   *     chroot
   * @return A {@link CloudSolrClient.Builder} that can be further customized
   */
  public static CloudSolrClient.Builder from(List<String> zkHosts, String chRoot) {
    return new CloudSolrClient.Builder(zkHosts, Optional.ofNullable(chRoot));
  }
}
