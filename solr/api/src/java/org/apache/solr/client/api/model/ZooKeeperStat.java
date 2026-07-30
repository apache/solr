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
package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Represents the data returned by a ZooKeeper 'stat' call */
public class ZooKeeperStat {
  @JsonProperty("version")
  public int version;

  @JsonProperty("aversion")
  public int aversion;

  @JsonProperty("children")
  public int children;

  @JsonProperty("ctime")
  public long ctime;

  @JsonProperty("cversion")
  public int cversion;

  @JsonProperty("czxid")
  public long czxid;

  @JsonProperty("ephemeralOwner")
  public long ephemeralOwner;

  @JsonProperty("mtime")
  public long mtime;

  @JsonProperty("mzxid")
  public long mzxid;

  @JsonProperty("pzxid")
  public long pzxid;

  @JsonProperty("dataLength")
  public int dataLength;
}
