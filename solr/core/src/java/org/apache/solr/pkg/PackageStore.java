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

package org.apache.solr.pkg;

import static org.apache.solr.common.cloud.ZkStateReader.SOLR_PKGS_PATH;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.solr.client.api.model.AddPackageVersionRequestBody;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.SolrJacksonAnnotationInspector;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds package data loaded from ZooKeeper ({@code /solr/packages.json}) and manages ZK watchers.
 */
public class PackageStore {
  public final boolean enablePackages = EnvUtils.getPropertyAsBool("solr.packages.enabled", false);
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String ERR_MSG =
      "Package loading is not enabled , Start your nodes with -Dsolr.packages.enabled=true";

  final CoreContainer coreContainer;
  final ObjectMapper mapper = SolrJacksonAnnotationInspector.createObjectMapper();
  final SolrPackageLoader packageLoader;
  Packages pkgs;

  public PackageStore(CoreContainer coreContainer, SolrPackageLoader loader) {
    this.coreContainer = coreContainer;
    this.packageLoader = loader;
    pkgs = new Packages();
    SolrZkClient zkClient = coreContainer.getZkController().getZkClient();
    try {
      pkgs = readPkgsFromZk(null, null);
    } catch (KeeperException | InterruptedException e) {
      pkgs = new Packages();
      // ignore
    }
    try {
      registerListener(zkClient);
    } catch (KeeperException | InterruptedException e) {
      SolrZkClient.checkInterrupted(e);
    }
  }

  private void registerListener(SolrZkClient zkClient)
      throws KeeperException, InterruptedException {
    zkClient.exists(
        SOLR_PKGS_PATH,
        new Watcher() {

          @Override
          public void process(WatchedEvent event) {
            // session events are not change events, and do not remove the watcher
            if (Event.EventType.None.equals(event.getType())) {
              return;
            }
            synchronized (this) {
              log.debug("Updating [{}] ... ", SOLR_PKGS_PATH);
              // remake watch
              final Watcher thisWatch = this;
              refreshPackages(thisWatch);
            }
          }
        });
  }

  public void refreshPackages(Watcher watcher) {
    final Stat stat = new Stat();
    try {
      final byte[] data =
          coreContainer.getZkController().getZkClient().getData(SOLR_PKGS_PATH, watcher, stat);
      pkgs = readPkgsFromZk(data, stat);
      packageLoader.refreshPackageConf();
    } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException e) {
      log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK: ", e);
    } catch (KeeperException e) {
      log.error("A ZK error has occurred", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      log.warn("Interrupted", e);
    }
  }

  Packages readPkgsFromZk(byte[] data, Stat stat) throws KeeperException, InterruptedException {

    if (data == null || stat == null) {
      stat = new Stat();
      data = coreContainer.getZkController().getZkClient().getData(SOLR_PKGS_PATH, null, stat);
    }
    Packages packages = null;
    if (data == null || data.length == 0) {
      packages = new Packages();
    } else {
      try {
        packages = mapper.readValue(data, Packages.class);
        packages.znodeVersion = stat.getVersion();
      } catch (IOException e) {
        // invalid data in packages
        // TODO handle properly;
        return new Packages();
      }
    }
    return packages;
  }

  public static class Packages implements ReflectMapWriter {
    @JsonProperty public int znodeVersion = -1;

    @JsonProperty public Map<String, List<PkgVersion>> packages = new LinkedHashMap<>();

    public Packages copy() {
      Packages p = new Packages();
      p.znodeVersion = this.znodeVersion;
      p.packages = new LinkedHashMap<>();
      packages.forEach((s, versions) -> p.packages.put(s, new ArrayList<>(versions)));
      return p;
    }
  }

  public static class PkgVersion implements ReflectMapWriter {

    @JsonProperty("package")
    public String pkg;

    @JsonProperty public String version;

    @JsonProperty public List<String> files;

    @JsonProperty public String manifest;

    @JsonProperty public String manifestSHA512;

    public PkgVersion() {}

    public PkgVersion(String packageName, AddPackageVersionRequestBody addVersion) {
      this.pkg = packageName;
      this.version = addVersion.version;
      this.files = addVersion.files == null ? null : Collections.unmodifiableList(addVersion.files);
      this.manifest = addVersion.manifest;
      this.manifestSHA512 = addVersion.manifestSHA512;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof PkgVersion that) {
        return Objects.equals(this.version, that.version) && Objects.equals(this.files, that.files);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(version);
    }

    @Override
    public String toString() {
      try {
        return Utils.writeJson(this, new StringWriter(), false).toString();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public PkgVersion copy() {
      PkgVersion result = new PkgVersion();
      result.pkg = this.pkg;
      result.version = this.version;
      result.files = this.files;
      result.manifest = this.manifest;
      result.manifestSHA512 = this.manifestSHA512;
      return result;
    }
  }

  public boolean isEnabled() {
    return enablePackages;
  }

  public void handleZkErr(Exception e) {
    log.error("Error reading package config from zookeeper", SolrZkClient.checkInterrupted(e));
  }

  public boolean isJarInuse(String path) {
    Packages pkg = null;
    try {
      pkg = readPkgsFromZk(null, null);
    } catch (KeeperException.NoNodeException nne) {
      return false;
    } catch (InterruptedException | KeeperException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
    for (List<PkgVersion> vers : pkg.packages.values()) {
      for (PkgVersion ver : vers) {
        if (ver.files.contains(path)) {
          return true;
        }
      }
    }
    return false;
  }
}
