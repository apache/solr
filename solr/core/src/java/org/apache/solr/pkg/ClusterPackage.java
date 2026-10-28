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
import static org.apache.solr.security.PermissionNameProvider.Name.PACKAGE_EDIT_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.PACKAGE_READ_PERM;

import jakarta.inject.Inject;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.PackageApis;
import org.apache.solr.client.api.model.AddPackageVersionRequestBody;
import org.apache.solr.client.api.model.GetPackageResponse;
import org.apache.solr.client.api.model.ListPackagesResponse;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.PackageApi;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.filestore.FileStoreUtils;
import org.apache.solr.jersey.PermissionName;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JAX-RS implementation of the package management API ({@code /api/cluster/packages}).
 *
 * @see PackageApis
 */
public class ClusterPackage extends JerseyResource implements PackageApis {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int SYNC_MAX_RETRIES = 10;
  private static final long SYNC_SLEEP_MS = 10L;

  private final CoreContainer coreContainer;

  @Inject
  public ClusterPackage(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }

  @Override
  @PermissionName(PACKAGE_READ_PERM)
  public ListPackagesResponse listPackages(Integer expectedVersion) {
    PackageStore packageStore = coreContainer.getPackageLoader().getPackageStore();

    if (expectedVersion != null) {
      syncToVersion(packageStore, expectedVersion);
    }

    final var response = new ListPackagesResponse();
    response.result = toPackageData(packageStore.pkgs);
    return response;
  }

  @Override
  @PermissionName(PACKAGE_READ_PERM)
  public GetPackageResponse getPackage(String packageName, Integer expectedVersion) {
    PackageStore packageStore = coreContainer.getPackageLoader().getPackageStore();

    if (expectedVersion != null) {
      syncToVersion(packageStore, expectedVersion);
    }

    final List<PackageStore.PkgVersion> pkgVersions = packageStore.pkgs.packages.get(packageName);
    if (pkgVersions == null || pkgVersions.isEmpty()) {
      throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such package: " + packageName);
    }

    final var response = new GetPackageResponse();
    response.result = new GetPackageResponse.PackageInfo();
    response.result.znodeVersion = packageStore.pkgs.znodeVersion;
    response.result.name = packageName;
    response.result.versions =
        pkgVersions.stream().map(ClusterPackage::toPkgVersionResponse).collect(Collectors.toList());
    return response;
  }

  @Override
  @PermissionName(PACKAGE_EDIT_PERM)
  public SolrJerseyResponse addPackageVersion(
      String packageName, AddPackageVersionRequestBody requestBody) {
    final var response = new SolrJerseyResponse();
    PackageStore packageStore = coreContainer.getPackageLoader().getPackageStore();

    if (!packageStore.isEnabled()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, PackageStore.ERR_MSG);
    }
    if (requestBody == null || requestBody.files == null || requestBody.files.isEmpty()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No files specified");
    }

    final List<String> errors = new ArrayList<>();
    FileStoreUtils.validateFiles(
        coreContainer.getFileStore(), requestBody.files, true, errors::add);
    if (!errors.isEmpty()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, String.join("; ", errors));
    }

    final PackageStore.Packages[] finalState = new PackageStore.Packages[1];
    try {
      coreContainer
          .getZkController()
          .getZkClient()
          .atomicUpdate(
              SOLR_PKGS_PATH,
              (stat, bytes) -> {
                PackageStore.Packages packages;
                try {
                  packages =
                      bytes == null
                          ? new PackageStore.Packages()
                          : packageStore.mapper.readValue(bytes, PackageStore.Packages.class);
                  packages = packages.copy();
                } catch (IOException e) {
                  log.error("Error deserializing packages.json", e);
                  packages = new PackageStore.Packages();
                }
                List<PackageStore.PkgVersion> list =
                    packages.packages.computeIfAbsent(packageName, o -> new ArrayList<>());
                for (PackageStore.PkgVersion pkgVersion : list) {
                  if (Objects.equals(pkgVersion.version, requestBody.version)) {
                    throw new SolrException(
                        SolrException.ErrorCode.BAD_REQUEST,
                        "Version '" + requestBody.version + "' exists already");
                  }
                }
                list.add(new PackageStore.PkgVersion(packageName, requestBody));
                packages.znodeVersion = stat.getVersion() + 1;
                finalState[0] = packages;
                return Utils.toJSON(packages);
              });
    } catch (KeeperException | InterruptedException e) {
      finalState[0] = null;
      packageStore.handleZkErr(e);
    }

    if (finalState[0] != null) {
      packageStore.pkgs = finalState[0];
      notifyAllNodesToSync(packageStore.pkgs.znodeVersion);
      coreContainer.getPackageLoader().refreshPackageConf();
    }

    return response;
  }

  @Override
  @PermissionName(PACKAGE_EDIT_PERM)
  public SolrJerseyResponse deletePackageVersion(String packageName, String version) {
    final var response = new SolrJerseyResponse();
    PackageStore packageStore = coreContainer.getPackageLoader().getPackageStore();

    if (!packageStore.isEnabled()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, PackageStore.ERR_MSG);
    }

    try {
      coreContainer
          .getZkController()
          .getZkClient()
          .atomicUpdate(
              SOLR_PKGS_PATH,
              (stat, bytes) -> {
                PackageStore.Packages packages;
                try {
                  packages = packageStore.mapper.readValue(bytes, PackageStore.Packages.class);
                  packages = packages.copy();
                } catch (IOException e) {
                  packages = new PackageStore.Packages();
                }

                List<PackageStore.PkgVersion> versions = packages.packages.get(packageName);
                if (versions == null || versions.isEmpty()) {
                  throw new SolrException(
                      SolrException.ErrorCode.NOT_FOUND, "No such package: " + packageName);
                }
                int idxToRemove = -1;
                for (int i = 0; i < versions.size(); i++) {
                  if (Objects.equals(versions.get(i).version, version)) {
                    idxToRemove = i;
                    break;
                  }
                }
                if (idxToRemove == -1) {
                  throw new SolrException(
                      SolrException.ErrorCode.NOT_FOUND, "No such version: " + version);
                }
                versions.remove(idxToRemove);
                packages.znodeVersion = stat.getVersion() + 1;
                return Utils.toJSON(packages);
              });
    } catch (KeeperException | InterruptedException e) {
      packageStore.handleZkErr(e);
    }

    return response;
  }

  @Override
  @PermissionName(PACKAGE_EDIT_PERM)
  public SolrJerseyResponse refreshPackage(String packageName, Boolean localOnly) {
    final var response = new SolrJerseyResponse();
    PackageStore packageStore = coreContainer.getPackageLoader().getPackageStore();

    if (Boolean.TRUE.equals(localOnly)) {
      packageStore.packageLoader.notifyListeners(packageName);
      return response;
    }

    SolrPackageLoader.SolrPackage pkg = coreContainer.getPackageLoader().getPackage(packageName);
    if (pkg == null) {
      throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such package: " + packageName);
    }
    // first refresh on the current node
    packageStore.packageLoader.notifyListeners(packageName);

    final var request = new PackageApi.RefreshPackage(packageName);
    request.setLocalOnly(true);
    broadcastToLiveNodes(request, liveNode -> "Failed to refresh package on node: " + liveNode);

    return response;
  }

  @Override
  @PermissionName(PACKAGE_EDIT_PERM)
  public SolrJerseyResponse syncPackages(Integer expectedVersion) {
    if (expectedVersion == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "expectedVersion is required");
    }
    final var response = new SolrJerseyResponse();
    PackageStore packageStore = coreContainer.getPackageLoader().getPackageStore();
    syncToVersion(packageStore, expectedVersion);
    return response;
  }

  private void syncToVersion(PackageStore packageStore, int expectedVersion) {
    int origVersion = packageStore.pkgs.znodeVersion;
    for (int i = 0; i < SYNC_MAX_RETRIES; i++) {
      if (log.isDebugEnabled()) {
        log.debug(
            "my version is {} , and expected version {}",
            packageStore.pkgs.znodeVersion,
            expectedVersion);
      }
      if (packageStore.pkgs.znodeVersion >= expectedVersion) {
        if (origVersion < packageStore.pkgs.znodeVersion) {
          coreContainer.getPackageLoader().refreshPackageConf();
        }
        return;
      }
      try {
        Thread.sleep(SYNC_SLEEP_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      try {
        packageStore.pkgs = packageStore.readPkgsFromZk(null, null);
      } catch (KeeperException | InterruptedException e) {
        packageStore.handleZkErr(e);
      }
    }
  }

  private void notifyAllNodesToSync(int expectedVersion) {
    final var request = new PackageApi.SyncPackages(expectedVersion);
    broadcastToLiveNodes(
        request,
        liveNode ->
            "Failed to notify node: "
                + liveNode
                + " to sync expected package version: "
                + expectedVersion);
  }

  private void broadcastToLiveNodes(
      SolrRequest<? extends SolrJerseyResponse> request, Function<String, String> errorMessage) {
    for (String liveNode : FileStoreUtils.fetchAndShuffleRemoteLiveNodes(coreContainer)) {
      final var baseUrl =
          coreContainer.getZkController().zkStateReader.getBaseUrlV2ForNodeName(liveNode);
      try {
        var solrClient = coreContainer.getDefaultHttpSolrClient();
        request.processWithBaseUrl(solrClient, baseUrl, null);
      } catch (SolrServerException | IOException e) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, errorMessage.apply(liveNode), e);
      }
    }
  }

  private static ListPackagesResponse.PackageData toPackageData(PackageStore.Packages packages) {
    if (packages == null) {
      return null;
    }
    final var data = new ListPackagesResponse.PackageData();
    data.znodeVersion = packages.znodeVersion;
    data.packages =
        packages.packages.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        e.getValue().stream()
                            .map(ClusterPackage::toPkgVersionResponse)
                            .collect(Collectors.toList())));
    return data;
  }

  private static ListPackagesResponse.PackageVersion toPkgVersionResponse(
      PackageStore.PkgVersion pkgVersion) {
    final var v = new ListPackagesResponse.PackageVersion();
    v.pkg = pkgVersion.pkg;
    v.version = pkgVersion.version;
    v.files = pkgVersion.files;
    v.manifest = pkgVersion.manifest;
    v.manifestSHA512 = pkgVersion.manifestSHA512;
    return v;
  }
}
