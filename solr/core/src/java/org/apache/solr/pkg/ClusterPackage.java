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

import static org.apache.solr.security.PermissionNameProvider.Name.PACKAGE_EDIT_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.PACKAGE_READ_PERM;

import jakarta.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.Map;
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
import org.apache.solr.core.CoreContainer;
import org.apache.solr.filestore.FileStoreUtils;
import org.apache.solr.jersey.PermissionName;

/**
 * JAX-RS implementation of the package management API ({@code /api/cluster/packages}).
 *
 * @see PackageApis
 */
public class ClusterPackage extends JerseyResource implements PackageApis {
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
      packageStore.syncToVersion(expectedVersion);
    }

    final var response = new ListPackagesResponse();
    populatePackageData(response, packageStore.pkgs);
    return response;
  }

  @Override
  @PermissionName(PACKAGE_READ_PERM)
  public GetPackageResponse getPackage(String packageName, Integer expectedVersion) {
    PackageStore packageStore = coreContainer.getPackageLoader().getPackageStore();

    if (expectedVersion != null) {
      packageStore.syncToVersion(expectedVersion);
    }

    final List<PackageStore.PkgVersion> pkgVersions = packageStore.pkgs.packages.get(packageName);
    if (pkgVersions == null || pkgVersions.isEmpty()) {
      throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such package: " + packageName);
    }

    final var response = new GetPackageResponse();
    response.znodeVersion = packageStore.pkgs.znodeVersion;
    response.name = packageName;
    response.versions =
        pkgVersions.stream().map(ClusterPackage::toPkgVersionResponse).collect(Collectors.toList());
    return response;
  }

  @Override
  @PermissionName(PACKAGE_EDIT_PERM)
  public SolrJerseyResponse addPackageVersion(
      String packageName, AddPackageVersionRequestBody requestBody) {
    final var response = new SolrJerseyResponse();
    PackageStore packageStore = coreContainer.getPackageLoader().getPackageStore();

    Integer newVersion = packageStore.addVersion(packageName, requestBody);
    if (newVersion != null) {
      notifyAllNodesToSync(newVersion);
    }

    return response;
  }

  @Override
  @PermissionName(PACKAGE_EDIT_PERM)
  public SolrJerseyResponse deletePackageVersion(String packageName, String version) {
    final var response = new SolrJerseyResponse();
    PackageStore packageStore = coreContainer.getPackageLoader().getPackageStore();

    packageStore.deleteVersion(packageName, version);

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
    packageStore.syncToVersion(expectedVersion);
    return response;
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

  private static void populatePackageData(
      ListPackagesResponse response, PackageStore.Packages packages) {
    if (packages == null) {
      return;
    }
    response.znodeVersion = packages.znodeVersion;
    response.packages =
        packages.packages.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        e.getValue().stream()
                            .map(ClusterPackage::toPkgVersionResponse)
                            .collect(Collectors.toList())));
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
