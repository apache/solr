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

package org.apache.solr.packagemanager;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.JsonMapResponseParser;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a serializable bean (for the JSON that is stored in /repository.json) representing a
 * repository of Solr packages. Supports standard repositories based on a webservice.
 */
public class DefaultPackageRepository extends PackageRepository {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public DefaultPackageRepository() { // this is needed for deserialization from JSON
  }

  public DefaultPackageRepository(String repositoryName, String repositoryURL) {
    this.name = repositoryName;
    this.repositoryURL = repositoryURL;
  }

  @Override
  public void refresh() {
    packages = null;
  }

  @JsonIgnore private Map<String, SolrPackage> packages;

  @Override
  public Map<String, SolrPackage> getPackages() {
    if (packages == null) {
      initPackages();
    }

    return packages;
  }

  @Override
  public SolrPackage getPackage(String packageName) {
    return getPackages().get(packageName);
  }

  @Override
  public boolean hasPackage(String packageName) {
    return getPackages().containsKey(packageName);
  }

  @Override
  public Path download(String artifactName) throws SolrException, IOException {
    Path tmpDirectory = Files.createTempDirectory("solr-packages");
    tmpDirectory.toFile().deleteOnExit();
    URL url =
        new URL(
            new URL(repositoryURL.endsWith("/") ? repositoryURL : repositoryURL + "/"),
            artifactName);
    String fileName = FilenameUtils.getName(url.getPath());
    Path destination = tmpDirectory.resolve(fileName);

    switch (url.getProtocol()) {
      case "http":
      case "https":
      case "ftp":
        FileUtils.copyURLToFile(url, destination.toFile());
        break;
      default:
        throw new SolrException(
            ErrorCode.BAD_REQUEST, "URL protocol " + url.getProtocol() + " not supported");
    }

    return destination;
  }

  private void initPackages() {
    // We need http 1.1 protocol here because we are talking to the repository server and not to
    // an actual Solr server.
    // We use an Http2SolrClient so that we do not need a raw jetty http client for this GET.
    // We may get a text/plain mimetype for the repository.json (for instance when looking up a repo
    // on Github), so use custom ResponseParser.
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(repositoryURL).useHttp1_1(true).build()) {
      GenericSolrRequest request =
          new GenericSolrRequest(SolrRequest.METHOD.GET, "/repository.json");
      request.setResponseParser(new TalkToRepoResponseParser());
      NamedList<Object> resp = client.request(request);
      SolrPackage[] items =
          PackageUtils.getMapper().readValue("[" + resp.jsonStr() + "]", SolrPackage[].class);
      packages = CollectionUtil.newHashMap(items.length);
      for (SolrPackage pkg : items) {
        pkg.setRepository(name);
        packages.put(pkg.name, pkg);
      }
    } catch (SolrServerException | IOException ex) {
      throw new SolrException(ErrorCode.INVALID_STATE, ex);
    }
    if (log.isDebugEnabled()) {
      log.debug("Found {} packages in repository '{}'", packages.size(), name);
    }
  }

  /**
   * Github links for repository.json are returned in JSON format but with text/plain mimetype, so
   * this works around that issue.
   */
  private static class TalkToRepoResponseParser extends JsonMapResponseParser {

    @Override
    public Collection<String> getContentTypes() {
      return Set.of("application/json", "text/plain");
    }
  }
}
