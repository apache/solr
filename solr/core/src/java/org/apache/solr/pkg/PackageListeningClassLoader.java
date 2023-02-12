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

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.lucene.util.ResourceLoaderAware;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrClassLoader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrResourceLoader;

/**
 * A {@link SolrClassLoader} that is designed to listen to a set of packages. This class registers a
 * listener for each package that is loaded through this. If any of those packages are updated, the
 * onReload runnable is run
 */
public class PackageListeningClassLoader implements SolrClassLoader, PackageListeners.Listener {
  private final CoreContainer coreContainer;
  private final SolrClassLoader fallbackClassLoader;
  private final Function<String, String> pkgVersionSupplier;
  /** package name and the versions that we are tracking */
  private Map<String, PackageAPI.PkgVersion> packageVersions = new ConcurrentHashMap<>(1);

  private Map<String, String> classNameVsPackageName = new ConcurrentHashMap<>();
  private final Runnable onReload;

  /**
   * @param fallbackClassLoader The {@link SolrClassLoader} to use if no package is specified
   * @param pkgVersionSupplier Get the version configured for a given package
   * @param onReload The callback function that should be run if a package is updated
   */
  public PackageListeningClassLoader(
      CoreContainer coreContainer,
      SolrClassLoader fallbackClassLoader,
      Function<String, String> pkgVersionSupplier,
      Runnable onReload) {
    this.coreContainer = coreContainer;
    this.fallbackClassLoader = fallbackClassLoader;
    this.pkgVersionSupplier = pkgVersionSupplier;
    this.onReload =
        () -> {
          packageVersions = new ConcurrentHashMap<>();
          classNameVsPackageName = new ConcurrentHashMap<>();
          onReload.run();
        };
  }

  @Override
  public <T> T newInstance(String cname, Class<T> expectedType, String... subpackages) {
    PluginInfo.ClassName cName = new PluginInfo.ClassName(cname);
    if (cName.pkg == null) {
      return fallbackClassLoader.newInstance(cname, expectedType, subpackages);
    } else {
      SolrPackageLoader.SolrPackage.Version version = findPackageVersion(cName, true);
      T obj = version.getLoader().newInstance(cName.className, expectedType, subpackages);
      classNameVsPackageName.put(cName.original, cName.pkg);
      return applyResourceLoaderAware(version, obj);
    }
  }

  /**
   * This looks up for package and also listens for that package if required
   *
   * @param cName The class name
   */
  public SolrPackageLoader.SolrPackage.Version findPackageVersion(
      PluginInfo.ClassName cName, boolean registerListener) {
    SolrPackageLoader.SolrPackage p = coreContainer.getPackageLoader().getPackage(cName.pkg);
    if (p == null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "No such package: " + cName.pkg);
    }
    SolrPackageLoader.SolrPackage.Version theVersion =
        p.getLatest(pkgVersionSupplier.apply(cName.pkg));
    if (registerListener) {
      classNameVsPackageName.put(cName.original, cName.pkg);
      PackageAPI.PkgVersion pkgVersion = theVersion.getPkgVersion();
      if (pkgVersion != null) packageVersions.put(cName.pkg, pkgVersion);
    }
    return theVersion;
  }

  @Override
  public MapWriter getPackageVersion(PluginInfo.ClassName cName) {
    if (cName.pkg == null) return null;
    PackageAPI.PkgVersion p = packageVersions.get(cName.pkg);
    return p == null ? null : p::writeMap;
  }

  private <T> T applyResourceLoaderAware(SolrPackageLoader.SolrPackage.Version version, T obj) {
    if (obj instanceof ResourceLoaderAware) {
      SolrResourceLoader.assertAwareCompatibility(ResourceLoaderAware.class, obj);
      try {
        ((ResourceLoaderAware) obj).inform(version.getLoader());
        return obj;
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }
    return obj;
  }

  @Override
  public <T> T newInstance(
      String cname, Class<T> expectedType, String[] subPackages, Class<?>[] params, Object[] args) {
    PluginInfo.ClassName cName = new PluginInfo.ClassName(cname);
    if (cName.pkg == null) {
      return fallbackClassLoader.newInstance(cname, expectedType, subPackages, params, args);
    } else {
      SolrPackageLoader.SolrPackage.Version version = findPackageVersion(cName, true);
      T obj =
          version.getLoader().newInstance(cName.className, expectedType, subPackages, params, args);
      classNameVsPackageName.put(cName.original, cName.pkg);
      return applyResourceLoaderAware(version, obj);
    }
  }

  @Override
  public <T> Class<? extends T> findClass(String cname, Class<T> expectedType) {
    PluginInfo.ClassName cName = new PluginInfo.ClassName(cname);
    if (cName.pkg == null) {
      return fallbackClassLoader.findClass(cname, expectedType);
    } else {
      SolrPackageLoader.SolrPackage.Version version = findPackageVersion(cName, true);
      Class<? extends T> klas = version.getLoader().findClass(cName.className, expectedType);
      classNameVsPackageName.put(cName.original, cName.pkg);
      return klas;
    }
  }

  @Override
  public String packageName() {
    return null;
  }

  @Override
  public Map<String, PackageAPI.PkgVersion> packageDetails() {
    Map<String, PackageAPI.PkgVersion> result = new LinkedHashMap<>();
    classNameVsPackageName.forEach((k, v) -> result.put(k, packageVersions.get(v)));
    return result;
  }

  @Override
  public void changed(SolrPackageLoader.SolrPackage pkg, Ctx ctx) {
    PackageAPI.PkgVersion currVer = packageVersions.get(pkg.name);
    if (currVer == null) {
      // not watching this
      return;
    }
    String latestSupportedVersion = pkgVersionSupplier.apply(pkg.name);
    if (latestSupportedVersion == null) {
      // no specific version configured. use the latest
      latestSupportedVersion = pkg.getLatest().getVersion();
    }
    if (Objects.equals(currVer.version, latestSupportedVersion)) {
      // no need to update
      return;
    }
    doReloadAction(ctx);
  }

  protected void doReloadAction(Ctx ctx) {
    if (onReload == null) return;
    ctx.runLater(null, onReload);
  }
}
