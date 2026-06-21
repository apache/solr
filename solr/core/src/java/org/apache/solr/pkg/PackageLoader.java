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

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.lucene.util.IOUtils.closeWhileHandlingException;

/**
 * The class that holds a mapping of various packages and classloaders
 */
public class PackageLoader implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CoreContainer coreContainer;
  private final Map<String, Package> packageClassLoaders = new NonBlockingHashMap<>();

  private PackageAPI.Packages myCopy =  new PackageAPI.Packages();

  private PackageAPI packageAPI;


  public PackageLoader(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
    packageAPI = new PackageAPI(coreContainer, this);
    refreshPackageConf();
  }

  public PackageAPI getPackageAPI() {
    return packageAPI;
  }

  public Package getPackage(String key) {
    return packageClassLoaders.get(key);
  }

  public static Map<String, Package> getPackages() {
    return Collections.emptyMap();
  }

  public void refreshPackageConf() {
    if (log.isDebugEnabled()) {
      log.debug("{} updated to version {}", ZkStateReader.SOLR_PKGS_PATH, packageAPI.pkgs.znodeVersion);
    }

    Map<String, List<PackageAPI.PkgVersion>> modified = getModified(myCopy, packageAPI.pkgs);
    List<Package> updated = new ArrayList<>(modified.size());
    modified.forEach((key, value) -> {
      if (value != null) {
        Package p = packageClassLoaders.get(key);
        if (value != null && p == null) {
          packageClassLoaders.put(key, p = new Package(this, key));
        }
        p.updateVersions(value);
        updated.add(p);
      } else {
        Package p = packageClassLoaders.remove(key);
        if (p != null) {
          //other classes are holding to a reference to this object
          // they should know that this is removed
          p.markDeleted();
          closeWhileHandlingException(p);
        }
      }
    });
    for (SolrCore core : coreContainer.getCores()) {
      core.getPackageListeners().packagesUpdated(updated);
    }
    myCopy = packageAPI.pkgs;
  }

  public static Map<String, List<PackageAPI.PkgVersion>> getModified(PackageAPI.Packages old, PackageAPI.Packages newPkgs) {
    Map<String, List<PackageAPI.PkgVersion>> changed = new HashMap<>();
    newPkgs.packages.forEach((key, value) -> {
      List<PackageAPI.PkgVersion> versions = old.packages.get(key);
      if (versions != null) {
        if (!Objects.equals(value, versions)) {
          if (log.isInfoEnabled()) {
            log.info("Package {} is modified ", key);
          }
          changed.put(key, value);
        }
      } else {
        if (log.isInfoEnabled()) {
          log.info("A new package: {} introduced", key);
        }
        changed.put(key, value);
      }
    });
    //some packages are deleted altogether
    for (String s : old.packages.keySet()) {
      if (!newPkgs.packages.containsKey(s)) {
        log.info("Package: {} is removed althogether", s);
        changed.put(s, null);
      }
    }

    return changed;

  }

  public void notifyListeners(String pkg) {
    Package p = packageClassLoaders.get(pkg);
    if (p != null) {
      List<Package> l = Collections.singletonList(p);
      try (ParWork work = new ParWork(this, false)) {
        for (SolrCore core : coreContainer.getCores()) {
          work.collect("packageListeners", () -> core.getPackageListeners().packagesUpdated(l));
        }
      }
    }
  }

  /**
   * represents a package definition in the packages.json
   */
  public static class Package implements Closeable {
    final String name;
    final Map<String, Version> myVersions = new NonBlockingHashMap<>();
    private final List<String> sortedVersions = new CopyOnWriteArrayList<>();
    final AtomicReference<String> latest = new AtomicReference<>();
    private boolean deleted;
    private PackageLoader packageLoader;

    Package(PackageLoader packageLoader, String name) {
      this.name = name;
      this.packageLoader = packageLoader;
    }

    public boolean isDeleted() {
      return deleted;
    }

    private static String findBiggest(String lessThan, List<String> sortedList) {
      String latest = null;
      for (String v : sortedList) {
        if (v.compareTo(lessThan) < 1) {
          latest = v;
        } else break;
      }
      return latest;
    }

    private void updateVersions(List<PackageAPI.PkgVersion> modified) {
      for (PackageAPI.PkgVersion v : modified) {
        Version version = myVersions.get(v.version);
        if (version == null) {
          log.info("A new version: {} added for package: {} with artifacts {}", v.version, this.name, v.files);
          Version ver = null;
          try {
            ver = new Version(name ,packageLoader, v);
          } catch (Exception e) {
            log.error("package could not be loaded {}", ver, e);
            continue;
          }
          myVersions.put(v.version, ver);
          sortedVersions.add(v.version);
        }
      }

      Set<String> newVersions = new HashSet<>();
      for (PackageAPI.PkgVersion v : modified) {
        newVersions.add(v.version);
      }
      for (String s : new HashSet<>(myVersions.keySet())) {
        if (!newVersions.contains(s)) {
          log.info("version: {} is removed from package: {}", s, this.name);
          sortedVersions.remove(s);
          Version removed = myVersions.remove(s);
          if (removed != null) {
            closeWhileHandlingException(removed);
          }
        }
      }

      sortedVersions.sort(String::compareTo);
      if (!sortedVersions.isEmpty()) {
        String latest = sortedVersions.get(sortedVersions.size() - 1);
        String currentLatest = this.latest.get();
        if (!latest.equals(currentLatest)) {
          log.info("version: {} is the new latest in package: {}", latest, this.name);
        }
        this.latest.set(latest);
      } else {
        log.error("latest version:  null");
        latest.set(null);
      }
    }


    public Version getLatest() {
      return latest.get() == null ? null : myVersions.get(latest.get());
    }

    public Version getLatest(String lessThan) {
      if (lessThan == null) {
        return getLatest();
      }
      String latest = findBiggest(lessThan, new ArrayList(sortedVersions));
      return latest == null ? null : myVersions.get(latest);
    }

    public String name() {
      return name;
    }

    private void markDeleted() {
      deleted = true;
      myVersions.clear();
      sortedVersions.clear();
      latest.set(null);
    }

    @Override
    public void close() throws IOException {
      for (Version v : myVersions.values()) v.close();
    }

    public static class Version implements MapWriter, Closeable {
      private final PackageLoader packageLoader;
      private final String name;
      private SolrResourceLoader loader;

      private final PackageAPI.PkgVersion version;

      @Override
      public void writeMap(EntryWriter ew) throws IOException {
        ew.put("package", name);
        version.writeMap(ew);
      }

      Version(String name, PackageLoader packageLoader, PackageAPI.PkgVersion v) {
        this.packageLoader = packageLoader;
        version = v;
        this.name = name;
        List<Path> paths = new ArrayList<>(version.files.size());

        List<String> errs = new ArrayList<>();
        packageLoader.coreContainer.getPackageStoreAPI().validateFiles(version.files, true, errs::add);
        if(!errs.isEmpty()) {
          throw new RuntimeException("Cannot load package: " +errs);
        }
        for (String file : version.files) {
          paths.add(packageLoader.coreContainer.getPackageStoreAPI().getPackageStore().getRealpath(file));
        }

        loader = new PackageResourceLoader(
            "PACKAGE_LOADER: " + name + ":" + version,
            paths,
            Paths.get(packageLoader.coreContainer.getSolrHome()),
            packageLoader.coreContainer.getResourceLoader());
      }

      public String getVersion() {
        return version.version;
      }

      public Collection getFiles() {
        return Collections.unmodifiableList(version.files);
      }

      public SolrResourceLoader getLoader() {
        return loader;
      }

      @Override
      public void close() throws IOException {
        if (loader != null) {
          closeWhileHandlingException(loader);
        }
      }

      @Override
      public String toString() {
        return jsonStr();
      }
    }
  }
  static class PackageResourceLoader extends SolrResourceLoader {

    PackageResourceLoader(String name, List<Path> classpath, Path instanceDir, SolrResourceLoader parent) {
      super(name, classpath, instanceDir, parent);
    }

    @Override
    public <T> boolean addToCoreAware(T obj) {
      //do not do anything
      //this class is not aware of a SolrCore and it is totally not tied to
      // the lifecycle of SolrCore. So, this returns 'false' & it should be
      // taken care of by the caller
      return false;
    }

    @Override
    public <T> boolean addToResourceLoaderAware(T obj) {
      // do not do anything
      // this should be invoked only after the init() is invoked.
      // The caller should take care of that
      return false;
    }

    @Override
    public  <T> void addToInfoBeans(T obj) {
      //do not do anything. It should be handled externally
    }
  }

  @Override
  public void close()  {
    for (Package p : packageClassLoaders.values()) closeWhileHandlingException(p);
  }
}
