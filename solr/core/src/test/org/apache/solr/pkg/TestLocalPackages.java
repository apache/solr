package org.apache.solr.pkg;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.pkg.PackageLoader.ENABLED_LOCAL_PKGS_PROP;
import static org.apache.solr.pkg.PackageLoader.LOCAL_PKGS_DIR_PROP;

public class TestLocalPackages extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public void testLocalPackagesAsDir() throws Exception {
    String PKG_NAME = "mypkg";
    String jarName = "mypkg1.jar";
    String COLLECTION_NAME = "testLocalPkgsColl";
    String localPackagesDir = "testpkgdir";
    System.setProperty(ENABLED_LOCAL_PKGS_PROP, PKG_NAME);
    System.setProperty(LOCAL_PKGS_DIR_PROP, localPackagesDir);
    MiniSolrCloudCluster cluster =
            configureCluster(4)
                    .withJettyConfig(builder -> builder.enableV2(true))
                    .withJettyConfig(it -> it.withPreStartupHook(jsr -> {
                      try {
                        Files.write(Files.createDirectories(Path.of(jsr.getSolrHome(), localPackagesDir, PKG_NAME)).resolve(jarName),
                                Files.readAllBytes(getFile("runtimecode/runtimelibs.jar.bin").toPath()));
                      } catch (Exception e) {
                        throw new RuntimeException("Unable to create files", e);
                      }
                    }))
                    .addConfig("conf", configset("conf2"))
                    .configure();

    System.clearProperty(ENABLED_LOCAL_PKGS_PROP);
    System.clearProperty(LOCAL_PKGS_DIR_PROP);
    try {
      for (JettySolrRunner jsr : cluster.getJettySolrRunners()) {
        List<String> packageFiles = Arrays.asList(new File(jsr.getSolrHome() + File.separator + localPackagesDir + File.separator + PKG_NAME).list());
        assertTrue(packageFiles.contains(jarName));
      }
      CollectionAdminRequest
              .createCollection(COLLECTION_NAME, "conf", 2, 2)
              .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(COLLECTION_NAME, 2, 4);

      log.info("Collection created successfully");

      TestPackages.verifyComponent(cluster.getSolrClient(), COLLECTION_NAME, "query", "filterCache", PKG_NAME, "0");
    } finally {
      cluster.shutdown();
    }
  }

  public void testLocalPackages() throws Exception {
    String PKG_NAME = "mypkg";
    String jarName = "mypkg1.jar";
    String COLLECTION_NAME = "testLocalPkgsColl";
    String localPackagesDir = "local_packages";
    PackageAPI.Packages p = new PackageAPI.Packages();
    PackageAPI.PkgVersion pkgVersion = new PackageAPI.PkgVersion();
    pkgVersion.files = Collections.singletonList(jarName);
    pkgVersion.version = "0.1";
    pkgVersion.pkg = PKG_NAME;
    p.packages.put(PKG_NAME, Collections.singletonList(pkgVersion));

    log.info("local_packages.json: {}" , Utils.toJSONString(p));
    log.info("Local packages dir: {}" , localPackagesDir);
    System.setProperty(ENABLED_LOCAL_PKGS_PROP, PKG_NAME);
    System.setProperty(LOCAL_PKGS_DIR_PROP, localPackagesDir);
    MiniSolrCloudCluster cluster =
            configureCluster(4)
                    .withJettyConfig(builder -> builder.enableV2(true))
                    .withJettyConfig(it -> it.withPreStartupHook(jsr -> {
                      try {
                        Path pkgDir = Files.createDirectories(Path.of(jsr.getSolrHome(), localPackagesDir));
                        Files.write(pkgDir.resolve(jarName), Files.readAllBytes(getFile("runtimecode/runtimelibs.jar.bin").toPath()));
                        Files.write(pkgDir.resolve(PackageLoader.LOCAL_PACKAGES_JSON), Utils.toJSON(p));
                      } catch (Exception e) {
                        throw new RuntimeException("Unable to create files", e);
                      }
                    }))
                    .addConfig("conf", configset("conf2"))
                    .configure();

    System.clearProperty(ENABLED_LOCAL_PKGS_PROP);
    System.clearProperty(LOCAL_PKGS_DIR_PROP);
    try {
      for (JettySolrRunner jsr : cluster.getJettySolrRunners()) {
        List<String> packageFiles = Arrays.asList(new File(jsr.getSolrHome() + File.separator + localPackagesDir).list());
        assertTrue(packageFiles.contains(PackageLoader.LOCAL_PACKAGES_JSON));
        assertTrue(packageFiles.contains(jarName));
      }
      CollectionAdminRequest
              .createCollection(COLLECTION_NAME, "conf", 2, 2)
              .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(COLLECTION_NAME, 2, 4);

      log.info("Collection created successfully");

      TestPackages.verifyComponent(cluster.getSolrClient(), COLLECTION_NAME, "query", "filterCache", PKG_NAME, pkgVersion.version);


    } finally {
      cluster.shutdown();
    }
  }
}
