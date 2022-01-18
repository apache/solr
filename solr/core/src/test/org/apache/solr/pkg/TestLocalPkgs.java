package org.apache.solr.pkg;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.lang.invoke.MethodHandles;
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

public class TestLocalPkgs extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  public void testLocalPackages() throws Exception {
    String jarName = "mypkg1.jar";
    String PKG_NAME = "mypkg";
    String PKG_DIR_NAME = "localpkgs";
    String COLLECTION_NAME = "testLocalPkgsColl";

    PackageAPI.Packages p = new PackageAPI.Packages();
    PackageAPI.PkgVersion pkgVersion =  new PackageAPI.PkgVersion();
    pkgVersion.files = Collections.singletonList(jarName);
    pkgVersion.version = "0.1";
    pkgVersion.pkg = PKG_NAME;
    p.packages.put(PKG_NAME, Collections.singletonList(pkgVersion));
    log.info("packages.json : "+ Utils.toJSONString(p));
    System.setProperty(PackageLoader.PKGS_DIR, PKG_DIR_NAME);
    MiniSolrCloudCluster cluster =
            configureCluster(4)
                    .withJettyConfig(builder -> builder.enableV2(true))
                    .withJettyConfig(it -> it.withPreStartupHook(jsr -> {
                      try {
                        File pkgDir = new File(jsr.getSolrHome() + File.separator + PKG_DIR_NAME);
                        pkgDir.mkdir();
                        try (FileInputStream fis = new FileInputStream(getFile("runtimecode/runtimelibs.jar.bin"))) {
                          byte[] buf = new byte[fis.available()];

                          fis.read(buf);
                          try( FileOutputStream fos = new FileOutputStream( new File(pkgDir, jarName) )) {
                            fos.write(buf, 0,buf.length);
                          }

                        }

                        try( FileOutputStream fos = new FileOutputStream( new File(pkgDir, "packages.json") )) {
                          fos.write(Utils.toJSON(p));
                        }
                      } catch (Exception e) {
                        throw new RuntimeException("Unable to create files", e);
                      }

                    }))
                    .addConfig("conf", configset("conf2"))
                    .configure();

    try {


      for (JettySolrRunner jsr : cluster.getJettySolrRunners()) {
        List<String> packageFiles = Arrays.asList(new File(jsr.getSolrHome() + File.separator + "localpkgs").list());
       assertTrue(packageFiles.contains("packages.json"));
       assertTrue(packageFiles.contains(jarName));
      }


      CollectionAdminRequest
          .createCollection(COLLECTION_NAME, "conf", 2, 2)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(COLLECTION_NAME, 2, 4);

      log.info("collection created successfully");

      TestPackages.verifyComponent(cluster.getSolrClient(), COLLECTION_NAME, "query", "filterCache", PKG_NAME ,pkgVersion.version );


    } finally {
      cluster.shutdown();
    }
  }
}
