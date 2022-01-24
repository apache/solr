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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLocalPackages extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static String localPackagesDir = getFile("runtimecode").getAbsolutePath();
  public static String PKG_NAME = "mypkg";
  public static String jarName = "mypkg1.jar";
  public static String COLLECTION_NAME = "testLocalPkgsColl";

  @BeforeClass
  public static void setup() {
    System.setProperty("solr.packages.local.whitelist", PKG_NAME);
    System.setProperty("solr.packages.local.dir", localPackagesDir);
  }

  @AfterClass
  public static void shutdown() {
    System.clearProperty("solr.packages.local.whitelist");
    System.clearProperty("solr.packages.local.dir");
  }

  public void testLocalPackages() throws Exception {

    PackageAPI.Packages p = new PackageAPI.Packages();
    PackageAPI.PkgVersion pkgVersion =  new PackageAPI.PkgVersion();
    pkgVersion.files = Collections.singletonList(jarName);
    pkgVersion.version = "0.1";
    pkgVersion.pkg = PKG_NAME;
    p.packages.put(PKG_NAME, Collections.singletonList(pkgVersion));

    log.info("local_packages.json: " + Utils.toJSONString(p));
    log.info("Local packages dir: " + localPackagesDir);

    MiniSolrCloudCluster cluster =
            configureCluster(4)
                    .withJettyConfig(builder -> builder.enableV2(true))
                    .withJettyConfig(it -> it.withPreStartupHook(jsr -> {
                      try {
                        File pkgDir = new File(localPackagesDir);
                        if(!pkgDir.exists()) {
                          pkgDir.mkdir();
                        }
                        try (FileInputStream fis = new FileInputStream(getFile("runtimecode/runtimelibs.jar.bin"))) {
                          byte[] buf = new byte[fis.available()];

                          fis.read(buf);
                          try( FileOutputStream fos = new FileOutputStream( new File(pkgDir, jarName) )) {
                            fos.write(buf, 0,buf.length);
                          }
                        }

                        try( FileOutputStream fos = new FileOutputStream( new File(pkgDir, PackageLoader.LOCAL_PACKAGES_JSON) )) {
                          fos.write(Utils.toJSON(p));
                        }
                      } catch (Exception e) {
                        throw new RuntimeException("Unable to create files", e);
                      }
                    }))
                    .addConfig("conf", configset("conf2"))
                    .configure();

    System.clearProperty(PackageLoader.LOCAL_PACKAGES_WHITELIST);
    try {
      for (JettySolrRunner jsr : cluster.getJettySolrRunners()) {
        List<String> packageFiles = Arrays.asList(new File(localPackagesDir).list());
        assertTrue(packageFiles.contains(PackageLoader.LOCAL_PACKAGES_JSON));
        assertTrue(packageFiles.contains(jarName));
      }
      CollectionAdminRequest
          .createCollection(COLLECTION_NAME, "conf", 2, 2)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(COLLECTION_NAME, 2, 4);

      log.info("Collection created successfully");

      TestPackages.verifyComponent(cluster.getSolrClient(), COLLECTION_NAME, "query", "filterCache", PKG_NAME ,pkgVersion.version );


    } finally {
      cluster.shutdown();
    }
  }
}
