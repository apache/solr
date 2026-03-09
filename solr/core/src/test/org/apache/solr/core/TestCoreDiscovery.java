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
package org.apache.solr.core;

import static org.apache.solr.core.CoreContainer.CORE_DISCOVERY_COMPLETE;
import static org.apache.solr.core.CoreContainer.INITIAL_CORE_LOAD_COMPLETE;
import static org.apache.solr.core.CoreContainer.LOAD_COMPLETE;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.StringContains.containsString;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.AssumptionViolatedException;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCoreDiscovery extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore();
  }

  private final Path solrHomeDirectory = createTempDir();

  private void setMeUp(String alternateCoreDir) throws Exception {
    String xmlStr = SOLR_XML;
    if (alternateCoreDir != null) {
      xmlStr =
          xmlStr.replace(
              "<solr>", "<solr> <str name=\"coreRootDirectory\">" + alternateCoreDir + "</str> ");
    }
    Path tmpFile = solrHomeDirectory.resolve(SolrXmlConfig.SOLR_XML_FILE);
    Files.writeString(tmpFile, xmlStr, StandardCharsets.UTF_8);
  }

  private void setMeUp() throws Exception {
    setMeUp(null);
  }

  private Properties makeCoreProperties(String name, boolean loadOnStartup, String... extraProps) {
    Properties props = new Properties();
    props.put(CoreDescriptor.CORE_NAME, name);
    props.put(CoreDescriptor.CORE_SCHEMA, "schema-tiny.xml");
    props.put(CoreDescriptor.CORE_CONFIG, "solrconfig-minimal.xml");
    props.put(CoreDescriptor.CORE_LOADONSTARTUP, Boolean.toString(loadOnStartup));
    props.put(CoreDescriptor.CORE_DATADIR, "${core.dataDir:stuffandnonsense}");

    for (String extra : extraProps) {
      String[] parts = extra.split("=");
      props.put(parts[0], parts[1]);
    }

    return props;
  }

  private void addCoreWithProps(Properties stockProps, Path propFile) throws Exception {
    if (!Files.exists(propFile.getParent())) {
      Files.createDirectories(propFile.getParent());
    }
    try (Writer out = Files.newBufferedWriter(propFile, StandardCharsets.UTF_8)) {
      stockProps.store(out, null);
    }
    addConfFiles(propFile.getParent().resolve("conf"));
  }

  private void addCoreWithProps(String name, Properties stockProps) throws Exception {
    Path propFile =
        solrHomeDirectory.resolve(name).resolve(CorePropertiesLocator.PROPERTIES_FILENAME);
    Path parent = propFile.getParent();
    try {
      Files.createDirectories(parent);
    } catch (Exception e) {
      throw new IOException("Failed to mkdirs for " + parent.toAbsolutePath());
    }
    addCoreWithProps(stockProps, propFile);
  }

  private void addConfFiles(Path confDir) throws Exception {
    Path top = SolrTestCaseJ4.TEST_HOME().resolve("collection1").resolve("conf");
    Files.createDirectories(confDir);
    Files.copy(top.resolve("schema-tiny.xml"), confDir.resolve("schema-tiny.xml"));
    Files.copy(top.resolve("solrconfig-minimal.xml"), confDir.resolve("solrconfig-minimal.xml"));
    Files.copy(
        top.resolve("solrconfig.snippet.randomindexconfig.xml"),
        confDir.resolve("solrconfig.snippet.randomindexconfig.xml"));
  }

  private CoreContainer init() {
    final CoreContainer container = new CoreContainer(solrHomeDirectory, new Properties());
    try {
      container.load();
    } catch (Exception e) {
      container.shutdown();
      throw e;
    }

    long status = container.getStatus();

    assertEquals("Load complete flag should be set", LOAD_COMPLETE, (status & LOAD_COMPLETE));
    assertEquals(
        "Core discovery should be complete",
        CORE_DISCOVERY_COMPLETE,
        (status & CORE_DISCOVERY_COMPLETE));
    assertEquals(
        "Initial core loading should be complete",
        INITIAL_CORE_LOAD_COMPLETE,
        (status & INITIAL_CORE_LOAD_COMPLETE));
    return container;
  }

  // Test the basic setup, create some dirs with core.properties files in them, but solr.xml has
  // discoverCores set and ensure that we find all the cores and can load them.
  @Test
  @SuppressWarnings({"try"})
  public void testDiscovery() throws Exception {
    setMeUp();

    // name, loadOnStartup
    addCoreWithProps("core1", makeCoreProperties("core1", true, "dataDir=core1"));
    addCoreWithProps("core2", makeCoreProperties("core2", false, "dataDir=core2"));

    CoreContainer cc = init();
    try {

      TestLazyCores.checkLoadedCores(cc, "core1");
      TestLazyCores.checkCoresNotLoaded(cc, "core2");

      // force loading of core2 by getting it from the CoreContainer
      try (SolrCore core1 = cc.getCore("core1");
          SolrCore core2 = cc.getCore("core2")) {

        // Let's assert we did the right thing for implicit properties too.
        CoreDescriptor desc = core1.getCoreDescriptor();
        assertEquals("core1", desc.getName());

        // This is too long and ugly to put in. Besides, it varies.
        assertNotNull(desc.getInstanceDir());

        assertEquals("core1", desc.getDataDir());
        assertEquals("solrconfig-minimal.xml", desc.getConfigName());
        assertEquals("schema-tiny.xml", desc.getSchemaName());

        TestLazyCores.checkLoadedCores(cc, "core1", "core2");
        // Can we persist an existing core's properties?

        // Insure we can persist a new properties file if we want.
        CoreDescriptor cd1 = core1.getCoreDescriptor();
        Properties persistable = cd1.getPersistableUserProperties();
        persistable.setProperty("bogusprop", "bogusval");
        cc.getCoresLocator().persist(cc, cd1);
        Path propFile =
            solrHomeDirectory.resolve("core1").resolve(CorePropertiesLocator.PROPERTIES_FILENAME);
        Properties newProps = new Properties();
        try (InputStreamReader is =
            new InputStreamReader(Files.newInputStream(propFile), StandardCharsets.UTF_8)) {
          newProps.load(is);
        }
        // is it there?
        assertEquals(
            "Should have persisted bogusprop to disk",
            "bogusval",
            newProps.getProperty("bogusprop"));
        // is it in the user properties?
        CorePropertiesLocator cpl = new CorePropertiesLocator(solrHomeDirectory);
        List<CoreDescriptor> cores = cpl.discover(cc);
        boolean found = false;
        for (CoreDescriptor cd : cores) {
          if (cd.getName().equals("core1")) {
            found = true;
            assertEquals(
                "Should have persisted bogusprop to disk in user properties",
                "bogusval",
                cd.getPersistableUserProperties().getProperty("bogusprop"));
            break;
          }
        }
        assertTrue("Should have found core descriptor for core1", found);
      }

    } finally {
      cc.shutdown();
    }
  }

  @Test
  public void testPropFilePersistence() throws Exception {
    setMeUp();

    // Test that an existing core.properties file is _not_ deleted if the core fails to load.
    Properties badProps = makeCoreProperties("corep1", true);
    badProps.setProperty(CoreDescriptor.CORE_SCHEMA, "not-there.xml");

    addCoreWithProps("corep1", badProps);
    // Sanity check that a core did get loaded
    addCoreWithProps("corep2", makeCoreProperties("corep2", true));

    Path coreP1PropFile = solrHomeDirectory.resolve("corep1").resolve("core.properties");
    assertTrue(
        "Core.properties file should exist for before core load failure core corep1",
        Files.exists(coreP1PropFile));

    CoreContainer cc = init();
    try {
      Exception thrown =
          expectThrows(SolrCoreInitializationException.class, () -> cc.getCore("corep1"));
      assertTrue(thrown.getMessage().contains("init failure"));
      try (SolrCore sc = cc.getCore("corep2")) {
        assertNotNull("Core corep2 should be loaded", sc);
      }
      assertTrue(
          "Core.properties file should still exist for core corep1", Files.exists(coreP1PropFile));

      // Creating a core successfully should create a core.properties file
      Path corePropFile = solrHomeDirectory.resolve("corep3").resolve("core.properties");
      assertFalse("Should not be a properties file yet", Files.exists(corePropFile));
      cc.create("corep3", Map.of("configSet", "minimal"));
      assertTrue("Should be a properties file for newly created core", Files.exists(corePropFile));

      // Failing to create a core should _not_ leave a core.properties file hanging around.
      corePropFile = solrHomeDirectory.resolve("corep4").resolve("core.properties");
      assertFalse("Should not be a properties file yet for corep4", Files.exists(corePropFile));

      thrown =
          expectThrows(
              SolrException.class,
              () -> {
                cc.create(
                    "corep4",
                    Map.of(
                        CoreDescriptor.CORE_NAME, "corep4",
                        CoreDescriptor.CORE_SCHEMA, "not-there.xml",
                        CoreDescriptor.CORE_CONFIG, "solrconfig-minimal.xml",
                        CoreDescriptor.CORE_LOADONSTARTUP, "true"));
              });
      assertTrue(thrown.getMessage().contains("Can't find resource"));
      assertFalse(
          "Failed corep4 should not have left a core.properties file around",
          Files.exists(corePropFile));

      // Finally, let's determine that this create path operation also leaves a prop file.

      corePropFile = solrHomeDirectory.resolve("corep5").resolve("core.properties");
      assertFalse("Should not be a properties file yet for corep5", Files.exists(corePropFile));

      cc.create("corep5", Map.of("configSet", "minimal"));

      assertTrue(
          "corep5 should have left a core.properties file on disk", Files.exists(corePropFile));

    } finally {
      cc.shutdown();
    }
  }

  @Test
  public void testDuplicateNames() throws Exception {
    setMeUp();

    // name, isLazy, loadOnStartup
    addCoreWithProps("core1", makeCoreProperties("core1", true));
    addCoreWithProps("core2", makeCoreProperties("core2", false, "name=core1"));
    SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              CoreContainer cc = null;
              try {
                cc = init();
              } finally {
                if (cc != null) cc.shutdown();
              }
            });
    final String message = thrown.getMessage();
    assertTrue(
        "Wrong exception thrown on duplicate core names",
        message.contains("Found multiple cores with the name [core1]"));
    assertTrue(
        FileSystems.getDefault().getSeparator()
            + "core1 should have been mentioned in the message: "
            + message,
        message.contains(FileSystems.getDefault().getSeparator() + "core1"));
    assertTrue(
        FileSystems.getDefault().getSeparator()
            + "core2 should have been mentioned in the message:"
            + message,
        message.contains(FileSystems.getDefault().getSeparator() + "core2"));
  }

  @Test
  public void testAlternateCoreDir() throws Exception {

    Path alt = createTempDir();

    setMeUp(alt.toAbsolutePath().toString());
    addCoreWithProps(
        makeCoreProperties("core1", true, "dataDir=core1"),
        alt.resolve("core1").resolve(CorePropertiesLocator.PROPERTIES_FILENAME));
    addCoreWithProps(
        makeCoreProperties("core2", false, "dataDir=core2"),
        alt.resolve("core2").resolve(CorePropertiesLocator.PROPERTIES_FILENAME));
    CoreContainer cc = init();
    try (SolrCore core1 = cc.getCore("core1");
        SolrCore core2 = cc.getCore("core2")) {
      assertNotNull(core1);
      assertNotNull(core2);
    } finally {
      cc.shutdown();
    }
  }

  @Test
  public void testAlternateRelativeCoreDir() throws Exception {

    String relative = "relativeCoreDir";

    setMeUp(relative);
    // two cores under the relative directory
    addCoreWithProps(
        makeCoreProperties("core1", true, "dataDir=core1"),
        solrHomeDirectory
            .resolve(relative)
            .resolve("core1")
            .resolve(CorePropertiesLocator.PROPERTIES_FILENAME));
    addCoreWithProps(
        makeCoreProperties("core2", false, "dataDir=core2"),
        solrHomeDirectory
            .resolve(relative)
            .resolve("core2")
            .resolve(CorePropertiesLocator.PROPERTIES_FILENAME));
    // one core *not* under the relative directory
    addCoreWithProps(
        makeCoreProperties("core0", true, "datadir=core0"),
        solrHomeDirectory.resolve("core0").resolve(CorePropertiesLocator.PROPERTIES_FILENAME));

    CoreContainer cc = init();
    try (SolrCore core1 = cc.getCore("core1");
        SolrCore core2 = cc.getCore("core2")) {
      assertNotNull(core1);
      assertNotNull(core2);

      assertNull(cc.getCore("core0"));

      SolrCore core3 = cc.create("core3", Map.of("configSet", "minimal"));
      assertThat(core3.getCoreDescriptor().getInstanceDir().toString(), containsString("relative"));

    } finally {
      cc.shutdown();
    }
  }

  @Test
  public void testNoCoreDir() throws Exception {
    Path noCoreDir = createTempDir();
    setMeUp(noCoreDir.toAbsolutePath().toString());
    addCoreWithProps(
        makeCoreProperties("core1", true),
        noCoreDir.resolve("core1").resolve(CorePropertiesLocator.PROPERTIES_FILENAME));
    addCoreWithProps(
        makeCoreProperties("core2", false),
        noCoreDir.resolve("core2").resolve(CorePropertiesLocator.PROPERTIES_FILENAME));
    CoreContainer cc = init();
    try (SolrCore core1 = cc.getCore("core1");
        SolrCore core2 = cc.getCore("core2")) {
      assertNotNull(core1);
      assertNotNull(core2);
    } finally {
      cc.shutdown();
    }
  }

  @Test
  public void testCoreDirCantRead() throws Exception {
    Path coreDir = solrHomeDirectory;
    setMeUp(coreDir.toAbsolutePath().toString());
    addCoreWithProps(
        makeCoreProperties("core1", true),
        coreDir.resolve("core1").resolve(CorePropertiesLocator.PROPERTIES_FILENAME));

    // Ensure that another core is opened successfully
    addCoreWithProps(
        makeCoreProperties("core2", false, "dataDir=core2"),
        coreDir.resolve("core2").resolve(CorePropertiesLocator.PROPERTIES_FILENAME));

    Path toSet = coreDir.resolve("core1");
    try {
      Set<PosixFilePermission> perms = Files.getPosixFilePermissions(toSet);
      perms.remove(PosixFilePermission.OWNER_READ);
      perms.remove(PosixFilePermission.GROUP_READ);
      perms.remove(PosixFilePermission.OTHERS_READ);
      Files.setAttribute(toSet, "posix:permissions", perms);
    } catch (UnsupportedOperationException e) {
      throw new AssumptionViolatedException(
          "Cannot make " + toSet + " non-readable. Test aborted.", e);
    }
    assumeFalse("Appears we are a super user, skip test", Files.isReadable(toSet));
    CoreContainer cc = init();
    try (SolrCore core1 = cc.getCore("core1");
        SolrCore core2 = cc.getCore("core2")) {
      assertNull(core1);
      assertNotNull(core2);
    } finally {
      cc.shutdown();
    }
    // So things can be cleaned up by the framework!
    Set<PosixFilePermission> perms = Files.getPosixFilePermissions(toSet);
    perms.add(PosixFilePermission.OWNER_READ);
    perms.add(PosixFilePermission.GROUP_READ);
    perms.add(PosixFilePermission.OTHERS_READ);
    Files.setAttribute(toSet, "posix:permissions", perms);
  }

  @Test
  public void testNonCoreDirCantRead() throws Exception {
    Path coreDir = solrHomeDirectory;
    setMeUp(coreDir.toAbsolutePath().toString());
    addCoreWithProps(
        makeCoreProperties("core1", true),
        coreDir.resolve("core1").resolve(CorePropertiesLocator.PROPERTIES_FILENAME));

    addCoreWithProps(
        makeCoreProperties("core2", false, "dataDir=core2"),
        coreDir.resolve("core2").resolve(CorePropertiesLocator.PROPERTIES_FILENAME));

    Path toSet = solrHomeDirectory.resolve("cantReadDir");
    try {
      Files.createDirectories(toSet);
    } catch (IOException e) {
      throw new RuntimeException(
          "Should have been able to make directory '" + toSet.toAbsolutePath() + "' ", e);
    }
    try {
      Set<PosixFilePermission> perms = Files.getPosixFilePermissions(toSet);
      perms.remove(PosixFilePermission.OWNER_READ);
      perms.remove(PosixFilePermission.GROUP_READ);
      perms.remove(PosixFilePermission.OTHERS_READ);
      Files.setAttribute(toSet, "posix:permissions", perms);
    } catch (UnsupportedOperationException e) {
      throw new AssumptionViolatedException(
          "Cannot make " + toSet + " non-readable. Test aborted.", e);
    }
    assumeFalse("Appears we are a super user, skip test", Files.isReadable(toSet));
    CoreContainer cc = init();
    try (SolrCore core1 = cc.getCore("core1");
        SolrCore core2 = cc.getCore("core2")) {
      // Should be able to open the perfectly valid core1 despite a non-readable directory
      assertNotNull(core1);
      assertNotNull(core2);
    } finally {
      cc.shutdown();
    }
    // So things can be cleaned up by the framework!
    Set<PosixFilePermission> perms = Files.getPosixFilePermissions(toSet);
    perms.add(PosixFilePermission.OWNER_READ);
    perms.add(PosixFilePermission.GROUP_READ);
    perms.add(PosixFilePermission.OTHERS_READ);
    Files.setAttribute(toSet, "posix:permissions", perms);
  }

  @Test
  public void testFileCantRead() throws Exception {
    Path coreDir = solrHomeDirectory;
    setMeUp(coreDir.toAbsolutePath().toString());
    addCoreWithProps(
        makeCoreProperties("core1", true),
        coreDir.resolve("core1").resolve(CorePropertiesLocator.PROPERTIES_FILENAME));

    Path toSet = solrHomeDirectory.resolve("cantReadFile");

    try {
      Files.createFile(toSet);
    } catch (IOException e) {
      throw new RuntimeException(
          "Should have been able to make file '" + toSet.toAbsolutePath() + "' ", e);
    }

    try {
      Set<PosixFilePermission> perms = Files.getPosixFilePermissions(toSet);
      perms.remove(PosixFilePermission.OWNER_READ);
      perms.remove(PosixFilePermission.GROUP_READ);
      perms.remove(PosixFilePermission.OTHERS_READ);
      Files.setAttribute(toSet, "posix:permissions", perms);
    } catch (UnsupportedOperationException e) {
      throw new AssumptionViolatedException(
          "Cannot make " + toSet + " non-readable. Test aborted.", e);
    }

    CoreContainer cc = init();
    try (SolrCore core1 = cc.getCore("core1")) {
      assertNotNull(core1); // Should still be able to create core despite r/o file.
    } finally {
      cc.shutdown();
    }
    // So things can be cleaned up by the framework!
    Set<PosixFilePermission> perms = Files.getPosixFilePermissions(toSet);
    perms.add(PosixFilePermission.OWNER_READ);
    perms.add(PosixFilePermission.GROUP_READ);
    perms.add(PosixFilePermission.OTHERS_READ);
    Files.setAttribute(toSet, "posix:permissions", perms);
  }

  @Test
  public void testSolrHomeDoesntExist() throws Exception {
    IOUtils.rm(solrHomeDirectory);
    CoreContainer cc = null;
    try {
      cc = init();
    } catch (SolrException ex) {
      assertTrue(
          "Core init doesn't report if solr home directory doesn't exist " + ex.getMessage(),
          ex.getMessage().contains("Error reading core root directory"));
    } finally {
      if (cc != null) {
        cc.shutdown();
      }
    }
  }

  @Test
  public void testSolrHomeNotReadable() throws Exception {
    Path homeDir = solrHomeDirectory;
    setMeUp(homeDir.toAbsolutePath().toString());
    addCoreWithProps(
        makeCoreProperties("core1", true),
        homeDir.resolve("core1").resolve(CorePropertiesLocator.PROPERTIES_FILENAME));

    try {
      Set<PosixFilePermission> perms = Files.getPosixFilePermissions(homeDir);
      perms.remove(PosixFilePermission.OWNER_READ);
      perms.remove(PosixFilePermission.GROUP_READ);
      perms.remove(PosixFilePermission.OTHERS_READ);
      Files.setAttribute(homeDir, "posix:permissions", perms);
    } catch (UnsupportedOperationException e) {
      throw new AssumptionViolatedException(
          "Cannot make " + homeDir + " non-readable. Test aborted.", e);
    }

    assumeFalse("Appears we are a super user, skip test", Files.isReadable(homeDir));
    Exception thrown =
        expectThrows(
            Exception.class,
            () -> {
              CoreContainer cc = null;
              try {
                cc = init();
              } finally {
                if (cc != null) cc.shutdown();
              }
            });
    assertThat(thrown.getMessage(), containsString("Error reading core root directory"));
    // So things can be cleaned up by the framework!
    Set<PosixFilePermission> perms = Files.getPosixFilePermissions(homeDir);
    perms.add(PosixFilePermission.OWNER_READ);
    perms.add(PosixFilePermission.GROUP_READ);
    perms.add(PosixFilePermission.OTHERS_READ);
    Files.setAttribute(homeDir, "posix:permissions", perms);
  }

  // For testing whether finding a solr.xml overrides looking at solr.properties
  private static final String SOLR_XML =
      "<solr> "
          + "<str name=\"configSetBaseDir\">"
          + TEST_HOME().resolve("configsets")
          + "</str>"
          + "<solrcloud> "
          + "<int name=\"zkClientTimeout\">20</int> "
          + "<str name=\"host\">222.333.444.555</str> "
          + "<int name=\"hostPort\">6000</int>  "
          + "</solrcloud> "
          + "</solr>";

  @Test
  public void testRootDirectoryResolution() {
    NodeConfig config =
        SolrXmlConfig.fromString(
            solrHomeDirectory, "<solr><str name=\"coreRootDirectory\">relative</str></solr>");
    assertThat(
        config.getCoreRootDirectory().toString(),
        containsString(solrHomeDirectory.toAbsolutePath().toString()));

    NodeConfig absConfig =
        SolrXmlConfig.fromString(
            solrHomeDirectory, "<solr><str name=\"coreRootDirectory\">/absolute</str></solr>");
    assertThat(
        absConfig.getCoreRootDirectory().toString(),
        not(containsString(solrHomeDirectory.toAbsolutePath().toString())));
  }
}
