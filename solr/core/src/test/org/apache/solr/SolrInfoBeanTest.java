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
package org.apache.solr;

import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Stream;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.handler.admin.LukeRequestHandler;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.SearchHandler;
import org.apache.solr.highlight.DefaultSolrHighlighter;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.search.CaffeineCache;
import org.junit.BeforeClass;

/** A simple test used to increase code coverage for some standard things... */
public class SolrInfoBeanTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  /**
   * Gets a list of everything we can find in the classpath and makes sure it has a name,
   * description, etc...
   */
  public void testCallMBeanInfo() throws Exception {
    List<Class<?>> classes = new ArrayList<>();
    classes.addAll(getClassesForPackage(SearchHandler.class.getPackage().getName()));
    classes.addAll(getClassesForPackage(SearchComponent.class.getPackage().getName()));
    classes.addAll(getClassesForPackage(LukeRequestHandler.class.getPackage().getName()));
    classes.addAll(getClassesForPackage(DefaultSolrHighlighter.class.getPackage().getName()));
    classes.addAll(getClassesForPackage(CaffeineCache.class.getPackage().getName()));
    // System.out.println(classes);

    int checked = 0;
    SolrMetricManager metricManager = h.getCoreContainer().getMetricManager();
    String registry = h.getCore().getCoreMetricManager().getRegistryName();
    SolrMetricsContext solrMetricsContext = new SolrMetricsContext(metricManager, registry, "foo");
    String scope = TestUtil.randomSimpleString(random(), 2, 10);
    for (Class<?> clazz : classes) {
      if (SolrInfoBean.class.isAssignableFrom(clazz)) {
        try {
          SolrInfoBean info = clazz.asSubclass(SolrInfoBean.class).getConstructor().newInstance();
          info.initializeMetrics(solrMetricsContext, scope);

          // System.out.println( info.getClass() );
          assertNotNull(info.getClass().getCanonicalName(), info.getName());
          assertNotNull(info.getClass().getCanonicalName(), info.getDescription());
          assertNotNull(info.getClass().getCanonicalName(), info.getCategory());

          if (info instanceof CaffeineCache) {
            continue;
          }

          assertNotNull(info.toString());
          checked++;
        } catch (ReflectiveOperationException ex) {
          // expected...
          // System.out.println( "unable to initialize: "+clazz );
        }
      }
    }
    assertTrue(
        "there are at least 10 SolrInfoBean that should be found in the classpath, found "
            + checked,
        checked > 10);
  }

  private static List<Class<?>> getClassesForPackage(String pckgname) throws Exception {
    ArrayList<Path> directories = new ArrayList<>();
    ClassLoader cld = h.getCore().getResourceLoader().getClassLoader();
    String path = pckgname.replace('.', '/');
    Enumeration<URL> resources = cld.getResources(path);
    while (resources.hasMoreElements()) {
      final URI uri = resources.nextElement().toURI();
      if (!"file".equalsIgnoreCase(uri.getScheme())) continue;
      final Path f = Path.of(uri);
      directories.add(f);
    }

    ArrayList<Class<?>> classes = new ArrayList<>();
    for (Path directory : directories) {
      if (Files.exists(directory)) {
        try (Stream<Path> files = Files.list(directory)) {
          files.forEach(
              (file) -> {
                String fileName = file.getFileName().toString();
                if (fileName.endsWith(".class")) {
                  String clazzName = fileName.substring(0, fileName.length() - 6);
                  // exclude Test classes that happen to be in these packages.
                  // class.ForName'ing some of them can cause trouble.
                  if (!clazzName.endsWith("Test") && !clazzName.startsWith("Test")) {
                    try {
                      classes.add(Class.forName(pckgname + '.' + clazzName));
                    } catch (ClassNotFoundException e) {
                      throw new RuntimeException(e);
                    }
                  }
                }
              });
        }
      }
    }
    assertFalse(
        "No classes found in package '"
            + pckgname
            + "'; maybe your test classes are packaged as JAR file?",
        classes.isEmpty());
    return classes;
  }
}
