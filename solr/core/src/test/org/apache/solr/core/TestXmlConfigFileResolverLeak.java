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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.common.AlreadyClosedException;
import org.junit.Test;

/**
 * Regression test for a cross-test entity-resolver leak.
 *
 * <p>{@link XmlConfigFile#parseXml} parses through Saxon's shared, static
 * {@link SolrResourceLoader#conf} Configuration, which pools its Xerces source parsers for the
 * lifetime of the JVM. A pooled parser retains the {@link org.apache.solr.util.SystemIdResolver}
 * (and the {@link SolrResourceLoader} it wraps) that was installed for its XInclude handling on a
 * previous parse. Consequently, a config parse performed earlier in the same JVM through a
 * {@code ZkSolrResourceLoader} would leave that Zk-backed resolver installed on a pooled parser;
 * once the owning cloud test closed its ZK connection, a subsequent, unrelated, non-cloud
 * {@code initCore()} would resolve its XIncludes against the already-closed loader and fail with
 * {@link AlreadyClosedException} from inside the XInclude handler.
 *
 * <p>This test reproduces that JVM-local leak deterministically without ZooKeeper by standing in a
 * resource loader that throws {@link AlreadyClosedException} after it has been "closed", exactly as
 * a closed {@code ZkSolrResourceLoader} does. The fix supplies a fresh, XInclude-aware reader per
 * parse so the resolver can never leak across parses.
 */
public class TestXmlConfigFileResolverLeak extends SolrTestCaseJ4 {

  /** Mimics a ZkSolrResourceLoader whose backing ZK connection is closed after the test using it. */
  private static class ClosableResourceLoader extends SolrResourceLoader {
    volatile boolean closed = false;

    ClosableResourceLoader(Path instanceDir) {
      super(instanceDir);
    }

    @Override
    public InputStream openResource(String resource) throws IOException {
      if (closed) {
        throw new AlreadyClosedException("simulated closed ZkSolrResourceLoader");
      }
      return super.openResource(resource);
    }
  }

  @Test
  public void testEntityResolverDoesNotLeakAcrossParses() throws Exception {
    final Path instanceDir = SolrTestUtil.TEST_PATH().resolve("collection1");

    // First parse: stands in for a cloud test loading a config (with XIncludes) through a
    // Zk-backed loader. This installs the loader's SystemIdResolver onto a Saxon-pooled parser.
    ClosableResourceLoader first = new ClosableResourceLoader(instanceDir);
    new XmlConfigFile(first, "solrconfig-xinclude.xml");

    // The cloud test ends and its ZK connection (resource loader) is closed.
    first.closed = true;

    // A later, unrelated non-cloud parse must resolve its OWN XIncludes through ITS loader, never
    // the now-closed first loader. Before the fix, the pooled parser still carried the first
    // loader's resolver and this parse threw AlreadyClosedException from the XInclude handler.
    SolrResourceLoader second = new SolrResourceLoader(instanceDir);
    new XmlConfigFile(second, "solrconfig-xinclude.xml");
  }
}
