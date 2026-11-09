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

import java.util.function.BiPredicate;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MMapDirectory;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.NamedList;

/** Test-case for MMapDirectoryFactory */
public class MMapDirectoryFactoryTest extends SolrTestCase {

  public void testPreloadDefaultsToNoFiles() {
    assertSame(MMapDirectory.NO_FILES, preloadPredicate(new NamedList<>()));
  }

  public void testPreloadTrueSelectsAllFiles() {
    NamedList<Object> args = new NamedList<>();
    args.add("preload", true);
    assertSame(MMapDirectory.ALL_FILES, preloadPredicate(args));
  }

  public void testPreloadExtensionsTakesPrecedenceOverPreloadTrue() {
    NamedList<Object> args = new NamedList<>();
    args.add("preload", true);
    args.add("preloadExtensions", ".vex");
    BiPredicate<String, IOContext> predicate = preloadPredicate(args);

    assertMatches(predicate, "_0.vex");
    assertDoesNotMatch(predicate, "_0.fdt");
  }

  public void testEmptyPreloadExtensionsSelectsNoFiles() {
    NamedList<Object> args = new NamedList<>();
    args.add("preloadExtensions", " , ");
    assertSame(MMapDirectory.NO_FILES, preloadPredicate(args));
  }

  public void testEmptyPreloadExtensionsFallsBackToPreloadTrue() {
    NamedList<Object> args = new NamedList<>();
    args.add("preload", true);
    args.add("preloadExtensions", " , ");
    assertSame(MMapDirectory.ALL_FILES, preloadPredicate(args));
  }

  public void testPreloadExtensionsMatchesOnlyListedExtensions() {
    NamedList<Object> args = new NamedList<>();
    args.add("preloadExtensions", ".vex,.veb");
    BiPredicate<String, IOContext> predicate = preloadPredicate(args);

    assertMatches(predicate, "_0_Lucene99HnswVectorsFormat_0.vex");
    assertMatches(predicate, "_0_Lucene102BinaryQuantizedVectorsFormat_0.veb");
    assertDoesNotMatch(predicate, "_0.fdt");
    assertDoesNotMatch(predicate, "_0_Lucene99HnswVectorsFormat_0.vem");
  }

  public void testPreloadExtensionsIgnoresWhitespaceCaseAndLeadingDot() {
    NamedList<Object> args = new NamedList<>();
    args.add("preloadExtensions", " VEX , .veb ");
    BiPredicate<String, IOContext> predicate = preloadPredicate(args);

    assertMatches(predicate, "_0.vex");
    assertMatches(predicate, "_0.veb");
  }

  public void testPreloadExtensionsDoesNotMatchExtensionlessFiles() {
    NamedList<Object> args = new NamedList<>();
    args.add("preloadExtensions", ".vex");
    BiPredicate<String, IOContext> predicate = preloadPredicate(args);

    assertDoesNotMatch(predicate, "segments_1");
    assertDoesNotMatch(predicate, "write.lock");
  }

  public void testPreloadExtensionsMatchesSuffixNotSubstring() {
    NamedList<Object> args = new NamedList<>();
    args.add("preloadExtensions", ".vec");
    BiPredicate<String, IOContext> predicate = preloadPredicate(args);

    assertMatches(predicate, "_0.vec");
    assertDoesNotMatch(predicate, "_0.vector");
    assertDoesNotMatch(predicate, "_0.vec.tmp");
  }

  private static BiPredicate<String, IOContext> preloadPredicate(NamedList<Object> args) {
    MMapDirectoryFactory factory = new MMapDirectoryFactory();
    factory.init(args);
    return factory.preloadPredicate();
  }

  private static void assertMatches(BiPredicate<String, IOContext> predicate, String fileName) {
    assertTrue(
        "expected " + fileName + " to be preloaded", predicate.test(fileName, IOContext.DEFAULT));
  }

  private static void assertDoesNotMatch(
      BiPredicate<String, IOContext> predicate, String fileName) {
    assertFalse(
        "expected " + fileName + " to not be preloaded",
        predicate.test(fileName, IOContext.DEFAULT));
  }
}
