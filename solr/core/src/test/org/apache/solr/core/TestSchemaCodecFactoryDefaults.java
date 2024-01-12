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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Set;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.hamcrest.MatcherAssert;
import org.junit.BeforeClass;

/**
 * Asserts that <code>SchemaCodecFactory</code> is the implicit default factory when none is
 * specified in the solrconfig.xml, and that the behavior of the resulting Codec is identical to the
 * (current) Lucene default Codec for any arbitrary field name (when no schema based codec
 * proeprties are overridden for that field).
 *
 * <p>The primar goal of this test is to help ensure that Solr's <code>SchemaCodecFactory</code>
 * doesn't fall behind when Lucene improves it's default codec.
 */
public class TestSchemaCodecFactoryDefaults extends SolrTestCaseJ4 {

  // Note: we use TestUtil to get the "real" (not randomized) default for our current lucene version
  private static final Codec LUCENE_DEFAULT_CODEC = TestUtil.getDefaultCodec();

  @BeforeClass
  public static void beforeClass() throws Exception {
    // TODO: replace with EmbeddedSolrServerTestRule (at a glance, I'm not sure how just yet)
    initCore("solrconfig-minimal.xml", "schema-minimal.xml");

    // Some basic assertions that are fundemental to the entire test...

    assertNull(
        "Someone broke test config so it declares a CodecFactory, making this test useless",
        h.getCore().getSolrConfig().getPluginInfo(CodecFactory.class.getName()));

    MatcherAssert.assertThat(
        "WTF: SolrCore's implicit default Codec is literally same object as Lucene default?",
        h.getCore().getCodec(),
        not(sameInstance(LUCENE_DEFAULT_CODEC)));

    MatcherAssert.assertThat(
        "WTF: SolrCore's Codec is literally same object as Lucene (test randomixed) default?",
        h.getCore().getCodec(),
        not(sameInstance(Codec.getDefault())));

    // TODO: it would be nice if we could assert something like 'instanceOf(SchemaCodec.class)'
    // But making that kind of assertion would require refactoring SchemaCodecFactory
    // to return an instance of a concreate (named) class -- right now it's anonymous...
    MatcherAssert.assertThat(
        "WTF: SolrCore's implicit default Codec is not anon impl from SchemaCodecFactory",
        h.getCore().getCodec().getClass().getName(),
        containsString(".SchemaCodecFactory$"));
  }

  public void testSubclass() {
    MatcherAssert.assertThat(
        "SchemaCodec does not extend current default lucene codec",
        h.getCore().getCodec(),
        instanceOf(LUCENE_DEFAULT_CODEC.getClass()));
  }

  /**
   * Methods with these names must exist, take a single String arg, and return instances of the same
   * concrete class as the corrisponding method in the Lucene default codec
   *
   * @see #testPerFieldMethodEquivilence
   * @see #testCodecDeclaredMethodCoverage
   */
  public static Set<String> PER_FIELD_FORMAT_METHOD_NAMES =
      Set.of(
          "getKnnVectorsFormatForField", "getPostingsFormatForField", "getDocValuesFormatForField");

  /**
   * Methods with these names will be ignored when doing equivilence checks of the Lucene default
   * codec.
   *
   * <p>The primary purpose of this Set is to list the no-arg versions of per-field methods that are
   * already tested via in {@link #PER_FIELD_FORMAT_METHOD_NAMES} (Because the solr "per-field"
   * wrapper wll be different then the Lucene "per-field" default impl of these methods)
   *
   * <p>The other reason to include methods in this set, is if they already have bespoke tests
   * (perhaps because thye require arugments?) and can't be automatically tested via reflection
   *
   * @see #testCodecDeclaredMethodCoverage
   */
  public static Set<String> IGNORED_CODEC_DECLARED_METHOD_NAMES =
      Set.of("knnVectorsFormat", "postingsFormat", "docValuesFormat");

  /**
   * For all of the "per-field" format methods supported by the Lucene default codec, assert that
   * the SchemaCodec returns an instance of the same classes as lucene (Since our schema does not
   * override this in any way)
   *
   * @see #PER_FIELD_FORMAT_METHOD_NAMES
   */
  public void testPerFieldMethodEquivilence() throws Exception {
    final String fieldNameShouldNotMatter = TestUtil.randomSimpleString(random(), 64);

    final Codec expectedCodec = LUCENE_DEFAULT_CODEC;
    final Codec actualCodec = h.getCore().getCodec();

    for (String methodName : PER_FIELD_FORMAT_METHOD_NAMES) {

      final Method expected = expectedCodec.getClass().getMethod(methodName, String.class);
      final Method actual = actualCodec.getClass().getMethod(methodName, String.class);

      assertEquals(
          methodName,
          expected.invoke(expectedCodec, fieldNameShouldNotMatter).getClass(),
          actual.invoke(actualCodec, fieldNameShouldNotMatter).getClass());
    }
  }

  /**
   * Loops over all methods declared by the {@link Codec} class, executing them against both the
   * Lucene default codec and our SchemaCodec, asserting that the resulting objects have the same
   * runtime type.
   *
   * <p>Any method found which requires arguments will cause a failure unless it is in {@link
   * #PER_FIELD_FORMAT_METHOD_NAMES} (which are tested by {@link #testPerFieldMethodEquivilence}) or
   * {@link #IGNORED_CODEC_DECLARED_METHOD_NAMES} (Exempt from testing, or should have their own
   * bespoke test)
   */
  public void testCodecDeclaredMethodCoverage() throws Exception {
    final Codec expectedCodec = LUCENE_DEFAULT_CODEC;
    final Codec actualCodec = h.getCore().getCodec();

    for (Method declaredMethod : Codec.class.getDeclaredMethods()) {
      // Things we know we can skip...
      if (declaredMethod.isSynthetic()
          || Modifier.isStatic(declaredMethod.getModifiers())
          || Modifier.isFinal(declaredMethod.getModifiers())
          || Modifier.isPrivate(declaredMethod.getModifiers())
          || IGNORED_CODEC_DECLARED_METHOD_NAMES.contains(declaredMethod.getName())
          || PER_FIELD_FORMAT_METHOD_NAMES.contains(declaredMethod.getName())) {
        continue;
      }

      final String methodName = declaredMethod.getName();
      assertEquals(
          "Codec declared Method has non-zero arg count, "
              + "must be ignored and tested via bespoke method: "
              + methodName,
          0,
          declaredMethod.getParameterCount());

      final Method expected = expectedCodec.getClass().getMethod(methodName);
      final Method actual = actualCodec.getClass().getMethod(methodName);

      assertEquals(
          methodName,
          expected.invoke(expectedCodec).getClass(),
          actual.invoke(actualCodec).getClass());
    }
  }
}
