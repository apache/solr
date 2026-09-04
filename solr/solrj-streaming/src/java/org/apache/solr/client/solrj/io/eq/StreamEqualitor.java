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
package org.apache.solr.client.solrj.io.eq;

import java.io.IOException;
import java.io.Serializable;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.MultipleFieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;

/** Defines a comparator we can use with TupleStreams */
public interface StreamEqualitor extends Equalitor<Tuple>, Expressible, Serializable {
  public boolean isDerivedFrom(StreamEqualitor base);

  public boolean isDerivedFrom(StreamComparator base);

  /**
   * Whether this equalitor's left-hand field(s) are exactly the field(s) that {@code base} - a
   * single stream's own sort comparator, whose left/right field names are necessarily identical -
   * sorts on. Used to validate the stream feeding the left side of a two-stream equality (e.g.
   * streamA in complement/intersect), as opposed to {@link #isDerivedFrom(StreamComparator)} which
   * matches either side and so cannot validate an asymmetric {@code on=} clause correctly.
   */
  boolean isDerivedFromLeft(StreamComparator base);

  /** Right-hand counterpart of {@link #isDerivedFromLeft(StreamComparator)}. */
  boolean isDerivedFromRight(StreamComparator base);

  /**
   * Verifies that this equalitor's field(s) are actually present in the given tuples, as opposed to
   * merely holding a null value, throwing if not. A missing field compares as null just like a
   * present-but-null field, which would otherwise silently mask a stream wiring bug (e.g. an {@code
   * on=} clause naming a field that one side's search/select never returns).
   */
  default void assertFieldsPresent(Tuple left, Tuple right) throws IOException {
    // no-op by default; overridden by equalitors that know their own field names
  }

  /**
   * Builds a {@link StreamComparator} that orders tuples the way this equalitor pairs fields across
   * two streams, taking the sort order(s) from {@code comp} (typically one of the two streams' own
   * sort). Unlike {@code comp} itself - whose left and right field names are identical, since it's
   * a single stream's own sort - the returned comparator carries this equalitor's (possibly
   * different) left/right field names, so it can correctly order a tuple from one stream against a
   * tuple from the other even when {@code on=} maps differently-named fields.
   */
  static StreamComparator deriveComparator(StreamEqualitor eq, StreamComparator comp)
      throws IOException {
    if (eq instanceof MultipleFieldEqualitor multiEq
        && comp instanceof MultipleFieldComparator multiComp) {
      // comp is at least as long as eq because tuple order has already been validated
      StreamComparator[] compoundComps = new StreamComparator[multiEq.getEqs().length];
      for (int idx = 0; idx < compoundComps.length; ++idx) {
        compoundComps[idx] = deriveComparator(multiEq.getEqs()[idx], multiComp.getComps()[idx]);
      }
      return new MultipleFieldComparator(compoundComps);
    } else if (comp instanceof MultipleFieldComparator multiComp) {
      return deriveComparator(eq, multiComp.getComps()[0]);
    } else if (eq instanceof FieldEqualitor fieldEq && comp instanceof FieldComparator fieldComp) {
      return new FieldComparator(
          fieldEq.getLeftFieldName(), fieldEq.getRightFieldName(), fieldComp.getOrder());
    } else {
      throw new IOException(
          "Failed to derive a comparator from equalitor " + eq + " and comparator " + comp);
    }
  }

  /**
   * Derives an equalitor referencing only this equalitor's right-hand field(s), for comparing two
   * tuples that both come from the "right" stream (e.g. de-duplicating streamB against itself in
   * complement/intersect, where using the original, possibly asymmetric equalitor would compare the
   * wrong field on one side).
   */
  static StreamEqualitor deriveRightEqualitor(StreamEqualitor eq) throws IOException {
    if (eq instanceof MultipleFieldEqualitor multiEq) {
      StreamEqualitor[] rightEqs = new StreamEqualitor[multiEq.getEqs().length];
      for (int idx = 0; idx < rightEqs.length; ++idx) {
        rightEqs[idx] = deriveRightEqualitor(multiEq.getEqs()[idx]);
      }
      return new MultipleFieldEqualitor(rightEqs);
    } else if (eq instanceof FieldEqualitor fieldEq) {
      return new FieldEqualitor(fieldEq.getRightFieldName());
    } else {
      throw new IOException("Failed to derive a right-side equalitor from " + eq);
    }
  }
}
