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
package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.util.LinkedList;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eq.StreamEqualitor;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 * Joins leftStream with rightStream based on an Equalitor. Both streams must be sorted by the
 * fields being joined on. Resulting stream is sorted by the equalitor.
 *
 * @since 9.10.0
 */
public class FullOuterJoinStream extends BiJoinStream {

  @SuppressWarnings("JdkObsolete")
  private final LinkedList<Tuple> joinedTuples = new LinkedList<>();

  @SuppressWarnings("JdkObsolete")
  private final LinkedList<Tuple> leftTupleGroup = new LinkedList<>();

  @SuppressWarnings("JdkObsolete")
  private final LinkedList<Tuple> rightTupleGroup = new LinkedList<>();

  public FullOuterJoinStream(TupleStream leftStream, TupleStream rightStream, StreamEqualitor eq)
      throws IOException {
    super(leftStream, rightStream, eq);
  }

  public FullOuterJoinStream(StreamExpression expression, StreamFactory factory)
      throws IOException {
    super(expression, factory);
  }

  @Override
  public Tuple read() throws IOException {
    // if we've already figured out the next joined tuple then just return it
    if (joinedTuples.size() > 0) {
      return joinedTuples.removeFirst();
    }

    // keep going until we find something to return or both streams are empty
    while (true) {

      // load next set of equal tuples from leftStream into leftTupleGroup
      if (0 == leftTupleGroup.size()) {
        loadEqualTupleGroup(leftStream, leftTupleGroup, leftStreamComparator);
      }

      // same for right
      if (0 == rightTupleGroup.size()) {
        loadEqualTupleGroup(rightStream, rightTupleGroup, rightStreamComparator);
      }

      Boolean leftFinished = (0 == leftTupleGroup.size() || leftTupleGroup.get(0).EOF);
      Boolean rightFinished = (0 == rightTupleGroup.size() || rightTupleGroup.get(0).EOF);

      // If both streams are at EOF, we're done
      if (leftFinished && rightFinished) {
        return Tuple.EOF();
      }

      // If the left stream is at the EOF, we just return the next element from the right stream
      if (leftFinished) {
        return rightTupleGroup.removeFirst();
      }

      // If the right stream is at the EOF, we just return the next element from the left stream
      if (rightFinished) {
        return leftTupleGroup.removeFirst();
      }

      // At this point we know both left and right groups have at least 1 member
      if (eq.test(leftTupleGroup.get(0), rightTupleGroup.get(0))) {
        // The groups are equal. Join em together and build the joinedTuples
        for (Tuple left : leftTupleGroup) {
          for (Tuple right : rightTupleGroup) {
            Tuple clone = left.clone();
            clone.merge(right);
            joinedTuples.add(clone);
          }
        }

        // Cause each to advance next time we need to look
        leftTupleGroup.clear();
        rightTupleGroup.clear();

        return joinedTuples.removeFirst();
      } else {
        int c = iterationComparator.compare(leftTupleGroup.get(0), rightTupleGroup.get(0));
        if (c < 0) {
          // If there's no match, we still advance the left stream while returning every element.
          // Because it's an outer join we still return the left tuple if no match on right.
          return leftTupleGroup.removeFirst();
        } else {
          // return right item as it didn't match left and this is a full outer join
          return rightTupleGroup.removeFirst();
        }
      }
    }
  }

  @Override
  public StreamComparator getStreamSort() {
    return iterationComparator;
  }
}
