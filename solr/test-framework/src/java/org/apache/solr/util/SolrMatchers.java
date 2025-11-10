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
package org.apache.solr.util;

import java.util.List;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

/**
 * A Utility class for extra Hamcrest {@link Matcher} implementations.
 *
 * <p>These may be directly related to Solr, or be filling gaps that the default Hamcrest Matchers
 * do not cover.
 */
public class SolrMatchers {

  /**
   * Matches a segment of a list between two indices against a provided matcher, useful for checking
   * ordered and unordered sub-sequences in a larger list
   *
   * @param fromIndex starting index of the desired sub-list (inclusive)
   * @param toIndex ending index of the desired sub-list (exclusive)
   * @param subListMatcher matcher for the resulting sub-list
   * @return a Matcher for the original super-list
   * @param <T> The type that the list being matched against will contain
   */
  public static <T> Matcher<List<? extends T>> subListMatches(
      int fromIndex, int toIndex, Matcher<? super List<? super T>> subListMatcher) {
    return new SubListMatcher<>(fromIndex, toIndex, subListMatcher);
  }

  public static class SubListMatcher<T> extends TypeSafeDiagnosingMatcher<List<? extends T>> {
    private final int fromIndex;
    private final int toIndex;
    private final Matcher<? super List<T>> matchOnSubList;

    public SubListMatcher(
        int fromIndex, int toIndex, Matcher<? super List<? super T>> matchOnSubList) {
      super();
      this.fromIndex = fromIndex;
      this.toIndex = toIndex;
      this.matchOnSubList = matchOnSubList;
    }

    @Override
    public void describeTo(Description description) {
      description
          .appendText("sub-list from ")
          .appendValue(fromIndex)
          .appendText(" to ")
          .appendValue(toIndex)
          .appendText(" matching ")
          .appendDescriptionOf(matchOnSubList);
    }

    @Override
    protected boolean matchesSafely(List<? extends T> item, Description mismatchDescription) {
      if (item.size() < toIndex) {
        mismatchDescription
            .appendText(": expected sub-list endIndex ")
            .appendValue(toIndex)
            .appendText(" greater than list size ")
            .appendValue(item.size())
            .appendText("\n          full-list: ")
            .appendValue(item);
        return false;
      } else {
        List<? extends T> subList = item.subList(fromIndex, toIndex);
        if (!matchOnSubList.matches(subList)) {
          matchOnSubList.describeMismatch(subList, mismatchDescription);
          mismatchDescription
              .appendText("\n          sub-list:  ")
              .appendValue(subList)
              .appendText("\n          full-list: ")
              .appendValue(item);
          return false;
        }
      }
      return true;
    }
  }
}
