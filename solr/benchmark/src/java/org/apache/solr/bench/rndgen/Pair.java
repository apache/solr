package org.apache.solr.bench.rndgen;

import java.util.Objects;
import java.util.StringJoiner;

public final class Pair<A, B> {
  public final A first;
  public final B second;

  private Pair(A _1, B _2) {
    super();
    this.first = _1;
    this.second = _2;
  }

  /**
   * Creates a pair from the two supplied values
   *
   * @param <A> first type
   * @param <B> second type
   * @param a first supplied value
   * @param b second supplied value
   * @return a pair
   */
  public static <A, B> Pair<A, B> of(A a, B b) {
    return new Pair<>(a, b);
  }

  @Override
  public String toString() {
    StringJoiner sj = new StringJoiner(", ", "{", "}");
    return sj.add("" + first).add("" + second).toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(first, second);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    @SuppressWarnings("rawtypes")
    Pair other = (Pair) obj;
    return Objects.equals(first, other.first) && Objects.equals(second, other.second);
  }
}
