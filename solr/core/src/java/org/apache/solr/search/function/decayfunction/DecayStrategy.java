package org.apache.solr.search.function.decayfunction;

public interface DecayStrategy {
  double scale(double scale, double decay);

  double calculate(double value, double scale);

  String explain(double scale);
}
