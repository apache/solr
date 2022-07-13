package org.apache.solr.search.function.decayfunction;

public class LinearDecayFunctionValueSourceParser extends DecayFunctionValueSourceParser {

  @Override
  DecayStrategy getDecayStrategy() {
    return new LinearDecay();
  }

  @Override
  String name() {
    return "linear";
  }
}

final class LinearDecay implements DecayStrategy {

  @Override
  public double scale(double scale, double decay) {
    return scale / (1.0 - decay);
  }

  @Override
  public double calculate(double value, double scale) {
    return Math.max(0.0, (scale - value) / scale);
  }

  @Override
  public String explain(double scale) {
    return "max(0.0, ((" + scale + " - <val>)/" + scale + ")";
  }
}
