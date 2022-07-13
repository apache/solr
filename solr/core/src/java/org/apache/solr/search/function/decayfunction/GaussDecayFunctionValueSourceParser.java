package org.apache.solr.search.function.decayfunction;

public class GaussDecayFunctionValueSourceParser extends DecayFunctionValueSourceParser {
  @Override
  DecayStrategy getDecayStrategy() {
    return new GaussDecay();
  }

  @Override
  String name() {
    return "gauss";
  }
}

final class GaussDecay implements DecayStrategy {

  @Override
  public double scale(double scale, double decay) {
    return 0.5 * Math.pow(scale, 2.0) / Math.log(decay);
  }

  @Override
  public double calculate(double value, double scale) {
    // note that we already computed scale^2 in processScale() so we do
    // not need to square it here.
    return Math.exp(0.5 * Math.pow(value, 2.0) / scale);
  }

  @Override
  public String explain(double scale) {
    return "exp(-0.5*pow(<val>,2.0)/" + -1 * scale + ")";
  }
}
