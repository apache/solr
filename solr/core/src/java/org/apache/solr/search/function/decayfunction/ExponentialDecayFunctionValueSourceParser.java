package org.apache.solr.search.function.decayfunction;

public class ExponentialDecayFunctionValueSourceParser extends DecayFunctionValueSourceParser {
  @Override
  DecayStrategy getDecayStrategy() {
    return new ExponentialDecay();
  }

  @Override
  String name() {
    return "exp";
  }
}

final class ExponentialDecay implements DecayStrategy {

  @Override
  public double scale(double scale, double decay) {
    return Math.log(decay) / scale;
  }

  @Override
  public double calculate(double value, double scale) {
    return Math.exp(scale * value);
  }

  @Override
  public String explain(double scale) {
    return "exp(- <val> * " + -1 * scale + ")";
  }
}
