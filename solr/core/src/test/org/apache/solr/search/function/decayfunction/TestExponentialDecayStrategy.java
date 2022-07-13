package org.apache.solr.search.function.decayfunction;

import org.junit.Assert;
import org.junit.Test;

public class TestExponentialDecayStrategy {

  @Test
  public void testExponentialDecayStrategy() {
    DecayFunctionValueSourceParser parser = new ExponentialDecayFunctionValueSourceParser();
    DecayStrategy strategy = parser.getDecayStrategy();
    for (double scale = 0; scale < 100; scale += 0.1)
      for (double decay = 0; decay < 1; decay += 0.01)
        for (double value = 0; value < 100; value += 0.1) test(scale, decay, value, strategy);
  }

  private void test(double scale, double decay, double value, DecayStrategy strategy) {
    double s = strategy.scale(scale, decay);
    Assert.assertEquals(Math.log(decay) / scale, s, 0);
    double v = strategy.calculate(value, s);
    Assert.assertEquals(Math.exp(s * value), v, 0);
  }
}
