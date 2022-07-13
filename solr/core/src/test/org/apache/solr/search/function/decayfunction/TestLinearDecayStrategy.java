package org.apache.solr.search.function.decayfunction;

import org.junit.Assert;
import org.junit.Test;

public class TestLinearDecayStrategy {

  @Test
  public void testLinearDecayFunctionValueSourceParser() {
    DecayFunctionValueSourceParser parser = new LinearDecayFunctionValueSourceParser();
    DecayStrategy strategy = parser.getDecayStrategy();
    for (double scale = 0; scale < 100; scale += 0.1)
      for (double decay = 0; decay < 1; decay += 0.01)
        for (double value = 0; value < 100; value += 0.1) testScale(scale, decay, value, strategy);
  }

  private void testScale(double scale, double decay, double value, DecayStrategy strategy) {
    double s = strategy.scale(scale, decay);
    Assert.assertEquals(scale / (1.0 - decay), s, 0);
    double v = strategy.calculate(value, s);
    Assert.assertEquals(Math.max(0.0, (s - value) / s), v, 0);
  }
}
