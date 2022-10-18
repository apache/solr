package org.apache.solr.client.solrj.io.stream.eval;

import junit.framework.Assert;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eval.MovingAverageEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MovingAverageEvaluatorTest extends SolrTestCase {

    StreamFactory factory;
    Map<String, Object> values;

    public MovingAverageEvaluatorTest() {
        super();

        factory =
                new StreamFactory()
                        .withFunctionName("movingAvg", MovingAverageEvaluator.class);
        values = new HashMap<>();
    }

    @Test
    public void doesNotFailWithEmptyList() throws Exception {
        StreamEvaluator evaluator = factory.constructEvaluator("movingAvg(a,30)");
        Object result;

        values.clear();
        values.put("a", new ArrayList<Double>());
        result = evaluator.evaluate(new Tuple(values));
        Assert.assertEquals(Collections.emptyList(), result);
    }
}
