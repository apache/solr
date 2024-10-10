package org.apache.solr.handler.component;

import org.apache.solr.client.solrj.io.Lang;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class UBIComponentLocalLoggingTest extends SolrCloudTestCase {


    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testLocalCatStream() throws Exception {

        File localFile = File.createTempFile("topLevel1", ".txt");

        TupleStream stream;
        List<Tuple> tuples;
        StreamContext streamContext = new StreamContext();
        SolrClientCache solrClientCache = new SolrClientCache();

        streamContext.setSolrClientCache(solrClientCache);

        StreamFactory streamFactory = new StreamFactory();


        // LocalCatStream extends CatStream and disables the Solr cluster specific
        // logic about where to read data from.
        streamFactory.withFunctionName("logging", LogStream.class);


        Lang.register(streamFactory);

        String clause = "logging(bob.txt,echo(\"bob\"))";
        stream = streamFactory.constructStream(clause);
        stream.setStreamContext(streamContext);
        tuples = getTuples(stream);
        stream.close();
        solrClientCache.close();


        //populateFileWithData(localFile.toPath());


        Tuple tuple = new Tuple(new HashMap());
        tuple.put("field1", "blah");
        tuple.put("field2", "blah");
        tuple.put("field3", "blah");

       LogStream logStream =
                new LogStream(localFile.getAbsolutePath());
        List<Tuple> tuples2 = new ArrayList();
        try {
            logStream.open();



//            while (true) {
//                Tuple tuple = logStream.read();
//                if (tuple.EOF) {
//                    break;
//                } else {
//                    tuples.add(tuple);
//                }
//            }

        } finally {
            logStream.close();
        }

        assertEquals(4, tuples.size());

        for (int i = 0; i < 4; i++) {
            Tuple t = tuples.get(i);
            assertEquals(localFile.getName() + " line " + (i + 1), t.get("line"));
            assertEquals(localFile.getAbsolutePath(), t.get("file"));
        }
    }

    private List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
        tupleStream.open();
        List<Tuple> tuples = new ArrayList<>();
        for (; ; ) {
            Tuple t = tupleStream.read();
            // log.info(" ... {}", t.fields);
            if (t.EOF) {
                break;
            } else {
                tuples.add(t);
            }
        }
        tupleStream.close();
        return tuples;
    }
}
