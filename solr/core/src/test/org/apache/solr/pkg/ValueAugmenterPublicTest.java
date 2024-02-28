package org.apache.solr.pkg;

import org.apache.solr.response.transform.ValueAugmenterFactory;
import org.junit.Test;

public class ValueAugmenterPublicTest {

    @Test
    public void testValueAugmenterIsOpenForExtension() {
        // this should compile from this package
        new ValueAugmenterFactory.ValueAugmenter("bee_sI", new Object());
    }

}
