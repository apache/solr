package org.apache.solr.handler.api;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

public class V2ApiUtilsTest extends SolrTestCaseJ4 {

    @Test
    public void testReadsDisableV2ApiSysprop() {
       System.clearProperty("disable.v2.api");
       assertTrue("v2 API should be enabled if sysprop not specified", V2ApiUtils.isEnabled());

       System.setProperty("disable.v2.api", "false");
       assertTrue("v2 API should be enabled if sysprop explicitly enables it", V2ApiUtils.isEnabled());

       System.setProperty("disable.v2.api", "asdf");
       assertTrue("v2 API should be enabled if sysprop has unexpected value", V2ApiUtils.isEnabled());

       System.setProperty("disable.v2.api", "true");
       assertFalse("v2 API should be disabled if sysprop explicitly disables it", V2ApiUtils.isEnabled());
    }
}
