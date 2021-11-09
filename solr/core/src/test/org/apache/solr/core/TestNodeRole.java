package org.apache.solr.core;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestNodeRole extends SolrTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  @SuppressWarnings("rawtypes")
  public void testZkData() {
    //empty
    Map<String,Object> rolesData = null;
    //start a node with overseer role
    rolesData = new NodeRole("overseer").modifyRoleData(rolesData, "node1");
    assertEquals("node1", _val(rolesData,  "overseer[0]"));
    assertEquals("node1", _val(rolesData,  "no-replicas[0]"));
    //now start another node with overseer
    rolesData = new NodeRole("overseer").modifyRoleData(rolesData, "node2");
    assertEquals("node2", _val(rolesData,  "overseer[1]"));
    assertEquals("node2", _val(rolesData,  "no-replicas[1]"));
    //now start another node with overseer,data
    rolesData = new NodeRole("overseer,data").modifyRoleData(rolesData, "node3");

    assertEquals("node3", _val(rolesData, "overseer[2]"));
    assertEquals(2, ((Collection) _val(rolesData, "no-replicas")).size() );
    //now restart node2 with no role

    rolesData = new NodeRole(null).modifyRoleData(rolesData, "node2");
    assertEquals("node3", _val(rolesData, "overseer[1]"));
    assertEquals(1, ((Collection) _val(rolesData, "no-replicas")).size() );
    //now restart node1 with the original values
    assertNull(new NodeRole("overseer").modifyRoleData(rolesData, "node1"));

    assertEquals("node1", _val(rolesData,  "overseer[0]"));
    assertEquals("node1", _val(rolesData,  "no-replicas[0]"));
    //now restart node1 with overseer,data
    rolesData =  new NodeRole("overseer,data").modifyRoleData(rolesData, "node1");
    assertEquals("node1", _val(rolesData,  "overseer[0]"));
    assertEquals(0, ((Collection) _val(rolesData, "no-replicas")).size() );

    log.info("rolesdata : {}", Utils.toJSONString(rolesData));
  }

  private Object _val(Map<String, Object> rolesData, String path) {
    return Utils.getObjectByPath(rolesData, false, path);
  }
}
