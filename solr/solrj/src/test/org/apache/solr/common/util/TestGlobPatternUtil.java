package org.apache.solr.common.util;

import org.apache.solr.SolrTestCase;

public class TestGlobPatternUtil extends SolrTestCase {

  public void testMatches() {
    assertTrue(GlobPatternUtil.matches("*_str", "user_str"));
    assertFalse(GlobPatternUtil.matches("*_str", "str_user"));
    assertTrue(GlobPatternUtil.matches("str_*", "str_user"));
    assertFalse(GlobPatternUtil.matches("str_*", "user_str"));
    assertTrue(GlobPatternUtil.matches("str?", "str1"));
    assertFalse(GlobPatternUtil.matches("str?", "str_user"));
    assertTrue(GlobPatternUtil.matches("user_*_str", "user_type_str"));
    assertFalse(GlobPatternUtil.matches("user_*_str", "user_str"));
  }
}
