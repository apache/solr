package org.apache.solr.servlet;

import java.util.ArrayList;
import java.util.regex.Pattern;

public interface PathExcluder {
  void setExcludePatterns(ArrayList<Pattern> excludePatterns);
}
