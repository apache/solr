package com.flipkart.solr.ltr.query;

import java.util.Optional;
import java.util.Set;

public interface BaseRescoringContext {
  Optional<String> getCustomGroupField();
  Set<String> getGroupsRequired();
  int getLimitPerGroup();
}
