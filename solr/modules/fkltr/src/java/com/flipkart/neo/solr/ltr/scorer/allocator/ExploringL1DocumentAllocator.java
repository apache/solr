package com.flipkart.neo.solr.ltr.scorer.allocator;

import com.flipkart.neo.solr.ltr.banner.entity.IndexedBanner;
import com.flipkart.neo.solr.ltr.query.NeoRescoringContext;
import com.flipkart.neo.solr.ltr.schema.SchemaFieldNames;
import com.flipkart.neo.solr.ltr.scorer.util.HeapUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.lucene.search.ScoreDoc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ExploringL1DocumentAllocator implements L1DocumentAllocator {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int ROUGH_MAX_DOCS_PER_GROUP = 50;

  private static final String DEFAULT_LOCALE = "en";

  @Override
  public Map<String, List<ScoreDoc>> allocatePerGroup(ScoreDoc[] docs, NeoRescoringContext rescoringContext) {
    Set<String> groupsRequired = rescoringContext.getGroupsRequired();
    // initialising groupedScoreDocs which will hold docs per group.
    Map<String, List<ScoreDoc>> groupedScoreDocs = new HashMap<>(groupsRequired.size(), 1);
    for (String groupName: groupsRequired) {
      // setting initialCapacity to groupTopN under the assumption that docs per group will generally be greater than it.
      // taking min of groupTopN, ROUGH_MAX_DOCS_PER_GROUP as in some cases groupTopN can be inflated (eg. to avoid
      // limit for some group).
      groupedScoreDocs.put(groupName,
          new ArrayList<>(Math.min(ROUGH_MAX_DOCS_PER_GROUP, rescoringContext.getLimitForGroup(groupName))));
    }

    // reducing it to enum first to avoid string comparison per doc.
    GroupingCriteria groupingCriteria = getGroupingCriteria(rescoringContext.getScoringSchemeConfig().getGroupField());

    if (!rescoringContext.getVernacularInfo().isLocaleRelaxed()) {
      // relying on the fact that in non-relaxed case documents are already filtered by requestLocale.
      collectDocsForGivenGroups(docs, groupingCriteria, groupedScoreDocs);
    } else {
      // allocate for requestLocale
      String requestLocale = rescoringContext.getVernacularInfo().getLocale();
      collectDocsForGivenGroupsAndLocale(docs, groupingCriteria, requestLocale, groupedScoreDocs);

      // check and allocate for fallbackLocale
      String fallbackLocale = getFallbackLocale();
      if (!requestLocale.equals(fallbackLocale)) {
        // fallback is applicable only for the groups where no doc was found with requestLocale.
        Set<String> groupsWithNoDocs = groupedScoreDocs.entrySet().stream()
            .filter(entry -> entry.getValue().isEmpty()).map(Map.Entry::getKey).collect(Collectors.toSet());
        if (!groupsWithNoDocs.isEmpty()) {
          collectDocsForGivenGroupsAndLocale(docs, groupingCriteria, groupsWithNoDocs, fallbackLocale, groupedScoreDocs);
        }
      }
    }

    // sort and limit per group and explore if required.
    int l1ExplorePercentage = rescoringContext.getL1ScoringConfig().getExplorePercentage();
    long seedValueForExplore = getSeedValueForExplore(rescoringContext);
    groupedScoreDocs.entrySet().parallelStream().forEach(entry -> {
      int groupTopN = rescoringContext.getLimitForGroup(entry.getKey());

      if (entry.getValue().size() > groupTopN) {
        Set<ScoreDoc> selectedDocs = new HashSet<>(groupTopN, 1);

        Set<String> preferredCreativeGroupIds = rescoringContext.getPreferredSecondaryIdsForGroup(entry.getKey());
        if (CollectionUtils.isNotEmpty(preferredCreativeGroupIds)) {
          for (ScoreDoc doc: entry.getValue()) {
            if (preferredCreativeGroupIds.contains(((IndexedBanner) doc.meta).getVernacularCreativeGroupId())) {
              selectedDocs.add(doc);
              if (selectedDocs.size() == groupTopN) break;
            }
          }
        }

        // todo:fkltr add documentation for these steps.
        if (selectedDocs.size() < groupTopN) {
          entry.getValue().sort((o1, o2) -> Float.compare(o2.score, o1.score));
          if (l1ExplorePercentage != 0) {
            // todo:fkltr it can be optimised to use heap-based topN instead, it will require new strategy for explore.
            selectDocsWithExplore(selectedDocs, entry.getValue(), groupTopN, seedValueForExplore, l1ExplorePercentage);
          } else {
            for (ScoreDoc doc: entry.getValue()) {
              selectedDocs.add(doc);
              if (selectedDocs.size() == groupTopN) break;
            }
          }
        }

        entry.setValue(new ArrayList<>(selectedDocs));
      }

      if (log.isDebugEnabled()) {
        logL1Scores(rescoringContext.getRequestId(), entry.getKey(), entry.getValue());
      }
    });

    return groupedScoreDocs;
  }

  // todo:fkltr add explore.
  public void allocate(ScoreDoc[] docs, int topN) {
    // build the heap using first topN docs.
    HeapUtil.minHeapify(docs, topN);

    // Once the heap is ready, if the score of subsequent doc is lower than the minimum don't do anything.
    // Otherwise swap it with the minimum and fix the heap.
    for (int i=topN; i<docs.length; i++) {
      if (docs[i].score > docs[0].score) {
        ScoreDoc tmp = docs[0];
        docs[0] = docs[i];
        docs[i] = tmp;
        HeapUtil.minHeapAdjust(docs, topN, 0);
      }
    }
  }

  private GroupingCriteria getGroupingCriteria(String groupField) {
    switch (groupField) {
      case SchemaFieldNames.THEME_NAME_PLUS_TEMPLATE_ID_STRINGS:
        return GroupingCriteria.THEME_NAME_PLUS_TEMPLATE_ID_STRINGS;
      case SchemaFieldNames.THEMES:
        return GroupingCriteria.THEMES;
      case SchemaFieldNames.STORE_IDS:
        return GroupingCriteria.STORE_IDS;
      case SchemaFieldNames.SERVING_TEAM_ID:
        return GroupingCriteria.SERVING_TEAM_ID;
      default:
        throw new RuntimeException("groupField not supported");
    }
  }

  private void collectDocsForGivenGroups(ScoreDoc[] hits, GroupingCriteria groupingCriteria,
                                         Map<String, List<ScoreDoc>> groupedScoreDocs) {
    for (ScoreDoc hit: hits) {
      if (hit.meta == null) continue;
      collectDocForGivenGroups(hit, groupingCriteria, groupedScoreDocs);
    }
  }

  private void collectDocsForGivenGroupsAndLocale(ScoreDoc[] hits, GroupingCriteria groupingCriteria, String requiredLocale,
                                                  Map<String, List<ScoreDoc>> groupedScoreDocs) {
    for (ScoreDoc hit: hits) {
      if (hit.meta == null) continue;
      IndexedBanner indexedBanner = (IndexedBanner) hit.meta;
      if (!requiredLocale.equals(indexedBanner.getLocale())) continue;

      collectDocForGivenGroups(hit, groupingCriteria, groupedScoreDocs);
    }
  }

  private void collectDocsForGivenGroupsAndLocale(ScoreDoc[] hits, GroupingCriteria groupingCriteria,
                                                  Set<String> groupsRequired, String requiredLocale,
                                                  Map<String, List<ScoreDoc>> groupedScoreDocs) {
    for (ScoreDoc hit: hits) {
      if (hit.meta == null) continue;
      IndexedBanner indexedBanner = (IndexedBanner) hit.meta;
      if (!requiredLocale.equals(indexedBanner.getLocale())) continue;

      collectDocForGivenGroups(hit, groupingCriteria, groupsRequired, groupedScoreDocs);
    }
  }

  private void collectDocForGivenGroups(ScoreDoc hit, GroupingCriteria groupingCriteria,
                                        Map<String, List<ScoreDoc>> groupedScoreDocs) {
    for (String groupName: getDocGroups(hit, groupingCriteria)) {
      List<ScoreDoc> groupDocs = groupedScoreDocs.get(groupName);
      // expects that values in groupedScoreDocs are already initialized.
      if (groupDocs != null) {
        groupDocs.add(hit);
      }
    }
  }

  private void collectDocForGivenGroups(ScoreDoc hit, GroupingCriteria groupingCriteria, Set<String> groupsRequired,
                                        Map<String, List<ScoreDoc>> groupedScoreDocs) {
    for (String groupName: getDocGroups(hit, groupingCriteria)) {
      if (groupsRequired.contains(groupName)) {
        // expects that values in groupedScoreDocs are already initialized.
        groupedScoreDocs.get(groupName).add(hit);
      }
    }
  }

  private Collection<String> getDocGroups(ScoreDoc doc, GroupingCriteria groupingCriteria) {
    switch (groupingCriteria) {
      case THEME_NAME_PLUS_TEMPLATE_ID_STRINGS:
        return ((IndexedBanner) doc.meta).getThemeNamePlusTemplateIdStrings();
      case THEMES:
        return ((IndexedBanner) doc.meta).getThemes();
      case STORE_IDS:
        return ((IndexedBanner) doc.meta).getStoreIds();
      case SERVING_TEAM_ID:
        return ((IndexedBanner) doc.meta).getServingTeamId();
      default:
        throw new RuntimeException("unsupported groupingCriteria");
    }
  }

  private String getFallbackLocale() {
    return DEFAULT_LOCALE;
  }

  private long getSeedValueForExplore(NeoRescoringContext rescoringContext) {
    String userId = rescoringContext.getUserId();
    if (userId == null) userId = "";
    return userId.hashCode() + (rescoringContext.getRequestTimeStampInMillis() /
        ((long) rescoringContext.getL1ScoringConfig().getTimeBucketInMins() * 60 * 1000));
  }

  // keeping the strategy same as existing one, can be optimised and even bettered in terms of explore.
  private void selectDocsWithExplore(Set<ScoreDoc> selectedDocs, List<ScoreDoc> docs, int limit,
                                     long seedValueForExplore, int explorePercentage) {
    for (ScoreDoc doc : docs) {
      if (selectedDocs.size() < limit) {
        if (!selectedDocs.contains(doc)) {
          int position = getExplorePosition(((IndexedBanner) doc.meta).getCreativeId(), seedValueForExplore,
              docs.size(), explorePercentage);
          if (position != -1) {
            selectedDocs.add(docs.get(position));
          } else {
            selectedDocs.add(doc);
          }
        }
      } else {
        break;
      }
    }
  }

  private int getExplorePosition(String creativeId, long seedValueForExplore, int size, int explorePercentage) {
    long random = Math.abs(seedValueForExplore + creativeId.hashCode());
    if (random % 100 <= explorePercentage) {
      return (int) (random % size);
    }
    return -1;
  }

  private void logL1Scores(String requestId, String group, List<ScoreDoc> scoreDocs) {
    if(CollectionUtils.isNotEmpty(scoreDocs)) {
      StringBuilder builder = new StringBuilder();
      builder.append("[ReqId:").append(requestId)
          .append("]").append(" Group:").append(group)
          .append("L1Scores").append(" [");
      for (ScoreDoc doc : scoreDocs) {
        builder.append("Creative: ").append(((IndexedBanner) doc.meta).getCreativeId()).append(": ")
            .append(doc.score).append(", ");
      }
      builder.append("] ");
      if (log.isDebugEnabled()) {
        log.debug(builder.toString());
      }
    }
  }
}
