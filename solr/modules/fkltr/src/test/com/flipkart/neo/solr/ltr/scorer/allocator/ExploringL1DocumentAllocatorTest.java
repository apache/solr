package com.flipkart.neo.solr.ltr.scorer.allocator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.flipkart.neo.solr.ltr.banner.entity.IndexedBanner;
import com.flipkart.neo.solr.ltr.query.NeoRescoringContext;
import com.flipkart.neo.solr.ltr.query.models.context.VernacularInfo;
import com.flipkart.neo.solr.ltr.query.models.scoring.config.L1ScoringConfig;
import com.flipkart.neo.solr.ltr.query.models.ScoringSchemeConfig;
import com.flipkart.neo.solr.ltr.schema.SchemaFieldNames;
import org.apache.lucene.search.ScoreDoc;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExploringL1DocumentAllocatorTest {

  @Test
  public void testL1AllocationPerGroup() {
    L1DocumentAllocator allocator = new ExploringL1DocumentAllocator();

    String groupA = "a";
    String groupB = "b";
    String groupC = "c";

    String locale = "en";

    ScoreDoc[] docs = new ScoreDoc[5];
    docs[0] = getScoreDocWithGroup(1, locale, 1, groupA, "random1", groupC);
    docs[1] = getScoreDocWithGroup(2, locale, 1, groupA, "random2", groupB);
    docs[2] = getScoreDocWithGroup(3, locale, 1, groupA, "random2", groupC);
    docs[3] = getScoreDocWithGroup(4, locale, 1, groupB, "random1");
    docs[4] = getScoreDocWithGroup(5, locale, 1, groupA, "random3");

    NeoRescoringContext rescoringContext = new NeoRescoringContext();

    rescoringContext.setVernacularInfo(getVernacularInfo(locale));
    rescoringContext.setL1ScoringConfig(getL1ScoringConfig());

    ScoringSchemeConfig scoringSchemeConfig = new ScoringSchemeConfig();
    Map<String, Integer> groupsRequiredWithLimits = new HashMap<>();
    groupsRequiredWithLimits.put(groupA, 5);
    groupsRequiredWithLimits.put(groupB, 1);
    scoringSchemeConfig.setGroupField(SchemaFieldNames.THEME_NAME_PLUS_TEMPLATE_ID_STRINGS);
    scoringSchemeConfig.setGroupsRequiredWithLimits(groupsRequiredWithLimits);

    rescoringContext.setScoringSchemeConfig(scoringSchemeConfig);

    Map<String, List<ScoreDoc>> groupedScoreDocs =
        allocator.allocatePerGroup(docs, rescoringContext);

    assertEquals(2, groupedScoreDocs.size());
    assertEquals(4, groupedScoreDocs.get(groupA).size());
    assertEquals(1, groupedScoreDocs.get(groupB).size());
  }

  @Test
  public void testL1AllocationPerGroupWithManuallyRanked() {
    L1DocumentAllocator allocator = new ExploringL1DocumentAllocator();

    String groupA = "a";
    String groupB = "b";
    String groupC = "c";

    String locale = "en";

    ScoreDoc[] docs = new ScoreDoc[5];
    docs[0] = getScoreDocWithGroup(1, locale, 1, groupA, "random1", groupC);
    docs[1] = getScoreDocWithGroup(2, locale, 1, groupA, "random2", groupB);
    docs[2] = getScoreDocWithGroup(3, locale, 1, groupA, "random2", groupC);
    docs[3] = getScoreDocWithGroup(4, locale, 1, groupB, "random1");

    docs[4] = getScoreDocWithGroup(5, locale, Float.MIN_VALUE, groupA, "random3");
    String creativeGroupId1 = "cg1";
    ((IndexedBanner)docs[4].meta).setVernacularCreativeGroupId(creativeGroupId1);

    NeoRescoringContext rescoringContext = new NeoRescoringContext();

    rescoringContext.setVernacularInfo(getVernacularInfo(locale));

    rescoringContext.setL1ScoringConfig(getL1ScoringConfig());

    ScoringSchemeConfig scoringSchemeConfig = new ScoringSchemeConfig();
    Map<String, Integer> groupsRequiredWithLimits = new HashMap<>();
    groupsRequiredWithLimits.put(groupA, 2);
    groupsRequiredWithLimits.put(groupB, 1);
    scoringSchemeConfig.setGroupField(SchemaFieldNames.THEME_NAME_PLUS_TEMPLATE_ID_STRINGS);
    scoringSchemeConfig.setGroupsRequiredWithLimits(groupsRequiredWithLimits);
    Map<String, Set<String>> groupsWithPreferredSecondaryIds = new HashMap<>();
    groupsWithPreferredSecondaryIds.put(groupA, new HashSet<>(Arrays.asList(creativeGroupId1, "cgRandom1")));
    scoringSchemeConfig.setGroupsWithPreferredSecondaryIds(groupsWithPreferredSecondaryIds);

    rescoringContext.setScoringSchemeConfig(scoringSchemeConfig);

    // test with no explore.
    rescoringContext.getL1ScoringConfig().setExplorePercentage(0);
    Map<String, List<ScoreDoc>> groupedScoreDocs = allocator.allocatePerGroup(docs, rescoringContext);

    assertEquals(2, groupedScoreDocs.size());
    assertEquals(1, groupedScoreDocs.get(groupB).size());
    assertEquals(2, groupedScoreDocs.get(groupA).size());
    assertTrue(groupedScoreDocs.get(groupA).contains(docs[4]));


    // test with explore.
    rescoringContext.getL1ScoringConfig().setExplorePercentage(30);
    groupedScoreDocs = allocator.allocatePerGroup(docs, rescoringContext);

    assertEquals(2, groupedScoreDocs.size());
    assertEquals(1, groupedScoreDocs.get(groupB).size());
    assertEquals(2, groupedScoreDocs.get(groupA).size());
    assertTrue(groupedScoreDocs.get(groupA).contains(docs[4]));

    // test the case where all the contents of a group come from manually ranked.
    rescoringContext.getL1ScoringConfig().setExplorePercentage(30);
    ((IndexedBanner)docs[2].meta).setVernacularCreativeGroupId(creativeGroupId1);
    groupedScoreDocs = allocator.allocatePerGroup(docs, rescoringContext);
    assertEquals(2, groupedScoreDocs.size());
    assertEquals(1, groupedScoreDocs.get(groupB).size());
    assertEquals(2, groupedScoreDocs.get(groupA).size());
    assertTrue(groupedScoreDocs.get(groupA).contains(docs[4]));
    assertTrue(groupedScoreDocs.get(groupA).contains(docs[2]));
  }

  @Test
  public void testL1Allocation() {
    L1DocumentAllocator allocator = new ExploringL1DocumentAllocator();

    ScoreDoc[] docs = new ScoreDoc[5];
    docs[0] = new ScoreDoc(1, 80);
    docs[1] = new ScoreDoc(2, 91);
    docs[2] = new ScoreDoc(3, 99);
    docs[3] = new ScoreDoc(4, 60);
    docs[4] = new ScoreDoc(5, 81);


    allocator.allocate(docs, 3);

    Set<Integer> topSet = new HashSet<>(Arrays.asList(2,3,5));
    Set<Integer> bottomSet = new HashSet<>(Arrays.asList(1,4));

    assertEquals(topSet, new HashSet<>(Arrays.asList(docs[0].doc, docs[1].doc, docs[2].doc)));
    assertEquals(bottomSet, new HashSet<>(Arrays.asList(docs[3].doc, docs[4].doc)));
  }

  private ScoreDoc getScoreDocWithGroup(int docId, String locale, float score, String... groupName) {
    ScoreDoc doc = new ScoreDoc(docId, score);
    IndexedBanner indexedBanner = new IndexedBanner(Integer.toString(docId), 1);
    indexedBanner.setLocale(locale);
    indexedBanner.setCreativeId("cr123");
    indexedBanner.setThemeNamePlusTemplateIdStrings(Arrays.asList(groupName));
    doc.meta = indexedBanner;
    return doc;
  }

  private VernacularInfo getVernacularInfo(String locale) {
    VernacularInfo vernacularInfo = new VernacularInfo();
    vernacularInfo.setLocale(locale);
    vernacularInfo.setLocaleRelaxed(false);
    return vernacularInfo;
  }

  private L1ScoringConfig getL1ScoringConfig() {
    L1ScoringConfig l1ScoringConfig = new L1ScoringConfig();
    l1ScoringConfig.setExplorePercentage(30);
    l1ScoringConfig.setTimeBucketInMins(10);
    return l1ScoringConfig;
  }
}
