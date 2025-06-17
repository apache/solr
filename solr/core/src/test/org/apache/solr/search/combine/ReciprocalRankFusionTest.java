package org.apache.solr.search.combine;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.apache.lucene.search.TotalHits;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.component.ShardDoc;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.QueryResult;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.solr.common.params.CombinerParams.COMBINER_RRF_K;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ReciprocalRankFusionTest extends SolrTestCaseJ4 {

    public static ReciprocalRankFusion reciprocalRankFusion;

    @BeforeClass
    public static void beforeClass() {
        SolrParams params = new ModifiableSolrParams(Map.of(COMBINER_RRF_K, new String[]{"10"}));
        reciprocalRankFusion = new ReciprocalRankFusion(params);
    }
    @Test
    public void testSearcherCombine() {
        List<QueryResult> rankedList = getQueryResults();
        QueryResult result = reciprocalRankFusion.combine(rankedList);
        assertEquals(3, result.getDocList().size());
    }

    private static List<QueryResult> getQueryResults() {
        QueryResult r1 = new QueryResult();
        r1.setDocList(new DocSlice(
                0, 2, new int[]{1,2}, new float[]{0.67f, 0,62f},
                3, 0.67f, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO));
        QueryResult r2 = new QueryResult();
        r2.setDocList(new DocSlice(0, 1, new int[]{0}, new float[]{0.87f},
                2, 0.87f, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO));
        return List.of(r1, r2);
    }

    @Test
    public void testShardCombine() {
        Map<String, List<ShardDoc>> shardDocMap = new HashMap<>();
        ShardDoc shardDoc = new ShardDoc();
        shardDoc.id = "id1";
        shardDoc.shard = "shard1";
        shardDoc.orderInShard = 1;
        List<ShardDoc> shardDocList = new ArrayList<>();
        shardDocList.add(shardDoc);
        shardDoc = new ShardDoc();
        shardDoc.id = "id2";
        shardDoc.shard = "shard1";
        shardDoc.orderInShard = 2;
        shardDocList.add(shardDoc);
        shardDocMap.put(shardDoc.shard, shardDocList);

        shardDoc = new ShardDoc();
        shardDoc.id = "id2";
        shardDoc.shard = "shard2";
        shardDoc.orderInShard = 1;
        shardDocMap.put(shardDoc.shard, List.of(shardDoc));
        List<ShardDoc> shardDocs = reciprocalRankFusion.combine(shardDocMap);
        assertEquals(2, shardDocs.size());
        assertEquals("id2", shardDocs.getFirst().id);
    }

    @Test
    public void testExplain() {
        List<QueryResult> rankedLists = getQueryResults();
        List<DocList> docLists = new ArrayList<>(rankedLists.size());
        for (QueryResult rankedList : rankedLists) {
            docLists.add(rankedList.getDocList());
        }
        QueryResult combinedResult = new QueryResult();
        //reciprocalRankFusion.getExplanations()
    }
}