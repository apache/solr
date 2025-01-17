package org.apache.solr.util;

import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;

import java.io.IOException;

public class SolrDefaultScorerSupplier extends ScorerSupplier {
        private final Scorer scorer;

        public SolrDefaultScorerSupplier(Scorer scorer) {
            this.scorer = scorer;
        }

        @Override
        public Scorer get(long leadCost) throws IOException {
            return scorer;
        }

        @Override
        public long cost() {
            return scorer.iterator().cost();
        }
    }

