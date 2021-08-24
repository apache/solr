package org.apache.solr.handler.component;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.solr.search.SortSpec;
import org.mockito.Mockito;

public class MockSortSpecBuilder {
    private final SortSpec sortSpec;

    public MockSortSpecBuilder() {
        this.sortSpec = Mockito.mock(SortSpec.class);
        Mockito.when(sortSpec.getCount()).thenReturn(10);
    }

    public static MockSortSpecBuilder create() {
        return new MockSortSpecBuilder();
    }

    public MockSortSpecBuilder withSortFields(SortField[] sortFields) {
        Sort sort = Mockito.mock(Sort.class);
        Mockito.when(sort.getSort()).thenReturn(sortFields);
        Mockito.when(sortSpec.getSort()).thenReturn(sort);
        return this;
    }

    public MockSortSpecBuilder withIncludesNonScoreOrDocSortField(boolean include) {
        Mockito.when(sortSpec.includesNonScoreOrDocField()).thenReturn(include);
        return this;
    }

    public SortSpec build() {
        return sortSpec;
    }

}
