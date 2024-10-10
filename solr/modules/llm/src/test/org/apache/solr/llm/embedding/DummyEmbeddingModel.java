package org.apache.solr.llm.embedding;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;

import java.util.List;

public class DummyEmbeddingModel implements EmbeddingModel {
    public DummyEmbeddingModel() {
    }

    @Override
    public Response<Embedding> embed(String text) {
        Embedding dummy = new Embedding(new float[]{1.0f,2.0f,3.0f,4.0f});
        return new Response<Embedding>(dummy);
    }

    @Override
    public Response<Embedding> embed(TextSegment textSegment) {
        Embedding dummy = new Embedding(new float[]{1.0f,2.0f,3.0f,4.0f});
        return new Response<Embedding>(dummy);
    }

    @Override
    public Response<List<Embedding>> embedAll(List<TextSegment> textSegments) {
        return null;
    }

    @Override
    public int dimension() {
        return 4;
    }
    
    public static DummyEmbeddingModelBuilder builder(){
        return new DummyEmbeddingModelBuilder();
    }
   
    public static class DummyEmbeddingModelBuilder{
        public DummyEmbeddingModelBuilder() {
        }
        
        public DummyEmbeddingModel build(){
            return new DummyEmbeddingModel();
        }
    }
    
}
