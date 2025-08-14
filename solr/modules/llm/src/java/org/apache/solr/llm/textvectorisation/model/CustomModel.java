package org.apache.solr.llm.textvectorisation.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.DimensionAwareEmbeddingModel;
import dev.langchain4j.model.output.Response;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

// import lombok.Builder;

public class CustomModel extends DimensionAwareEmbeddingModel {

  private final boolean waitForModel;
  private ObjectMapper mapper = new ObjectMapper();
  private Duration timeout;
  private final String endpointUrl;
  private final String fieldName;

  public CustomModel(Boolean waitForModel, Duration timeout, String endpointUrl, String fieldName) {
    this.endpointUrl = endpointUrl;
    this.fieldName = fieldName;

    timeout = timeout == null ? Duration.ofSeconds(30) : timeout;

    mapper = new ObjectMapper();

    this.waitForModel = waitForModel == null || waitForModel;
  }

  @Override
  public Response<List<Embedding>> embedAll(List<TextSegment> textSegments) {
    
    List<Embedding> embeddings =
        textSegments.stream()
            .map(TextSegment::text)
            .map(this::doEmbeddingCall)
            .map(Embedding::from)
            .toList();

    return Response.from(embeddings);
  }

  private List<Float> doEmbeddingCall(String text) {
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpPost post = new HttpPost(endpointUrl);
      post.setHeader("Content-Type", "application/json");

      Map<String, String> data = new HashMap<>();
      data.put(fieldName, text);

      ObjectMapper mapper = new ObjectMapper();
      String jsonPayload = mapper.writeValueAsString(data);
      post.setEntity(new StringEntity(jsonPayload, "UTF-8"));

      try (CloseableHttpResponse response = httpClient.execute(post)) {
        HttpEntity entity = response.getEntity();

        String json = EntityUtils.toString(entity);
        return parseFloatArray(json);
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public List<Float> parseFloatArray(String json) {
    // You can also use double[].class or List<Double> if you prefer
    try {
      return mapper.readValue(
          json, mapper.getTypeFactory().constructCollectionType(List.class, Float.class));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static CustomModelBuilder builder() {
    /*      for (HuggingFaceEmbeddingModelBuilderFactory factory : loadFactories(CustomModel.class)) {
        return factory.get();
    }*/
    return new CustomModelBuilder();
  }

  public static class CustomModelBuilder {
    String endpointUrl;
    private String fieldName;

    public CustomModelBuilder() {
      // This is public so it can be extended
      // By default with Lombok it becomes package private
    }

    public CustomModel build() {
      return new CustomModel(false, Duration.ofSeconds(30), endpointUrl, fieldName);
    }

    public void endpointUrl(String endpointUrl) {
      this.endpointUrl = endpointUrl;
    }

    public void fieldName(String fieldName) {
      this.fieldName = fieldName;
    }
  }
}
