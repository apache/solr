package org.apache.solr.llm.textvectorisation.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.DimensionAwareEmbeddingModel;
import dev.langchain4j.model.output.Response;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CustomModel extends DimensionAwareEmbeddingModel {

  private final boolean waitForModel;
  private ObjectMapper mapper = new ObjectMapper();
  private Duration timeout;
  private final String endpointUrl;
  private final String fieldName;

  HttpClient httpClient;

  public CustomModel(Boolean waitForModel, Duration timeout, String endpointUrl, String fieldName) {
    this.endpointUrl = endpointUrl;
    this.fieldName = fieldName;

    timeout = timeout == null ? Duration.ofSeconds(30) : timeout;
    httpClient = HttpClient.newBuilder().connectTimeout(timeout).build();
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

    try {
      // Prepare JSON body
      Map<String, String> data = new HashMap<>();
      data.put(fieldName, text);
      ObjectMapper mapper = new ObjectMapper();
      String jsonPayload = mapper.writeValueAsString(data);

      // Create HTTP client and request

      HttpRequest request =
          HttpRequest.newBuilder()
              .timeout(timeout)
              .uri(URI.create(endpointUrl))
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(jsonPayload, StandardCharsets.UTF_8))
              .build();

      // Send request and get response
      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      // Parse and return
      return parseFloatArray(response.body());

    } catch (IOException | InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

  public List<Float> parseFloatArray(String json) {
    try {
      return mapper.readValue(
          json, mapper.getTypeFactory().constructCollectionType(List.class, Float.class));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static CustomModelBuilder builder() {
    return new CustomModelBuilder();
  }

  public static class CustomModelBuilder {
    String endpointUrl;
    private String fieldName;

    public CustomModelBuilder() {}

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
