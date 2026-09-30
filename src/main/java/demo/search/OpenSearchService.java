package demo.search;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class OpenSearchService {
  private static final String INDEX = "employees-v1";
  private static final String MAPPING = """
      {"settings":{"number_of_shards":1,"number_of_replicas":0,"refresh_interval":"5s"},
       "mappings":{"properties":{
         "id":{"type":"keyword"},"firstName":{"type":"text","fields":{"keyword":{"type":"keyword"}}},
         "lastName":{"type":"text","fields":{"keyword":{"type":"keyword"}}},
         "email":{"type":"keyword"},"department":{"type":"keyword"},
         "title":{"type":"text"},"status":{"type":"keyword"},
         "address":{"properties":{"city":{"type":"keyword"},"state":{"type":"keyword"},"country":{"type":"keyword"},"postalCode":{"type":"keyword"}}},
         "skills":{"type":"nested","properties":{"name":{"type":"keyword"},"level":{"type":"integer"}}},
         "assignments":{"type":"nested","properties":{"project":{"type":"keyword"},"role":{"type":"keyword"},"region":{"type":"keyword"},"active":{"type":"boolean"}}},
         "manager":{"properties":{"id":{"type":"keyword"},"name":{"type":"text"}}}
       }}}
      """;
  private final String base;
  private final ObjectMapper json;
  private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

  public OpenSearchService(@Value("${app.opensearch-url}") String base, ObjectMapper json) {
    this.base = base; this.json = json;
  }

  public void indexBatch(List<String> documents) throws Exception {
    if (documents.isEmpty()) return;
    ensureIndex();
    StringBuilder body = new StringBuilder();
    for (String document : documents) {
      JsonNode node = json.readTree(document);
      body.append(json.writeValueAsString(Map.of("index", Map.of("_index", INDEX, "_id", node.path("id").asText())))).append('\n');
      body.append(document).append('\n');
    }
    JsonNode response = request("POST", "/_bulk", body.toString(), "application/x-ndjson");
    if (response.path("errors").asBoolean()) throw new IOException("OpenSearch bulk indexing failed: " + response.path("items").findValues("error").stream().limit(3).toList());
  }

  public JsonNode search(String text, String department, String city, String skill, String project, int minSkillLevel, String role, int page, int size) throws Exception {
    ensureIndex();
    var filters = new java.util.ArrayList<Object>();
    if (department != null && !department.isBlank()) filters.add(Map.of("term", Map.of("department", department)));
    if (city != null && !city.isBlank()) filters.add(Map.of("term", Map.of("address.city", city)));
    if (skill != null && !skill.isBlank()) filters.add(Map.of("nested", Map.of("path", "skills", "query", Map.of("bool", Map.of("filter", List.of(
        Map.of("term", Map.of("skills.name", skill)), Map.of("range", Map.of("skills.level", Map.of("gte", minSkillLevel)))))))));
    var assignmentFilters = new java.util.ArrayList<Object>();
    if (project != null && !project.isBlank()) assignmentFilters.add(Map.of("term", Map.of("assignments.project", project)));
    if (role != null && !role.isBlank()) assignmentFilters.add(Map.of("term", Map.of("assignments.role", role)));
    if (!assignmentFilters.isEmpty()) filters.add(Map.of("nested", Map.of("path", "assignments", "query", Map.of("bool", Map.of("filter", assignmentFilters)))));
    Object must = text == null || text.isBlank() ? Map.of("match_all", Map.of())
        : Map.of("multi_match", Map.of("query", text, "fields", List.of("firstName^3", "lastName^3", "title", "manager.name")));
    Map<String, Object> body = Map.of("from", Math.max(0, page) * size, "size", size,
        "track_total_hits", true, "query", Map.of("bool", Map.of("must", List.of(must), "filter", filters)));
    return request("POST", "/" + INDEX + "/_search", json.writeValueAsString(body), "application/json");
  }

  public long count() throws Exception {
    ensureIndex();
    return request("GET", "/" + INDEX + "/_count", null, null).path("count").asLong();
  }

  private synchronized void ensureIndex() throws Exception {
    HttpResponse<String> result = http.send(HttpRequest.newBuilder(URI.create(base + "/" + INDEX)).method("HEAD", HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
    if (result.statusCode() == 404) request("PUT", "/" + INDEX, MAPPING, "application/json");
    else if (result.statusCode() >= 300) throw new IOException("OpenSearch index check: " + result.statusCode());
  }

  private JsonNode request(String method, String path, String body, String contentType) throws Exception {
    HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(60));
    if (contentType != null) builder.header("Content-Type", contentType);
    builder.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
    HttpResponse<String> response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() >= 300) throw new IOException("OpenSearch HTTP " + response.statusCode() + ": " + response.body());
    return json.readTree(response.body());
  }
}
