package demo.search;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;
import static org.junit.jupiter.api.Assertions.*;

class OpenSearchServiceTest {
  @Test void bulkItemFailureIsRetriedByThrowingAndNestedPredicatesStayTogether() throws Exception {
    var captured = new AtomicReference<String>();
    var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", exchange -> {
      captured.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
      if (exchange.getRequestMethod().equals("HEAD")) { exchange.sendResponseHeaders(200, -1); exchange.close(); return; }
      String response = exchange.getRequestURI().getPath().equals("/_bulk")
          ? "{\"errors\":true,\"items\":[{\"index\":{\"error\":{\"reason\":\"disk full\"}}}]}"
          : "{\"hits\":{\"total\":{\"value\":0},\"hits\":[]}}";
      byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, bytes.length); exchange.getResponseBody().write(bytes); exchange.close();
    });
    server.start();
    try {
      var mapper = JsonMapper.builder().build();
      var search = new OpenSearchService("http://127.0.0.1:" + server.getAddress().getPort(), mapper);
      assertThrows(java.io.IOException.class, () -> search.indexBatch(List.of("{\"id\":\"employee-1\"}")));
      assertTrue(captured.get().contains("\"_id\":\"employee-1\""));
      search.search("", "", "", "Java", "Project 0", 5, "Lead", 0, 20);
      var filters = mapper.readTree(captured.get()).path("query").path("bool").path("filter");
      assertEquals("skills", filters.get(0).path("nested").path("path").asString());
      assertEquals(2, filters.get(0).path("nested").path("query").path("bool").path("filter").size());
      assertEquals("assignments", filters.get(1).path("nested").path("path").asString());
      assertEquals(2, filters.get(1).path("nested").path("query").path("bool").path("filter").size());
    } finally { server.stop(0); }
  }
}
