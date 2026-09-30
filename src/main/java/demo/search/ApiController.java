package demo.search;

import java.util.Map;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.MediaType;

@RestController
@RequestMapping("/api")
public class ApiController {
  private final EmployeeService employees;
  private final SeedService seed;
  private final OpenSearchService search;
  private final MongoTemplate mongo;
  private final OutboxPublisher publisher;

  public ApiController(EmployeeService employees, SeedService seed, OpenSearchService search, MongoTemplate mongo, OutboxPublisher publisher) {
    this.employees = employees; this.seed = seed; this.search = search; this.mongo = mongo; this.publisher = publisher;
  }

  @PostMapping("/employees") @ResponseStatus(org.springframework.http.HttpStatus.CREATED)
  Employee create(@RequestBody Employee input) { return employees.save(input); }
  @PostMapping("/seed") SeedService.Status seed(@RequestParam(defaultValue = "1000000") long count) { return seed.start(count); }
  @GetMapping("/seed") SeedService.Status seedStatus() { return seed.status(); }
  @GetMapping("/pipeline/relay") OutboxPublisher.Status relayStatus() { return publisher.status(); }
  @GetMapping("/stats") Map<String, Long> stats() throws Exception {
    return Map.of("mongo", mongo.getCollection("employees").countDocuments(),
        "opensearch", search.count(), "pendingOutbox", mongo.getCollection("outbox").countDocuments(new org.bson.Document("published", false)));
  }
  @GetMapping(value = "/search", produces = MediaType.APPLICATION_JSON_VALUE) String search(@RequestParam(defaultValue = "") String q,
      @RequestParam(defaultValue = "") String department, @RequestParam(defaultValue = "") String city,
      @RequestParam(defaultValue = "") String skill, @RequestParam(defaultValue = "") String project,
      @RequestParam(defaultValue = "1") int minSkillLevel, @RequestParam(defaultValue = "") String role,
      @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "20") int size) throws Exception {
    if (size < 1 || size > 100 || page < 0 || ((long) page + 1) * size > 10000)
      throw new IllegalArgumentException("size must be 1–100 and pagination is limited to the first 10,000 matches; narrow your search");
    if (minSkillLevel < 1 || minSkillLevel > 5) throw new IllegalArgumentException("minSkillLevel must be 1–5");
    return search.search(q, department, city, skill, project, minSkillLevel, role, page, size).toString();
  }
}
