package demo.search;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Service;
import org.springframework.data.mongodb.core.MongoTemplate;
import jakarta.annotation.PreDestroy;

@Service
public class SeedService {
  private static final String[] FIRST = {"Ava", "Liam", "Noah", "Mia", "Aria", "Ethan", "Ivy", "Owen"};
  private static final String[] LAST = {"Patel", "Smith", "Chen", "Garcia", "Khan", "Brown", "Kim", "Wilson"};
  private static final String[] CITY = {"Bengaluru", "Hyderabad", "London", "Seattle", "Toronto", "Berlin"};
  private static final String[] DEPT = {"Engineering", "Finance", "Sales", "People", "Operations"};
  private final EmployeeService employees;
  private final MongoTemplate mongo;
  private final ExecutorService worker = Executors.newSingleThreadExecutor();
  private final AtomicBoolean running = new AtomicBoolean();
  private final AtomicLong inserted = new AtomicLong();
  private volatile long target;
  private volatile String error;

  public SeedService(EmployeeService employees, MongoTemplate mongo) { this.employees = employees; this.mongo = mongo; }

  public synchronized Status start(long count) {
    if (count < 1 || count > 2_000_000) throw new IllegalArgumentException("count must be 1 to 2,000,000");
    if (running.get()) return status();
    restore();
    target = Math.max(count, inserted.get()); error = null;
    if (inserted.get() >= count) return status();
    running.set(true);
    worker.submit(() -> {
      try {
        for (long start = inserted.get(); start < count && !Thread.currentThread().isInterrupted(); start += 500) {
          List<Employee> batch = new ArrayList<>();
          for (long i = start; i < Math.min(start + 500, count); i++) batch.add(generate(i));
          employees.saveSeedBatch(batch, start + batch.size(), count);
          inserted.addAndGet(batch.size());
        }
      } catch (Exception e) { error = e.getMessage(); }
      finally { running.set(false); }
    });
    return status();
  }

  private void restore() {
    var checkpoint = mongo.findById("employee-seed", org.bson.Document.class, "jobs");
    if (checkpoint != null) {
      inserted.set(((Number) checkpoint.get("inserted")).longValue());
      target = ((Number) checkpoint.get("target")).longValue();
    }
  }
  public synchronized Status status() {
    if (!running.get()) restore();
    return new Status(running.get(), inserted.get(), target, error);
  }
  @PreDestroy void shutdown() { worker.shutdownNow(); }
  public record Status(boolean running, long inserted, long target, String error) {}

  private Employee generate(long i) {
    String first = FIRST[(int)(i % FIRST.length)];
    String last = LAST[(int)((i / FIRST.length) % LAST.length)];
    String city = CITY[(int)(i % CITY.length)];
    String department = DEPT[(int)(i % DEPT.length)];
    return new Employee("emp-" + i, first, last, "employee" + i + "@example.test", department,
        i % 7 == 0 ? "Principal Engineer" : "Specialist", i % 13 == 0 ? "On leave" : "Active",
        new Employee.Address(city, "Region " + i % 12, i % 3 == 0 ? "India" : "Global", "P" + i % 10000),
        List.of(new Employee.Skill(i % 2 == 0 ? "Java" : "Python", (int)(i % 5 + 1)),
            new Employee.Skill("OpenSearch", (int)(i % 4 + 1))),
        List.of(new Employee.Assignment("Project " + i % 200, i % 3 == 0 ? "Lead" : "Contributor",
            city, true), new Employee.Assignment("Platform " + i % 20, "Reviewer", city, false)),
        new Employee.Manager("manager-" + i % 1000, "Manager " + i % 1000));
  }
}
