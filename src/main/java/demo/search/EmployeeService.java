package demo.search;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class EmployeeService {
  private final MongoTemplate mongo;
  private final TransactionTemplate transactions;
  private final ObjectMapper json;

  public EmployeeService(MongoTemplate mongo, TransactionTemplate transactions, ObjectMapper json) {
    this.mongo = mongo; this.transactions = transactions; this.json = json;
  }

  public Employee save(Employee input) {
    if (input.firstName() == null || input.firstName().isBlank() || input.lastName() == null || input.lastName().isBlank())
      throw new IllegalArgumentException("firstName and lastName are required");
    if (input.id() != null && (input.id().startsWith("emp-") || input.id().length() > 200))
      throw new IllegalArgumentException("Use an ID under 200 characters; emp- is reserved for the generator");
    Employee employee = input.id() == null || input.id().isBlank()
        ? new Employee(UUID.randomUUID().toString(), input.firstName(), input.lastName(), input.email(),
            input.department(), input.title(), input.status(), input.address(), input.skills(), input.assignments(), input.manager())
        : input;
    saveBatch(List.of(employee));
    return employee;
  }

  public void saveBatch(List<Employee> employees) {
    saveBatch(employees, null);
  }

  public void saveSeedBatch(List<Employee> employees, long inserted, long target) {
    saveBatch(employees, new org.bson.Document("_id", "employee-seed")
        .append("inserted", inserted).append("target", target));
  }

  private void saveBatch(List<Employee> employees, org.bson.Document checkpoint) {
    List<OutboxEvent> events = new ArrayList<>(employees.size());
    for (Employee employee : employees) {
      try {
        events.add(new OutboxEvent(UUID.randomUUID().toString(), employee.id(), "UPSERT",
            json.writeValueAsString(employee), false, Instant.now()));
      } catch (JacksonException e) { throw new IllegalArgumentException(e); }
    }
    transactions.executeWithoutResult(status -> {
      mongo.insert(employees, Employee.class);
      mongo.insert(events, OutboxEvent.class);
      if (checkpoint != null) mongo.save(checkpoint, "jobs");
    });
  }
}
