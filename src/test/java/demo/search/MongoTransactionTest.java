package demo.search;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "RUN_MONGO_IT", matches = "true")
class MongoTransactionTest {
  @Test void duplicateInBatchRollsBackEmployeesAndOutbox() throws Exception {
    var factory = new SimpleMongoClientDatabaseFactory("mongodb://localhost:27017/employee_atomicity_test?replicaSet=rs0&directConnection=true&w=majority");
    var mongo = new MongoTemplate(factory);
    try {
      mongo.getDb().drop();
      mongo.createCollection(Employee.class);
      mongo.createCollection(OutboxEvent.class);
      var service = new EmployeeService(mongo, new TransactionTemplate(new MongoTransactionManager(factory)), JsonMapper.builder().build());
      var original = employee("existing");
      service.saveBatch(List.of(original));
      assertThrows(Exception.class, () -> service.saveBatch(List.of(employee("must-rollback"), original)));
      assertNull(mongo.findById("must-rollback", Employee.class));
      assertEquals(1, mongo.getCollection("employees").countDocuments());
      assertEquals(1, mongo.getCollection("outbox").countDocuments());
    } finally { mongo.getDb().drop(); factory.destroy(); }
  }
  private Employee employee(String id) {
    return new Employee(id, "Test", "Employee", "test@example.test", "Engineering", "Engineer", "Active", null, List.of(), List.of(), null);
  }
}
