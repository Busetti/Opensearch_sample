package demo.search;

import java.util.List;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document("employees")
public record Employee(@Id String id, String firstName, String lastName, String email,
                       String department, String title, String status, Address address,
                       List<Skill> skills, List<Assignment> assignments, Manager manager) {
  public record Address(String city, String state, String country, String postalCode) {}
  public record Skill(String name, int level) {}
  public record Assignment(String project, String role, String region, boolean active) {}
  public record Manager(String id, String name) {}
}
