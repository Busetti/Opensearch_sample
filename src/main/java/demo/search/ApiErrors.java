package demo.search;

import java.util.Map;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
class ApiErrors {
  @ExceptionHandler(IllegalArgumentException.class)
  ResponseEntity<?> invalid(IllegalArgumentException e) { return ResponseEntity.badRequest().body(Map.of("error", e.getMessage())); }
  @ExceptionHandler(DuplicateKeyException.class)
  ResponseEntity<?> duplicate(DuplicateKeyException e) { return ResponseEntity.status(409).body(Map.of("error", "An employee with that ID already exists")); }
}
