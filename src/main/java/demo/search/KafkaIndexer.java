package demo.search;

import java.util.List;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaIndexer {
  private final OpenSearchService search;
  public KafkaIndexer(OpenSearchService search) { this.search = search; }

  @KafkaListener(topics = "${app.topic}", groupId = "employee-indexer")
  public void index(List<String> documents) throws Exception { search.indexBatch(documents); }
}
