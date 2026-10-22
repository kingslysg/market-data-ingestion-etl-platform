package com.kingsly.marketdata;

import jakarta.validation.Valid;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/market")
public class MarketIngestionController {
    private final MarketIngestionService ingestionService;

    public MarketIngestionController(MarketIngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @PostMapping("/ingest")
    public ResponseEntity<Map<String, Object>> ingest(@Valid @RequestBody MarketEventRequest event) {
        MarketEventPublisher.PublishedEvent published = ingestionService.ingest(event);
        return ResponseEntity.accepted().body(Map.of(
                "status", "ACCEPTED",
                "eventId", event.eventId(),
                "idempotencyKey", event.idempotencyKey(),
                "topic", published.topic(),
                "partition", published.partition(),
                "offset", published.offset()));
    }
}
