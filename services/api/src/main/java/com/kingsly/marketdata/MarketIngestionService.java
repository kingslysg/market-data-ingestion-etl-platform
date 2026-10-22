package com.kingsly.marketdata;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class MarketIngestionService {
    private final MarketEventPublisher publisher;

    public MarketIngestionService(MarketEventPublisher publisher) {
        this.publisher = publisher;
    }

    public MarketEventPublisher.PublishedEvent ingest(MarketEventRequest event) {
        String expected = IdempotencyKeys.forEvent(event.producer(), event.eventId());
        if (!expected.equalsIgnoreCase(event.idempotencyKey())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "idempotencyKey must be the SHA-256 key for producer and eventId");
        }
        return publisher.publish(event);
    }
}
