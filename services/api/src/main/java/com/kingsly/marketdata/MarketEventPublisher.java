package com.kingsly.marketdata;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

@Component
public class MarketEventPublisher {
    private final KafkaTemplate<String, MarketEventRequest> kafkaTemplate;
    private final String quoteTopic;
    private final String tradeTopic;
    private final long timeoutSeconds;

    public MarketEventPublisher(
            KafkaTemplate<String, MarketEventRequest> kafkaTemplate,
            @Value("${marketflow.kafka.quote-topic}") String quoteTopic,
            @Value("${marketflow.kafka.trade-topic}") String tradeTopic,
            @Value("${marketflow.kafka.publish-timeout-seconds:5}") long timeoutSeconds) {
        this.kafkaTemplate = kafkaTemplate;
        this.quoteTopic = quoteTopic;
        this.tradeTopic = tradeTopic;
        this.timeoutSeconds = timeoutSeconds;
    }

    public PublishedEvent publish(MarketEventRequest event) {
        String topic = "TRADE".equals(event.eventType()) ? tradeTopic : quoteTopic;
        try {
            SendResult<String, MarketEventRequest> result = kafkaTemplate
                    .send(topic, event.symbol().strip().toUpperCase(), event)
                    .get(timeoutSeconds, TimeUnit.SECONDS);
            RecordMetadata metadata = result.getRecordMetadata();
            return new PublishedEvent(metadata.topic(), metadata.partition(), metadata.offset());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Interrupted while publishing event to Kafka", ex);
        } catch (ExecutionException | TimeoutException ex) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Kafka did not acknowledge the event", ex);
        }
    }

    public record PublishedEvent(String topic, int partition, long offset) {}
}
