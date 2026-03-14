package com.knowledge.streams.producer;

import com.knowledge.streams.model.Note;
import com.knowledge.streams.model.NoteEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

/**
 * KAFKA CONCEPT — PRODUCERS:
 * A producer sends records to Kafka topics. Key decisions:
 *
 * 1. KEY SELECTION:
 *    The key determines which partition a record goes to (via hashing).
 *    Same key = same partition = guaranteed ordering for that key.
 *    We use the note ID as key, so all events for one note are ordered.
 *
 * 2. ACKS (acknowledgments):
 *    - acks=0: Don't wait for broker confirmation (fast, but may lose data)
 *    - acks=1: Wait for leader broker to confirm (good balance)
 *    - acks=all: Wait for all replicas to confirm (safest, slower)
 *    Spring defaults to acks=1. Production systems often use acks=all.
 *
 * 3. IDEMPOTENT PRODUCER:
 *    Setting enable.idempotence=true prevents duplicate messages from retries.
 *    The broker deduplicates based on producer ID + sequence number.
 *
 * 4. BATCHING:
 *    Producers batch multiple records into a single network request.
 *    batch.size and linger.ms control this tradeoff between latency and throughput.
 */
@Service
public class NoteProducer {

    private static final Logger log = LoggerFactory.getLogger(NoteProducer.class);

    private final KafkaTemplate<String, NoteEvent> kafkaTemplate;

    @Value("${knowledge.topics.notes-inbound}")
    private String notesInbound;

    public NoteProducer(KafkaTemplate<String, NoteEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<SendResult<String, NoteEvent>> publishNote(Note note, NoteEvent.EventType eventType, String source) {
        NoteEvent event = NoteEvent.builder()
                .eventType(eventType)
                .note(note)
                .timestamp(Instant.now())
                .source(source)
                .build();

        // send(topic, key, value) — key is the note ID
        // The returned future completes when the broker acknowledges the record.
        CompletableFuture<SendResult<String, NoteEvent>> future =
                kafkaTemplate.send(notesInbound, note.getId(), event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish note {}: {}", note.getId(), ex.getMessage());
            } else {
                var metadata = result.getRecordMetadata();
                log.info("Published note: id={}, topic={}, partition={}, offset={}",
                        note.getId(),
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset());
            }
        });

        return future;
    }
}
