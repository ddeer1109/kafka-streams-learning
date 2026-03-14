package com.knowledge.streams.consumer;

import com.knowledge.streams.model.NoteEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * KAFKA CONCEPT — CONSUMERS & CONSUMER GROUPS:
 *
 * Consumers read records from topics. Key concepts:
 *
 * 1. CONSUMER GROUP:
 *    Multiple consumer instances with the same group.id share the work.
 *    Kafka assigns partitions to consumers in the group. If a topic has
 *    3 partitions and 3 consumers in the group, each gets 1 partition.
 *    If one consumer dies, its partition is reassigned (rebalancing).
 *
 * 2. OFFSET MANAGEMENT:
 *    Each consumer tracks its position (offset) in each partition.
 *    - auto.offset.reset=earliest: Start from the beginning (first time)
 *    - auto.offset.reset=latest: Start from new messages only
 *    Offsets are committed to a special __consumer_offsets topic.
 *
 * 3. AT-LEAST-ONCE vs EXACTLY-ONCE:
 *    Default is at-least-once: process, then commit offset. If the app
 *    crashes between processing and committing, the message is re-processed.
 *    For exactly-once, use Kafka transactions (more complex).
 *
 * 4. @KafkaListener:
 *    Spring's abstraction over the consumer API. It handles polling,
 *    offset commits, and deserialization. The topics attribute specifies
 *    which topics to consume from.
 *
 * These consumers listen to the domain-specific output topics to demonstrate
 * the results of the routing topology.
 */
@Service
public class ProcessedNoteConsumer {

    private static final Logger log = LoggerFactory.getLogger(ProcessedNoteConsumer.class);

    @KafkaListener(topics = "${knowledge.topics.notes-devops}",
            groupId = "domain-consumer-devops",
            properties = {
                    "spring.json.value.default.type=com.knowledge.streams.model.NoteEvent"
            })
    public void consumeDevops(NoteEvent event) {
        log.info("[DEVOPS] Received: '{}' ({})",
                event.getNote().getTitle(), event.getEventType());
    }

    @KafkaListener(topics = "${knowledge.topics.notes-fullstack}",
            groupId = "domain-consumer-fullstack",
            properties = {
                    "spring.json.value.default.type=com.knowledge.streams.model.NoteEvent"
            })
    public void consumeFullstack(NoteEvent event) {
        log.info("[FULLSTACK] Received: '{}' ({})",
                event.getNote().getTitle(), event.getEventType());
    }

    @KafkaListener(topics = "${knowledge.topics.notes-music}",
            groupId = "domain-consumer-music",
            properties = {
                    "spring.json.value.default.type=com.knowledge.streams.model.NoteEvent"
            })
    public void consumeMusic(NoteEvent event) {
        log.info("[MUSIC] Received: '{}' ({})",
                event.getNote().getTitle(), event.getEventType());
    }

    @KafkaListener(topics = "${knowledge.topics.notes-ai-context}",
            groupId = "domain-consumer-ai-context",
            properties = {
                    "spring.json.value.default.type=com.knowledge.streams.model.NoteEvent"
            })
    public void consumeAiContext(NoteEvent event) {
        log.info("[AI-CONTEXT] Received: '{}' ({})",
                event.getNote().getTitle(), event.getEventType());
    }

    @KafkaListener(topics = "${knowledge.topics.notes-general}",
            groupId = "domain-consumer-general",
            properties = {
                    "spring.json.value.default.type=com.knowledge.streams.model.NoteEvent"
            })
    public void consumeGeneral(NoteEvent event) {
        log.info("[GENERAL] Received: '{}' ({})",
                event.getNote().getTitle(), event.getEventType());
    }
}
