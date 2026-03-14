package com.knowledge.streams.topology;

import com.knowledge.streams.model.NoteEvent;
import com.knowledge.streams.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * TOPOLOGY 1: ROUTING — Stateless Operations (filter, map, branch)
 * ================================================================
 *
 * This topology demonstrates the most fundamental Kafka Streams operations:
 *
 * 1. CONSUMING FROM A TOPIC (source):
 *    builder.stream("topic") creates a KStream — an unbounded, continuously
 *    updating stream of records. Each record has a key and value.
 *
 * 2. FILTER:
 *    Drops records that don't match a predicate. The filtered-out records
 *    are simply not forwarded downstream. No state is needed.
 *
 * 3. MAPVALUES:
 *    Transforms each record's value without changing the key. This is
 *    preferred over map() when you don't need to change the key, because
 *    it avoids repartitioning (an expensive network operation).
 *
 * 4. BRANCH (split):
 *    Routes records to different downstream processors based on predicates.
 *    Like a railroad switch — each record goes down exactly one branch.
 *    Records that match no branch go to a default branch.
 *
 * 5. PRODUCING TO A TOPIC (sink):
 *    .to("topic") writes the processed records to an output topic.
 *    Other topologies or consumers can then read from these output topics.
 *
 * Data flow:
 *   notes.inbound → [filter deleted] → [branch by domain] → notes.{domain}
 *                                                         → notes.dlq (errors)
 */
@Component
public class NoteRoutingTopology {

    private static final Logger log = LoggerFactory.getLogger(NoteRoutingTopology.class);

    @Value("${knowledge.topics.notes-inbound}")
    private String notesInbound;

    @Value("${knowledge.topics.notes-devops}")
    private String notesDevops;

    @Value("${knowledge.topics.notes-fullstack}")
    private String notesFullstack;

    @Value("${knowledge.topics.notes-music}")
    private String notesMusic;

    @Value("${knowledge.topics.notes-ai-context}")
    private String notesAiContext;

    @Value("${knowledge.topics.notes-general}")
    private String notesGeneral;

    @Value("${knowledge.topics.notes-dlq}")
    private String notesDlq;

    public void buildPipeline(StreamsBuilder builder) {
        JsonSerde<NoteEvent> noteEventSerde = new JsonSerde<>(NoteEvent.class);

        // STEP 1: Create a KStream from the inbound topic
        // Every message published to notes.inbound will flow through this stream.
        KStream<String, NoteEvent> inboundStream = builder.stream(
                notesInbound,
                Consumed.with(Serdes.String(), noteEventSerde)
                        .withName("source-notes-inbound")
        );

        // STEP 2: Filter — drop events with null notes (malformed data)
        // peek() is a side-effect operation for logging/monitoring.
        // It does NOT modify the stream, just observes each record.
        KStream<String, NoteEvent> validEvents = inboundStream
                .peek((key, event) -> log.info("Received event: key={}, type={}, domain={}",
                        key,
                        event.getEventType(),
                        event.getNote() != null ? event.getNote().getDomain() : "null"))
                .filter((key, event) -> {
                    if (event.getNote() == null) {
                        log.warn("Dropping event with null note, key={}", key);
                        return false;
                    }
                    return true;
                }, Named.as("filter-null-notes"));

        // STEP 3: Branch/Split — route to domain-specific topics
        // split() replaced the older branch() API in Kafka 3.x.
        // Each .branch() adds a named predicate. Records are tested in order;
        // first match wins. .defaultBranch() catches everything else.
        Map<String, KStream<String, NoteEvent>> branches = validEvents
                .split(Named.as("route-"))
                .branch((key, event) -> "devops".equalsIgnoreCase(event.getNote().getDomain()),
                        Branched.as("devops"))
                .branch((key, event) -> "fullstack".equalsIgnoreCase(event.getNote().getDomain()),
                        Branched.as("fullstack"))
                .branch((key, event) -> "music".equalsIgnoreCase(event.getNote().getDomain()),
                        Branched.as("music"))
                .branch((key, event) -> "ai-context".equalsIgnoreCase(event.getNote().getDomain()),
                        Branched.as("ai-context"))
                .defaultBranch(Branched.as("general"));

        // STEP 4: Send each branch to its domain-specific topic
        // The Produced.with() specifies which serdes to use for the output.
        Produced<String, NoteEvent> produced = Produced.with(Serdes.String(), noteEventSerde);

        branches.get("route-devops").to(notesDevops, produced);
        branches.get("route-fullstack").to(notesFullstack, produced);
        branches.get("route-music").to(notesMusic, produced);
        branches.get("route-ai-context").to(notesAiContext, produced);
        branches.get("route-general").to(notesGeneral, produced);

        log.info("Note routing topology built: {} -> domain-specific topics", notesInbound);
    }
}
