package com.knowledge.streams.topology;

import com.knowledge.streams.model.DomainStats;
import com.knowledge.streams.model.NoteEvent;
import com.knowledge.streams.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * TOPOLOGY 2: AGGREGATION — Stateful Operations
 * ===============================================
 *
 * This topology demonstrates stateful stream processing:
 *
 * 1. GROUPBY:
 *    Re-keys the stream by domain name. This triggers a REPARTITION — Kafka
 *    Streams creates an internal topic to shuffle data so that all records
 *    with the same key end up on the same partition. This is required for
 *    correct aggregation (you can't aggregate across partitions).
 *
 *    IMPORTANT: Repartitioning is expensive (network I/O). Use groupByKey()
 *    when the key is already what you want, and groupBy() only when you need
 *    to change the key.
 *
 * 2. AGGREGATE:
 *    Maintains a running aggregate per key. Unlike reduce() which combines
 *    two values of the same type, aggregate() can produce a different type
 *    (here: NoteEvent → DomainStats).
 *
 *    The aggregation state is stored in a LOCAL STATE STORE — an embedded
 *    RocksDB instance. This gives you:
 *      - Fast reads (no network round-trip)
 *      - Fault tolerance (state is backed by a changelog topic)
 *      - Exactly-once semantics (when configured)
 *
 * 3. KTABLE:
 *    The result of an aggregation is a KTable — a changelog stream where
 *    each record represents the LATEST state for a key. If domain "devops"
 *    has 5 notes, the KTable has one entry: ("devops" → DomainStats{count=5}).
 *
 *    KTable vs KStream:
 *      KStream: INSERT semantics (every record is a new fact)
 *      KTable:  UPSERT semantics (new record replaces previous for same key)
 *
 * 4. toStream().to():
 *    Converts the KTable back to a KStream and writes it to an output topic.
 *    This lets other consumers see the latest stats.
 *
 * Data flow:
 *   notes.inbound → [groupBy domain] → [aggregate into DomainStats] → domain.stats
 *                                          ↓
 *                                   (state store: "domain-stats-store")
 */
@Component
public class DomainStatsTopology {

    private static final Logger log = LoggerFactory.getLogger(DomainStatsTopology.class);

    public static final String STATS_STORE = "domain-stats-store";

    @Value("${knowledge.topics.notes-inbound}")
    private String notesInbound;

    @Value("${knowledge.topics.domain-stats}")
    private String domainStats;

    public void buildPipeline(StreamsBuilder builder) {
        JsonSerde<NoteEvent> noteEventSerde = new JsonSerde<>(NoteEvent.class);
        JsonSerde<DomainStats> statsSerde = new JsonSerde<>(DomainStats.class);

        // Read from the same inbound topic (multiple topologies can read the
        // same topic — each gets its own consumer within the application)
        KStream<String, NoteEvent> events = builder.stream(
                notesInbound,
                Consumed.with(Serdes.String(), noteEventSerde)
                        .withName("source-stats-inbound")
        );

        // Filter out events without valid notes
        KStream<String, NoteEvent> validEvents = events
                .filter((key, event) -> event.getNote() != null
                        && event.getNote().getDomain() != null,
                        Named.as("filter-stats-valid"));

        // GroupBy domain — this changes the key from note ID to domain name.
        // All events for "devops" will be routed to the same partition via
        // an internal repartition topic.
        KGroupedStream<String, NoteEvent> groupedByDomain = validEvents
                .groupBy(
                        (key, event) -> event.getNote().getDomain().toLowerCase(),
                        Grouped.with("group-by-domain", Serdes.String(), noteEventSerde)
                );

        // Aggregate into DomainStats, maintaining state in a local store.
        // - Initializer: creates a new DomainStats when a domain is seen for the first time
        // - Aggregator: applies each event to the running stats
        // - Materialized: configures the state store (name, serdes, storage engine)
        KTable<String, DomainStats> statsTable = groupedByDomain
                .aggregate(
                        // Initializer — called once per new key
                        () -> DomainStats.empty(""),
                        // Aggregator — called for each event
                        (domain, event, currentStats) -> {
                            currentStats.setDomain(domain);
                            currentStats.applyEvent(event);
                            log.debug("Stats updated for domain={}: total={}", domain, currentStats.getTotalNotes());
                            return currentStats;
                        },
                        // State store configuration
                        Materialized.<String, DomainStats, KeyValueStore<Bytes, byte[]>>as(STATS_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(statsSerde)
                );

        // Write the stats table to an output topic so other consumers can use it
        statsTable
                .toStream(Named.as("stats-to-stream"))
                .peek((domain, stats) -> log.info("Domain stats: {}={} notes", domain, stats.getTotalNotes()))
                .to(domainStats, Produced.with(Serdes.String(), statsSerde));

        log.info("Domain stats topology built: {} -> aggregate -> {}", notesInbound, domainStats);
    }
}
