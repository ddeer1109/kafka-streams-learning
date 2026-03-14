package com.knowledge.streams.rest;

import com.knowledge.streams.model.DomainStats;
import com.knowledge.streams.model.Note;
import com.knowledge.streams.model.NoteEvent;
import com.knowledge.streams.producer.NoteProducer;
import com.knowledge.streams.topology.DomainStatsTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.*;

/**
 * REST API for interacting with the knowledge stream hub.
 *
 * KAFKA CONCEPT — INTERACTIVE QUERIES:
 * Kafka Streams state stores are not just internal — you can query them
 * directly via the Interactive Queries API. This turns your stream processor
 * into a queryable service, no external database needed.
 *
 * The GET /api/stats endpoint demonstrates this: it reads directly from
 * the domain-stats-store (a RocksDB instance maintained by the DomainStatsTopology).
 *
 * Endpoints:
 *   POST /api/notes       — Publish a new note
 *   GET  /api/stats        — Get all domain stats (interactive query)
 *   GET  /api/stats/{domain} — Get stats for one domain
 */
@RestController
@RequestMapping("/api")
public class NoteController {

    private static final Logger log = LoggerFactory.getLogger(NoteController.class);

    private final NoteProducer producer;
    private final StreamsBuilderFactoryBean factoryBean;

    public NoteController(NoteProducer producer, StreamsBuilderFactoryBean factoryBean) {
        this.producer = producer;
        this.factoryBean = factoryBean;
    }

    @PostMapping("/notes")
    public Map<String, String> createNote(@RequestBody Note note) {
        if (note.getId() == null) {
            note.setId(UUID.randomUUID().toString());
        }
        if (note.getCreatedAt() == null) {
            note.setCreatedAt(Instant.now());
        }
        note.setUpdatedAt(Instant.now());

        producer.publishNote(note, NoteEvent.EventType.CREATED, "api");

        return Map.of(
                "status", "accepted",
                "noteId", note.getId(),
                "message", "Note published to inbound topic. It will be routed by domain."
        );
    }

    @GetMapping("/stats")
    public List<DomainStats> getAllStats() {
        List<DomainStats> stats = new ArrayList<>();
        ReadOnlyKeyValueStore<String, DomainStats> store = getStatsStore();
        if (store == null) return stats;

        store.all().forEachRemaining(kv -> stats.add(kv.value));
        return stats;
    }

    @GetMapping("/stats/{domain}")
    public DomainStats getDomainStats(@PathVariable String domain) {
        ReadOnlyKeyValueStore<String, DomainStats> store = getStatsStore();
        if (store == null) return DomainStats.empty(domain);

        DomainStats stats = store.get(domain.toLowerCase());
        return stats != null ? stats : DomainStats.empty(domain);
    }

    private ReadOnlyKeyValueStore<String, DomainStats> getStatsStore() {
        KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
        if (kafkaStreams == null || kafkaStreams.state() != KafkaStreams.State.RUNNING) {
            log.warn("Kafka Streams not running, state store not available");
            return null;
        }
        return kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(
                        DomainStatsTopology.STATS_STORE,
                        QueryableStoreTypes.keyValueStore()
                )
        );
    }
}
