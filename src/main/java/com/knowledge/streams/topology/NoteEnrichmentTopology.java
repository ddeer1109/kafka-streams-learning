package com.knowledge.streams.topology;

import com.knowledge.streams.model.*;
import com.knowledge.streams.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * TOPOLOGY 3: JOINS — Combining Data from Multiple Sources
 * =========================================================
 *
 * This topology demonstrates KStream-KTable joins:
 *
 * JOIN TYPES IN KAFKA STREAMS:
 *
 * 1. KStream-KTable JOIN (used here):
 *    - For each stream record, look up the latest value in the table
 *    - Like a LEFT JOIN in SQL, but continuous
 *    - The stream side drives the join (table updates don't trigger output)
 *    - No windowing needed — the table always has the "current" state
 *    - USE CASE: Enriching events with reference/lookup data
 *
 * 2. KStream-KStream JOIN (not shown):
 *    - Joins two event streams within a time window
 *    - Both sides can trigger output
 *    - Requires a JoinWindow (e.g., "match events within 5 minutes")
 *    - USE CASE: Correlating events from two different sources
 *
 * 3. KTable-KTable JOIN:
 *    - Joins two tables (changelog streams)
 *    - Either side can trigger output (when either table updates)
 *    - No windowing — always uses latest values
 *    - USE CASE: Combining two reference datasets
 *
 * KEY REQUIREMENT FOR JOINS:
 *    Both sides must be co-partitioned — same key, same number of partitions.
 *    If not, you need to repartition one side first (via selectKey + through/repartition).
 *
 * Data flow:
 *   notes.inbound ──┐
 *                    ├── [join] ──→ notes.enriched
 *   domain.stats  ──┘
 *   (KStream)      (KTable)       (enriched output)
 */
@Component
public class NoteEnrichmentTopology {

    private static final Logger log = LoggerFactory.getLogger(NoteEnrichmentTopology.class);

    @Value("${knowledge.topics.notes-inbound}")
    private String notesInbound;

    @Value("${knowledge.topics.domain-stats}")
    private String domainStats;

    @Value("${knowledge.topics.notes-enriched}")
    private String notesEnriched;

    public void buildPipeline(StreamsBuilder builder) {
        JsonSerde<NoteEvent> noteEventSerde = new JsonSerde<>(NoteEvent.class);
        JsonSerde<DomainStats> statsSerde = new JsonSerde<>(DomainStats.class);
        JsonSerde<EnrichedNote> enrichedSerde = new JsonSerde<>(EnrichedNote.class);

        // Read the domain stats as a KTable — this is a continuously-updated
        // lookup table of the latest stats per domain.
        KTable<String, DomainStats> statsTable = builder.table(
                domainStats,
                Consumed.with(Serdes.String(), statsSerde)
                        .withName("source-enrichment-stats")
        );

        // Read inbound note events as a KStream
        KStream<String, NoteEvent> events = builder.stream(
                notesInbound,
                Consumed.with(Serdes.String(), noteEventSerde)
                        .withName("source-enrichment-inbound")
        );

        // Re-key the events stream by domain (to match the stats table key)
        // This is necessary because the inbound stream is keyed by note ID,
        // but the stats table is keyed by domain name.
        KStream<String, NoteEvent> eventsByDomain = events
                .filter((key, event) -> event.getNote() != null
                        && event.getNote().getDomain() != null,
                        Named.as("filter-enrichment-valid"))
                .selectKey(
                        (key, event) -> event.getNote().getDomain().toLowerCase(),
                        Named.as("rekey-by-domain")
                );

        // LEFT JOIN: For each note event, look up the domain stats.
        // leftJoin means if there are no stats yet for a domain, the note
        // still passes through (stats will be null). A regular join() would
        // drop the note if no stats exist.
        KStream<String, EnrichedNote> enriched = eventsByDomain
                .leftJoin(
                        statsTable,
                        // ValueJoiner — defines how to combine the stream value + table value
                        (noteEvent, stats) -> {
                            Note note = noteEvent.getNote();
                            List<String> relatedDomains = inferRelatedDomains(note);

                            return EnrichedNote.builder()
                                    .note(note)
                                    .domain(note.getDomain())
                                    .domainNoteCount(stats != null ? stats.getTotalNotes() : 0)
                                    .domainLastActivity(stats != null ? stats.getLastActivity() : null)
                                    .relatedDomains(relatedDomains)
                                    .enrichedAt(Instant.now())
                                    .build();
                        },
                        Joined.with(Serdes.String(), noteEventSerde, statsSerde,
                                "join-notes-with-stats")
                );

        enriched
                .peek((domain, note) -> log.info("Enriched note: domain={}, noteCount={}, related={}",
                        domain, note.getDomainNoteCount(), note.getRelatedDomains()))
                .to(notesEnriched, Produced.with(Serdes.String(), enrichedSerde));

        log.info("Note enrichment topology built: {} + {} -> {}", notesInbound, domainStats, notesEnriched);
    }

    // Simple heuristic: infer related domains from note tags and content
    private List<String> inferRelatedDomains(Note note) {
        List<String> related = new ArrayList<>();
        if (note.getTags() == null) return related;

        for (String tag : note.getTags()) {
            String t = tag.toLowerCase();
            if (t.contains("docker") || t.contains("k8s") || t.contains("ci")) {
                addIfNotPresent(related, "devops", note.getDomain());
            }
            if (t.contains("java") || t.contains("python") || t.contains("react")) {
                addIfNotPresent(related, "fullstack", note.getDomain());
            }
            if (t.contains("daw") || t.contains("eq") || t.contains("synth")) {
                addIfNotPresent(related, "music", note.getDomain());
            }
            if (t.contains("llm") || t.contains("prompt") || t.contains("model")) {
                addIfNotPresent(related, "ai-context", note.getDomain());
            }
        }
        return related;
    }

    private void addIfNotPresent(List<String> list, String domain, String currentDomain) {
        if (!domain.equalsIgnoreCase(currentDomain) && !list.contains(domain)) {
            list.add(domain);
        }
    }
}
