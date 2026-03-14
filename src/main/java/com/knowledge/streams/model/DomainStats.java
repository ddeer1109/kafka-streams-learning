package com.knowledge.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Aggregated statistics per knowledge domain.
 *
 * KAFKA CONCEPT: This is the output of a stateful aggregation. Kafka Streams
 * maintains a local state store to compute these running totals. The state
 * store is backed by a changelog topic for fault tolerance — if this instance
 * crashes, another can rebuild the state from the changelog.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class DomainStats {

    private String domain;
    private long totalNotes;
    private long createdCount;
    private long updatedCount;
    private long deletedCount;
    private Instant lastActivity;

    public static DomainStats empty(String domain) {
        return DomainStats.builder()
                .domain(domain)
                .totalNotes(0)
                .createdCount(0)
                .updatedCount(0)
                .deletedCount(0)
                .lastActivity(Instant.now())
                .build();
    }

    public DomainStats applyEvent(NoteEvent event) {
        this.lastActivity = event.getTimestamp();
        switch (event.getEventType()) {
            case CREATED -> {
                this.totalNotes++;
                this.createdCount++;
            }
            case UPDATED, TAGGED -> this.updatedCount++;
            case DELETED -> {
                this.totalNotes = Math.max(0, this.totalNotes - 1);
                this.deletedCount++;
            }
        }
        return this;
    }
}
