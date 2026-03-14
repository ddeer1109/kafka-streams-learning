package com.knowledge.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Wraps a Note with event metadata — what happened and when.
 *
 * KAFKA CONCEPT: Events vs. state. A NoteEvent represents something that
 * happened (an event in a KStream). The Note inside it represents the
 * current state (what you'd materialize into a KTable).
 *
 * This distinction is fundamental:
 *   KStream  = unbounded sequence of events (NoteEvent)
 *   KTable   = latest value per key (Note)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class NoteEvent {

    public enum EventType {
        CREATED,    // New note added
        UPDATED,    // Existing note modified
        TAGGED,     // Tags changed
        DELETED     // Note removed
    }

    private EventType eventType;
    private Note note;
    private Instant timestamp;
    private String source;  // "pcloud", "api", "test-generator"
}
