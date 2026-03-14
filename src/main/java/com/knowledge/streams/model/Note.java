package com.knowledge.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

/**
 * Core domain object representing a knowledge note.
 *
 * KAFKA CONCEPT: This is what flows through your streams. In Kafka terms,
 * a Note serialized to JSON becomes the "value" part of a Kafka record.
 * The note's id becomes the "key" — this is critical because Kafka uses
 * keys for partitioning (same key = same partition = ordering guarantee).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Note {

    private String id;
    private String title;
    private String content;

    // Which knowledge domain: devops, fullstack, music, ai-context, general
    private String domain;

    // Freeform tags for filtering and enrichment
    private List<String> tags;

    // Source file path (e.g. from pCloud directory)
    private String sourcePath;

    private Instant createdAt;
    private Instant updatedAt;
}
