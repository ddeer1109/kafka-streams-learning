package com.knowledge.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

/**
 * A note enriched with cross-domain context.
 *
 * KAFKA CONCEPT: This is the result of a KStream-KTable join. The incoming
 * note event (stream) is joined with the latest domain stats (table) to
 * produce a richer output. Joins are one of the most powerful Kafka Streams
 * operations — they let you combine data from different topics without
 * a traditional database.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class EnrichedNote {

    private Note note;
    private String domain;
    private long domainNoteCount;
    private Instant domainLastActivity;
    private List<String> relatedDomains;
    private Instant enrichedAt;
}
