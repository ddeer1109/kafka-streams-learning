package com.knowledge.streams.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * KAFKA CONCEPT — TOPICS:
 * Topics are the fundamental unit of organization in Kafka. Think of them
 * as named log files that multiple producers can write to and multiple
 * consumers can read from.
 *
 * Key properties:
 *   - Partitions: How many parallel "lanes" a topic has. More partitions =
 *     more parallelism, but also more resource usage. For learning, 1-3 is fine.
 *   - Replication factor: How many copies of each partition exist across
 *     brokers. We use 1 here (single broker). Production typically uses 3.
 *   - Retention: How long messages are kept. Default is 7 days.
 *
 * Spring auto-creates these topics on startup via the AdminClient.
 */
@Configuration
public class TopicConfig {

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

    @Value("${knowledge.topics.notes-enriched}")
    private String notesEnriched;

    @Value("${knowledge.topics.domain-stats}")
    private String domainStats;

    @Value("${knowledge.topics.notes-dlq}")
    private String notesDlq;

    @Bean
    public NewTopic notesInboundTopic() {
        return TopicBuilder.name(notesInbound)
                .partitions(3)   // 3 partitions for some parallelism
                .replicas(1)     // Single broker setup
                .build();
    }

    @Bean
    public NewTopic notesDevopsTopic() {
        return TopicBuilder.name(notesDevops).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic notesFullstackTopic() {
        return TopicBuilder.name(notesFullstack).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic notesMusicTopic() {
        return TopicBuilder.name(notesMusic).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic notesAiContextTopic() {
        return TopicBuilder.name(notesAiContext).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic notesGeneralTopic() {
        return TopicBuilder.name(notesGeneral).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic notesEnrichedTopic() {
        return TopicBuilder.name(notesEnriched).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic domainStatsTopic() {
        return TopicBuilder.name(domainStats).partitions(1).replicas(1)
                // Compact this topic: keep only the latest value per key (domain name)
                .config("cleanup.policy", "compact")
                .build();
    }

    @Bean
    public NewTopic notesDlqTopic() {
        return TopicBuilder.name(notesDlq).partitions(1).replicas(1).build();
    }
}
