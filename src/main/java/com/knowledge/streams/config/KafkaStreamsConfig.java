package com.knowledge.streams.config;

import com.knowledge.streams.topology.NoteRoutingTopology;
import com.knowledge.streams.topology.DomainStatsTopology;
import com.knowledge.streams.topology.NoteEnrichmentTopology;
import com.knowledge.streams.topology.ActivityWindowTopology;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

/**
 * KAFKA CONCEPT — STREAMS CONFIGURATION:
 * Kafka Streams is a client library (not a separate cluster). Your app IS the
 * stream processor. Spring Boot's @EnableKafkaStreams wires up the StreamsBuilder
 * and manages the lifecycle (start on boot, close on shutdown).
 *
 * The StreamsBuilder is where you define your processing topology — the DAG
 * (directed acyclic graph) of stream operations. Think of it as a pipeline
 * blueprint: you declare what transformations to apply, and Kafka Streams
 * executes them continuously as new data arrives.
 *
 * IMPORTANT: The topology is built once at startup. It's a declaration, not
 * imperative code. Kafka Streams then runs this topology in a loop:
 *   poll -> process -> commit -> repeat
 */
@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    private final NoteRoutingTopology noteRoutingTopology;
    private final DomainStatsTopology domainStatsTopology;
    private final NoteEnrichmentTopology noteEnrichmentTopology;
    private final ActivityWindowTopology activityWindowTopology;

    public KafkaStreamsConfig(NoteRoutingTopology noteRoutingTopology,
                             DomainStatsTopology domainStatsTopology,
                             NoteEnrichmentTopology noteEnrichmentTopology,
                             ActivityWindowTopology activityWindowTopology) {
        this.noteRoutingTopology = noteRoutingTopology;
        this.domainStatsTopology = domainStatsTopology;
        this.noteEnrichmentTopology = noteEnrichmentTopology;
        this.activityWindowTopology = activityWindowTopology;
    }

    @Bean
    public Topology buildTopology(StreamsBuilder builder) {
        // Each topology method adds its processing nodes to the shared builder.
        // They all run within the same Kafka Streams application but operate
        // on different parts of the data flow.
        noteRoutingTopology.buildPipeline(builder);
        domainStatsTopology.buildPipeline(builder);
        noteEnrichmentTopology.buildPipeline(builder);
        activityWindowTopology.buildPipeline(builder);

        Topology topology = builder.build();
        // Print the topology for debugging — shows the DAG of processors
        System.out.println("=== KAFKA STREAMS TOPOLOGY ===");
        System.out.println(topology.describe());
        System.out.println("==============================");
        return topology;
    }
}
