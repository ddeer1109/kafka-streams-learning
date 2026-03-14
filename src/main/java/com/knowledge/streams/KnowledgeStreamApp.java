package com.knowledge.streams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Knowledge Stream Hub — Kafka Streams Learning Application
 *
 * Quick start:
 *   1. docker-compose up -d         (start Kafka + ZooKeeper + Kafka UI)
 *   2. mvn spring-boot:run          (start this app)
 *   3. Open http://localhost:8090    (Kafka UI — inspect topics and messages)
 *
 * Generate test data:
 *   mvn spring-boot:run -Dspring-boot.run.profiles=generate-test-data
 *
 * Send a note via API:
 *   curl -X POST http://localhost:8080/api/notes \
 *     -H "Content-Type: application/json" \
 *     -d '{"title":"My Note","content":"Some content","domain":"devops","tags":["docker"]}'
 *
 * Query domain stats (Interactive Query):
 *   curl http://localhost:8080/api/stats
 *   curl http://localhost:8080/api/stats/devops
 *
 * pCloud integration:
 *   Set knowledge.pcloud.enabled=true and adjust base-path in application.yml
 */
@SpringBootApplication
public class KnowledgeStreamApp {

    public static void main(String[] args) {
        SpringApplication.run(KnowledgeStreamApp.class, args);
    }
}
