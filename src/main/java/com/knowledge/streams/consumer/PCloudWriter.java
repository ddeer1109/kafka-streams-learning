package com.knowledge.streams.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.knowledge.streams.model.DomainStats;
import com.knowledge.streams.model.EnrichedNote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * PCLOUD WRITE-BACK:
 * Consumes enriched notes and domain stats from Kafka, writes them
 * back to pCloud as files. This closes the loop:
 *
 *   pCloud files → Kafka → [process/enrich] → pCloud .output/
 *
 * Output structure:
 *   KnowledgeHub/.output/
 *     enriched/
 *       devops/
 *         docker-networking.json      (enriched note as JSON)
 *         docker-networking.md        (human-readable summary)
 *       fullstack/
 *         ...
 *     stats/
 *       devops.json                   (latest domain stats)
 *       fullstack.json
 *       _summary.md                   (all domains overview)
 *
 * Since pCloud syncs these files across all machines, the enriched
 * output is available everywhere without running Kafka on every machine.
 */
@Service
@ConditionalOnProperty(name = "knowledge.pcloud.enabled", havingValue = "true")
public class PCloudWriter {

    private static final Logger log = LoggerFactory.getLogger(PCloudWriter.class);
    private static final DateTimeFormatter TIME_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    private final Path outputBase;
    private final ObjectMapper mapper;

    public PCloudWriter(@Value("${knowledge.pcloud.base-path}") String basePath) {
        this.outputBase = Paths.get(basePath, ".output");
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new JavaTimeModule());
        this.mapper.enable(SerializationFeature.INDENT_OUTPUT);
        this.mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        log.info("pCloud writer initialized, output path: {}", outputBase);
    }

    @KafkaListener(
            topics = "${knowledge.topics.notes-enriched}",
            groupId = "pcloud-writer-enriched",
            properties = {
                    "spring.json.value.default.type=com.knowledge.streams.model.EnrichedNote"
            })
    public void writeEnrichedNote(EnrichedNote enriched) {
        String domain = enriched.getDomain() != null ? enriched.getDomain() : "general";
        String slug = slugify(enriched.getNote().getTitle());

        try {
            // Write JSON (machine-readable, full data)
            Path jsonDir = outputBase.resolve("enriched").resolve(domain);
            Files.createDirectories(jsonDir);
            Path jsonFile = jsonDir.resolve(slug + ".json");
            mapper.writeValue(jsonFile.toFile(), enriched);

            // Write Markdown (human-readable summary)
            Path mdFile = jsonDir.resolve(slug + ".md");
            Files.writeString(mdFile, toMarkdown(enriched));

            log.info("Wrote enriched note to pCloud: {}/{}", domain, slug);
        } catch (IOException e) {
            log.error("Failed to write enriched note {}/{}: {}", domain, slug, e.getMessage());
        }
    }

    @KafkaListener(
            topics = "${knowledge.topics.domain-stats}",
            groupId = "pcloud-writer-stats",
            properties = {
                    "spring.json.value.default.type=com.knowledge.streams.model.DomainStats"
            })
    public void writeDomainStats(DomainStats stats) {
        try {
            Path statsDir = outputBase.resolve("stats");
            Files.createDirectories(statsDir);

            // Write per-domain JSON
            Path statsFile = statsDir.resolve(stats.getDomain() + ".json");
            mapper.writeValue(statsFile.toFile(), stats);

            // Update summary markdown
            writeSummary(statsDir);

            log.info("Wrote stats to pCloud: {}", stats.getDomain());
        } catch (IOException e) {
            log.error("Failed to write stats for {}: {}", stats.getDomain(), e.getMessage());
        }
    }

    private void writeSummary(Path statsDir) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("# Knowledge Hub — Domain Stats\n\n");
        sb.append("_Updated: ").append(TIME_FMT.format(Instant.now())).append("_\n\n");
        sb.append("| Domain | Total Notes | Created | Updated | Deleted | Last Activity |\n");
        sb.append("|--------|------------|---------|---------|---------|---------------|\n");

        try (var files = Files.list(statsDir)) {
            files.filter(f -> f.toString().endsWith(".json"))
                    .sorted()
                    .forEach(f -> {
                        try {
                            DomainStats s = mapper.readValue(f.toFile(), DomainStats.class);
                            sb.append(String.format("| %s | %d | %d | %d | %d | %s |\n",
                                    s.getDomain(),
                                    s.getTotalNotes(),
                                    s.getCreatedCount(),
                                    s.getUpdatedCount(),
                                    s.getDeletedCount(),
                                    s.getLastActivity() != null ? TIME_FMT.format(s.getLastActivity()) : "—"));
                        } catch (IOException e) {
                            log.warn("Could not read stats file {}: {}", f, e.getMessage());
                        }
                    });
        }

        Files.writeString(statsDir.resolve("_summary.md"), sb.toString());
    }

    private String toMarkdown(EnrichedNote enriched) {
        var note = enriched.getNote();
        StringBuilder sb = new StringBuilder();

        sb.append("# ").append(note.getTitle()).append("\n\n");

        if (note.getTags() != null && !note.getTags().isEmpty()) {
            sb.append("**Tags:** ").append(String.join(", ", note.getTags())).append("\n");
        }
        sb.append("**Domain:** ").append(enriched.getDomain()).append("\n");
        sb.append("**Domain note count:** ").append(enriched.getDomainNoteCount()).append("\n");

        if (enriched.getRelatedDomains() != null && !enriched.getRelatedDomains().isEmpty()) {
            sb.append("**Related domains:** ").append(String.join(", ", enriched.getRelatedDomains())).append("\n");
        }

        sb.append("**Enriched at:** ").append(
                enriched.getEnrichedAt() != null ? TIME_FMT.format(enriched.getEnrichedAt()) : "—"
        ).append("\n");

        sb.append("\n---\n\n");
        sb.append(note.getContent()).append("\n");

        if (note.getSourcePath() != null) {
            sb.append("\n---\n_Source: ").append(note.getSourcePath()).append("_\n");
        }

        return sb.toString();
    }

    private String slugify(String title) {
        if (title == null || title.isBlank()) return "untitled";
        return title.toLowerCase()
                .replaceAll("[^a-z0-9\\s-]", "")
                .replaceAll("\\s+", "-")
                .replaceAll("-+", "-")
                .replaceAll("^-|-$", "");
    }
}
