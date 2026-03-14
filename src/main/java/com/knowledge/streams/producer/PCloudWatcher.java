package com.knowledge.streams.producer;

import com.knowledge.streams.model.Note;
import com.knowledge.streams.model.NoteEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * PCLOUD INTEGRATION:
 * Watches a pCloud directory for new/modified files and publishes them as
 * NoteEvents to Kafka. This is the bridge between your file-based knowledge
 * and the streaming pipeline.
 *
 * Activate with: knowledge.pcloud.enabled=true in application.yml
 *
 * Expected directory structure on pCloud:
 *   KnowledgeHub/
 *     devops/         → notes.devops
 *     fullstack/      → notes.fullstack
 *     music/          → notes.music
 *     ai-context/     → notes.ai-context
 *     general/        → notes.general (or any other subdirectory)
 *
 * Place .txt or .md files in these directories. The watcher will detect
 * new and modified files, parse them into Notes, and stream them into Kafka.
 */
@Service
@EnableScheduling
@ConditionalOnProperty(name = "knowledge.pcloud.enabled", havingValue = "true")
public class PCloudWatcher {

    private static final Logger log = LoggerFactory.getLogger(PCloudWatcher.class);

    private static final Set<String> KNOWN_DOMAINS = Set.of(
            "devops", "fullstack", "music", "ai-context", "general"
    );

    private final NoteProducer producer;
    private final Path basePath;

    // Track file modification times to detect changes
    private final Map<Path, Long> knownFiles = new ConcurrentHashMap<>();

    public PCloudWatcher(NoteProducer producer,
                         @Value("${knowledge.pcloud.base-path}") String basePath) {
        this.producer = producer;
        this.basePath = Paths.get(basePath);
        log.info("pCloud watcher initialized, watching: {}", this.basePath);
    }

    @Scheduled(fixedDelayString = "${knowledge.pcloud.watch-interval-ms:5000}")
    public void scanForChanges() {
        if (!Files.exists(basePath)) {
            log.warn("pCloud base path does not exist: {}. Skipping scan.", basePath);
            return;
        }

        try (Stream<Path> paths = Files.walk(basePath, 2)) {
            paths
                    .filter(Files::isRegularFile)
                    // Skip .output directory to avoid re-ingesting our own output
                    .filter(p -> !p.toString().contains("/.output/"))
                    .filter(p -> {
                        String name = p.getFileName().toString().toLowerCase();
                        return name.endsWith(".md") || name.endsWith(".txt");
                    })
                    .forEach(this::processFile);
        } catch (IOException e) {
            log.error("Error scanning pCloud directory: {}", e.getMessage());
        }
    }

    private void processFile(Path filePath) {
        try {
            long lastModified = Files.getLastModifiedTime(filePath).toMillis();
            Long previousModified = knownFiles.get(filePath);

            if (previousModified != null && previousModified == lastModified) {
                return; // File unchanged
            }

            boolean isNew = previousModified == null;
            knownFiles.put(filePath, lastModified);

            String content = Files.readString(filePath);
            String domain = inferDomain(filePath);
            String title = extractTitle(filePath, content);

            Note note = Note.builder()
                    .id(generateStableId(filePath))
                    .title(title)
                    .content(content)
                    .domain(domain)
                    .tags(extractTags(content))
                    .sourcePath(filePath.toString())
                    .createdAt(isNew ? Instant.now() : Instant.ofEpochMilli(lastModified))
                    .updatedAt(Instant.now())
                    .build();

            NoteEvent.EventType eventType = isNew ? NoteEvent.EventType.CREATED : NoteEvent.EventType.UPDATED;
            producer.publishNote(note, eventType, "pcloud");

            log.info("Published {} note from pCloud: {} [{}]",
                    isNew ? "new" : "updated", title, domain);

        } catch (IOException e) {
            log.error("Error processing file {}: {}", filePath, e.getMessage());
        }
    }

    private String inferDomain(Path filePath) {
        // The parent directory name maps to the domain
        Path relative = basePath.relativize(filePath);
        if (relative.getNameCount() > 1) {
            String dir = relative.getName(0).toString().toLowerCase();
            if (KNOWN_DOMAINS.contains(dir)) {
                return dir;
            }
        }
        return "general";
    }

    private String extractTitle(Path filePath, String content) {
        // Try to extract a markdown heading, fall back to filename
        for (String line : content.split("\n")) {
            String trimmed = line.trim();
            if (trimmed.startsWith("# ")) {
                return trimmed.substring(2).trim();
            }
        }
        String filename = filePath.getFileName().toString();
        return filename.replaceAll("\\.(md|txt)$", "").replace("-", " ").replace("_", " ");
    }

    private List<String> extractTags(String content) {
        // Look for a tags line like: tags: docker, k8s, ci
        List<String> tags = new ArrayList<>();
        for (String line : content.split("\n")) {
            String trimmed = line.trim().toLowerCase();
            if (trimmed.startsWith("tags:")) {
                String tagStr = trimmed.substring(5).trim();
                for (String tag : tagStr.split("[,;]")) {
                    String t = tag.trim();
                    if (!t.isEmpty()) tags.add(t);
                }
                break;
            }
        }
        return tags;
    }

    private String generateStableId(Path filePath) {
        // Stable ID based on file path, so the same file always gets the same ID
        return UUID.nameUUIDFromBytes(filePath.toString().getBytes()).toString();
    }
}
