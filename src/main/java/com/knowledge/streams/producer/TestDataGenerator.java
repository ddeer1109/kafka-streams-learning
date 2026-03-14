package com.knowledge.streams.producer;

import com.knowledge.streams.model.Note;
import com.knowledge.streams.model.NoteEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Generates sample note events for testing. Activate with:
 *   --spring.profiles.active=generate-test-data
 *
 * This populates the inbound topic with realistic notes across all domains,
 * so you can observe how the topologies process them.
 */
@Configuration
@Profile("generate-test-data")
public class TestDataGenerator {

    private static final Logger log = LoggerFactory.getLogger(TestDataGenerator.class);

    @Bean
    public CommandLineRunner generateTestData(NoteProducer producer) {
        return args -> {
            log.info("Generating test data...");

            List<Note> testNotes = List.of(
                    // DevOps notes
                    Note.builder()
                            .id(UUID.randomUUID().toString())
                            .title("Docker Multi-Stage Builds")
                            .content("Use multi-stage builds to reduce image size. First stage compiles, second stage copies only the binary.")
                            .domain("devops")
                            .tags(List.of("docker", "optimization", "ci"))
                            .sourcePath("/devops/docker-tips.md")
                            .createdAt(Instant.now())
                            .updatedAt(Instant.now())
                            .build(),

                    Note.builder()
                            .id(UUID.randomUUID().toString())
                            .title("Kubernetes Pod Lifecycle")
                            .content("Pods go through: Pending -> Running -> Succeeded/Failed. Use readiness probes to control traffic routing.")
                            .domain("devops")
                            .tags(List.of("k8s", "pods", "lifecycle"))
                            .sourcePath("/devops/k8s-notes.md")
                            .createdAt(Instant.now())
                            .updatedAt(Instant.now())
                            .build(),

                    Note.builder()
                            .id(UUID.randomUUID().toString())
                            .title("Terraform State Management")
                            .content("Always use remote state (S3 + DynamoDB). Never commit .tfstate files. Use workspaces for environment separation.")
                            .domain("devops")
                            .tags(List.of("terraform", "iac", "aws"))
                            .sourcePath("/devops/terraform.md")
                            .createdAt(Instant.now())
                            .updatedAt(Instant.now())
                            .build(),

                    // Fullstack notes
                    Note.builder()
                            .id(UUID.randomUUID().toString())
                            .title("Spring Boot Exception Handling")
                            .content("Use @ControllerAdvice with @ExceptionHandler for global error handling. Return ProblemDetail for RFC 7807 compliance.")
                            .domain("fullstack")
                            .tags(List.of("java", "spring", "error-handling"))
                            .sourcePath("/fullstack/spring-patterns.md")
                            .createdAt(Instant.now())
                            .updatedAt(Instant.now())
                            .build(),

                    Note.builder()
                            .id(UUID.randomUUID().toString())
                            .title("Python Virtual Environments")
                            .content("Use venv for project isolation. poetry or pipenv for dependency management. Always pin versions in requirements.txt.")
                            .domain("fullstack")
                            .tags(List.of("python", "venv", "dependencies"))
                            .sourcePath("/fullstack/python-setup.md")
                            .createdAt(Instant.now())
                            .updatedAt(Instant.now())
                            .build(),

                    Note.builder()
                            .id(UUID.randomUUID().toString())
                            .title("React useEffect Cleanup")
                            .content("Always return a cleanup function from useEffect when setting up subscriptions, timers, or event listeners to prevent memory leaks.")
                            .domain("fullstack")
                            .tags(List.of("react", "hooks", "javascript"))
                            .sourcePath("/fullstack/react-patterns.md")
                            .createdAt(Instant.now())
                            .updatedAt(Instant.now())
                            .build(),

                    // Music production notes
                    Note.builder()
                            .id(UUID.randomUUID().toString())
                            .title("EQ Before Compression")
                            .content("High-pass filter at 30-40Hz on non-bass tracks before compression. This prevents the compressor from reacting to sub-bass rumble.")
                            .domain("music")
                            .tags(List.of("eq", "compression", "mixing"))
                            .sourcePath("/music/mixing-chains.md")
                            .createdAt(Instant.now())
                            .updatedAt(Instant.now())
                            .build(),

                    Note.builder()
                            .id(UUID.randomUUID().toString())
                            .title("Sidechain Compression for Pumping Effect")
                            .content("Route kick to sidechain input of pad/bass compressor. Fast attack, medium release. Threshold to taste for genre-appropriate pumping.")
                            .domain("music")
                            .tags(List.of("synth", "sidechain", "edm"))
                            .sourcePath("/music/production-techniques.md")
                            .createdAt(Instant.now())
                            .updatedAt(Instant.now())
                            .build(),

                    // AI context notes
                    Note.builder()
                            .id(UUID.randomUUID().toString())
                            .title("Prompt Engineering: Chain of Thought")
                            .content("Ask the model to think step-by-step. Include 'Let's think through this' or structured reasoning prompts. Improves accuracy on complex tasks.")
                            .domain("ai-context")
                            .tags(List.of("prompt", "llm", "technique"))
                            .sourcePath("/ai/prompt-patterns.md")
                            .createdAt(Instant.now())
                            .updatedAt(Instant.now())
                            .build(),

                    Note.builder()
                            .id(UUID.randomUUID().toString())
                            .title("RAG Architecture Overview")
                            .content("Retrieval Augmented Generation: embed documents -> vector store -> retrieve relevant chunks -> inject into prompt context -> generate. Use chunking strategy appropriate for your domain.")
                            .domain("ai-context")
                            .tags(List.of("llm", "rag", "architecture", "model"))
                            .sourcePath("/ai/rag-notes.md")
                            .createdAt(Instant.now())
                            .updatedAt(Instant.now())
                            .build(),

                    // Cross-domain note (will be routed to general)
                    Note.builder()
                            .id(UUID.randomUUID().toString())
                            .title("Learning Strategy: Spaced Repetition")
                            .content("Review material at increasing intervals: 1 day, 3 days, 7 days, 14 days, 30 days. Combine with active recall (testing yourself) for best retention.")
                            .domain("general")
                            .tags(List.of("learning", "productivity"))
                            .sourcePath("/general/learning-methods.md")
                            .createdAt(Instant.now())
                            .updatedAt(Instant.now())
                            .build()
            );

            for (Note note : testNotes) {
                producer.publishNote(note, NoteEvent.EventType.CREATED, "test-generator");
                // Small delay so you can observe events flowing through the topologies
                Thread.sleep(500);
            }

            log.info("Test data generation complete: {} notes published", testNotes.size());
        };
    }
}
