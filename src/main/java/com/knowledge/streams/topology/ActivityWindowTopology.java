package com.knowledge.streams.topology;

import com.knowledge.streams.model.NoteEvent;
import com.knowledge.streams.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * TOPOLOGY 4: WINDOWED OPERATIONS — Time-Based Aggregation
 * ==========================================================
 *
 * This topology demonstrates windowed stream processing:
 *
 * WINDOWING CONCEPTS:
 *
 * Windows let you group events by time periods. Without windows, an
 * aggregation runs forever (total count). With windows, you get counts
 * per time period (events in the last 5 minutes).
 *
 * WINDOW TYPES:
 *
 * 1. TUMBLING WINDOW (used here):
 *    Fixed-size, non-overlapping, gap-less windows.
 *    Example: [0:00-0:05), [0:05-0:10), [0:10-0:15) ...
 *    Each event belongs to exactly one window.
 *    USE CASE: "How many notes per 5-minute period?"
 *
 * 2. HOPPING WINDOW:
 *    Fixed-size, overlapping windows that advance by a hop interval.
 *    Example: size=10min, hop=5min → [0:00-0:10), [0:05-0:15), [0:10-0:20) ...
 *    Each event can belong to multiple windows.
 *    USE CASE: "Rolling average over 10 minutes, updated every 5 minutes"
 *
 * 3. SESSION WINDOW:
 *    Dynamically-sized windows based on activity gaps.
 *    If no event arrives within the gap, the window closes.
 *    USE CASE: "Group all notes from a single work session"
 *
 * 4. SLIDING WINDOW:
 *    Fixed-size window that starts/ends at each event timestamp.
 *    USE CASE: "Did more than 10 events happen within any 1-minute span?"
 *
 * GRACE PERIOD:
 *    How long to accept late-arriving events after a window closes.
 *    Events arriving after the grace period are dropped. This handles
 *    clock skew, network delays, etc.
 *
 * WINDOWED STATE STORE:
 *    The window state is stored locally, keyed by (key, window-start-time).
 *    Retention period controls how far back you can query.
 *
 * Data flow:
 *   notes.inbound → [groupBy domain] → [windowedBy 5min] → [count] → (log output)
 */
@Component
public class ActivityWindowTopology {

    private static final Logger log = LoggerFactory.getLogger(ActivityWindowTopology.class);

    public static final String WINDOW_STORE = "activity-window-store";

    @Value("${knowledge.topics.notes-inbound}")
    private String notesInbound;

    public void buildPipeline(StreamsBuilder builder) {
        JsonSerde<NoteEvent> noteEventSerde = new JsonSerde<>(NoteEvent.class);

        KStream<String, NoteEvent> events = builder.stream(
                notesInbound,
                Consumed.with(Serdes.String(), noteEventSerde)
                        .withName("source-activity-inbound")
        );

        // Group by domain (same as stats topology — but now we window it)
        KGroupedStream<String, NoteEvent> groupedByDomain = events
                .filter((key, event) -> event.getNote() != null
                        && event.getNote().getDomain() != null,
                        Named.as("filter-activity-valid"))
                .groupBy(
                        (key, event) -> event.getNote().getDomain().toLowerCase(),
                        Grouped.with("group-by-domain-windowed", Serdes.String(), noteEventSerde)
                );

        // TUMBLING WINDOW: 5-minute fixed windows
        // With a 1-minute grace period for late events
        TimeWindowedKStream<String, NoteEvent> windowed = groupedByDomain
                .windowedBy(
                        TimeWindows.ofSizeAndGrace(
                                Duration.ofMinutes(5),   // Window size
                                Duration.ofMinutes(1)    // Grace period for late arrivals
                        )
                );

        // Count events per domain per window
        windowed
                .count(
                        Named.as("activity-count"),
                        Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(WINDOW_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long())
                                // How far back the store retains windows
                                .withRetention(Duration.ofHours(1))
                )
                .toStream()
                .peek((windowedKey, count) -> {
                    // The key is now a Windowed<String> containing the domain + window time
                    String domain = windowedKey.key();
                    long windowStart = windowedKey.window().start();
                    long windowEnd = windowedKey.window().end();
                    log.info("Activity window: domain={}, window=[{}-{}], count={}",
                            domain,
                            java.time.Instant.ofEpochMilli(windowStart),
                            java.time.Instant.ofEpochMilli(windowEnd),
                            count);
                });

        log.info("Activity window topology built: {} -> 5-min tumbling windows", notesInbound);
    }
}
