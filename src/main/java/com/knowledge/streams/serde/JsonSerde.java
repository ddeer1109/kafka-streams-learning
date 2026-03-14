package com.knowledge.streams.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Generic JSON Serde (Serializer/Deserializer) for any Java type.
 *
 * KAFKA CONCEPT — SERDES:
 * Every piece of data in Kafka is stored as raw bytes. Serdes define how
 * your Java objects convert to/from bytes. Kafka provides built-in serdes
 * for primitives (String, Integer, Long, etc.), but for domain objects
 * you need custom serdes.
 *
 * This generic serde uses Jackson to convert any POJO to/from JSON bytes.
 * In production, you might use Avro + Schema Registry instead for:
 *   - Schema evolution (adding/removing fields safely)
 *   - Compact binary format (smaller messages)
 *   - Schema enforcement across producers and consumers
 *
 * Usage in a topology:
 *   Serde<NoteEvent> noteEventSerde = new JsonSerde<>(NoteEvent.class);
 *   stream.to("topic", Produced.with(Serdes.String(), noteEventSerde));
 */
public class JsonSerde<T> implements Serde<T> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    private final Class<T> type;

    public JsonSerde(Class<T> type) {
        this.type = type;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to serialize " + type.getSimpleName() + " for topic " + topic, e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null || bytes.length == 0) return null;
            try {
                return MAPPER.readValue(bytes, type);
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to deserialize " + type.getSimpleName() + " from topic " + topic, e);
            }
        };
    }
}
