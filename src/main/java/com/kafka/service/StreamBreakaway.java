package com.kafka.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Service
public class StreamBreakaway {

    private KafkaStreams streams;

    public void streamsBreakaway(int seconds) {

        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "210.178.40.82:29092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams_breakaway");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class.getName());
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, JsonNode.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("test");

        ObjectMapper objectMapper = new ObjectMapper();

        // 사용자별로 마지막 활동 시간 집계
        KTable<String, Long> userLastActivity = stream
                .groupBy((key, value) -> {
                    try {
                        JsonNode jsonNode = objectMapper.readTree(value);
                        return jsonNode.get("user").asText();
                    } catch (Exception e) {
                        e.printStackTrace();
                        return "unknown";
                    }
                })
                .aggregate(
                        () -> 0L,
                        (key, value, aggregate) -> {
                            try {
                                JsonNode jsonNode = objectMapper.readTree(value);
                                Long currentTimestamp = jsonNode.get("timestamp").asLong();
                                return Math.max(aggregate, currentTimestamp);
                            } catch (Exception e) {
                                e.printStackTrace();
                                return aggregate;
                            }
                        },
                        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("user-last-activity-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long())
                );

        // 잠재적 이탈 사용자 필터링
        long durationInSeconds = seconds;
        KStream<String, String> potentialChurnUsers = userLastActivity
                .toStream()
                .filter((user, lastActivity) -> (System.currentTimeMillis() / 1000) - lastActivity > durationInSeconds)
                .mapValues((user, lastActivity) -> "User " + user + " might be churning.");

        potentialChurnUsers.to("test_stream", Produced.with(Serdes.String(), Serdes.String()));

        streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
