package com.kafka.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class StreamAdd {

    public void streamsAdd(String columnName) {

        String mainTopic = "main_logs";
        // 스트림 설정
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "210.178.40.82:29092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "column_add_stream_"+columnName);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class.getName());
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, JsonNode.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        // 스트림을 빌드하기 위한 객체
        StreamsBuilder builder = new StreamsBuilder();
        // 토픽1과 토픽2로부터 스트림 생성
        KStream<String, String> topic1Stream = builder.stream(mainTopic);
        KStream<String, String> topic2Stream = builder.stream("userInfo");

        // json 처리를 위한 객체
        ObjectMapper objectMapper = new ObjectMapper();

        // topic2Stream을 KTable로 변환하여 마지막 활동 저장
        KTable<String, String> userLastActivity = topic2Stream
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
                        () -> "",
                        (key, value, aggregate) -> value, // 새로운 값으로 교체
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("user-last-activity-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String())
                );

        // topic1Stream과 userLastActivity를 left join
        KStream<String, String> joinedStream = topic1Stream
                .selectKey((key, value) -> {
                    try {
                        JsonNode jsonNode = objectMapper.readTree(value);
                        return jsonNode.get("user").asText();
                    } catch (Exception e) {
                        e.printStackTrace();
                        return "unknown";
                    }
                })
                .leftJoin(userLastActivity,
                        (value1, value2) -> {
                            try {
                                ObjectNode jsonNode1 = (ObjectNode) objectMapper.readTree(value1);
                                if (value2 != null) {
                                    JsonNode jsonNode2 = objectMapper.readTree(value2);
                                    jsonNode1.set(columnName, jsonNode2.get(columnName));
                                } else {
                                    jsonNode1.set(columnName, null);
                                }
                                return jsonNode1.toString();
                            } catch (Exception e) {
                                e.printStackTrace();
                                return value1;
                            }
                        },
                        Joined.with(Serdes.String(), Serdes.String(), Serdes.String())
                );

        // 수정된 스트림을 토픽3으로 전송
        joinedStream.to("add_stream_"+columnName, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}