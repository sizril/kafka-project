package com.kafka.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Service
public class StreamAdd {

    public void streamsAdd(String columnName) {
        // 스트림 설정
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "210.178.40.82:29092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams_add");
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
        KStream<String, String> topic1Stream = builder.stream("test");
        KTable<String, JsonNode> topic2Table = builder.table("test2");

        // json 처리를 위한 객체
        ObjectMapper objectMapper = new ObjectMapper();

        // 토픽1의 데이터를 토픽2의 데이터로 업데이트
        KStream<String, String> modifiedStream = topic1Stream.leftJoin(topic2Table, (value1, value2) -> {
            try {
                JsonNode jsonNode1 = objectMapper.readTree(value1);
                if (jsonNode1 instanceof ObjectNode) {
                    if (value2 != null && value2.has(columnName)) {
                        ((ObjectNode) jsonNode1).put(columnName, value2.get(columnName).asText());
                    } else {
                        ((ObjectNode) jsonNode1).putNull(columnName);
                    }
                }
                return objectMapper.writeValueAsString(jsonNode1);
            } catch (Exception e) {
                e.printStackTrace();
                return value1; // JSON 파싱 또는 변환 중 오류가 발생하면 원래 값을 반환
            }
        });

        // 수정된 스트림을 토픽3으로 전송
        modifiedStream.to("test_stream", Produced.with(Serdes.String(), Serdes.String()));
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
