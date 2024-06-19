package com.kafka.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class StreamCondition {

    public void streamsCondition(String column, String operator, String condition) {

        String mainTopic = "main_logs";

        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "210.178.40.82:29092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "filter_stream_"+operator+condition);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class.getName());
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, JsonNode.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(mainTopic);

        ObjectMapper objectMapper = new ObjectMapper();

        KStream<String, String> filteredStream = stream.filter((key, value) -> {
            try {
                JsonNode jsonNode = objectMapper.readTree(value);
                if (!jsonNode.has(column)) {
                    return false;
                }

                String columnValue_s = jsonNode.get(column).asText();

                if ("=".equals(operator)) {
                    // 문자열 비교
                    return columnValue_s.equals(condition);
                } else {
                    // 숫자 비교
                    return compareNumbers(columnValue_s, condition, operator);
                }
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        });

        filteredStream.to("filter_stream_"+operator+condition, Produced.with(Serdes.String(), Serdes.String()));
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private boolean compareNumbers(String columnValue_s, String condition, String operator) {
        try {
            double columnValue = Double.parseDouble(columnValue_s);
            double cond = Double.parseDouble(condition);

            switch (operator) {
                case ">":
                    return columnValue > cond;
                case "<":
                    return columnValue < cond;
                case ">=":
                    return columnValue >= cond;
                case "<=":
                    return columnValue <= cond;
                default:
                    return false;
            }
        } catch (NumberFormatException e) {
            return false; // 숫자 변환 실패 시 false 반환
        }
    }
}
