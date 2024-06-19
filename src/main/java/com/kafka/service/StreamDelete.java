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
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import org.springframework.stereotype.Service;
import java.util.Properties;

@Service
public class StreamDelete {


    public void streamsDelete(String columnName) {

        String mainTopic = "main_logs";

        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "210.178.40.82:29092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"column_delete_stream_"+columnName);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class.getName());
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS,JsonDeserializer.class.getName());
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE,JsonNode.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(mainTopic);

        ObjectMapper objectMapper = new ObjectMapper();

        KStream<String, String> modifiedStream = stream.mapValues(value -> {
            try {
                JsonNode jsonNode = objectMapper.readTree(value);
                if (jsonNode instanceof ObjectNode) {
                    ((ObjectNode) jsonNode).remove(columnName);
                }
                return objectMapper.writeValueAsString(jsonNode);
            } catch (Exception e) {
                e.printStackTrace();
                return value; // JSON 파싱 또는 변환 중 오류가 발생하면 원래 값을 반환
            }
        });

        modifiedStream.to("delete_stream_"+columnName,Produced.with(Serdes.String(),Serdes.String()));
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

