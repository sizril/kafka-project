package com.kafka.connector;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;

import java.util.Iterator;
import java.util.Properties;

// sink connector로 db에 삽입할 메시지에 스키마 추가
@Service
public class InjectSchema {
    public String injecting(String topic){

        String processedTopic = topic + "_processed";

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, topic + "_schemaInjection");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "210.178.40.82:29092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> sourceStream = builder.stream(topic);

        ObjectMapper objectMapper = new ObjectMapper();

        KStream<String, String> transformedStream = sourceStream.mapValues(value -> {
            try {
                // 토픽에서 가져온 원본 메시지
                ObjectNode originalNode = (ObjectNode) objectMapper.readTree(value);

                // 메시지에 추가할 내용들
                ObjectNode resultNode = objectMapper.createObjectNode();
                ObjectNode schemaNode = objectMapper.createObjectNode();
                ObjectNode payloadNode = objectMapper.createObjectNode();

                // Create fields array for schema
                ArrayNode fieldsArray = objectMapper.createArrayNode();

                // 원본 메시지의 필드 개수만큼 반복해서 type과 field생성
                Iterator<String> fieldNames = originalNode.fieldNames();
                while (fieldNames.hasNext()) {
                    String fieldName = fieldNames.next();
                    JsonNode fieldValue = originalNode.get(fieldName);
                    payloadNode.put(fieldName, fieldValue.asText());

                    fieldsArray.add(objectMapper.createObjectNode()
                            .put("type", "string")
                            .put("field", fieldName));
                }

                // 스키마 추가
                schemaNode.put("type", "struct");
                schemaNode.set("fields", fieldsArray);
                schemaNode.put("optional", false);

                resultNode.set("schema", schemaNode);
                resultNode.set("payload", payloadNode);

                return objectMapper.writeValueAsString(resultNode);
            } catch (Exception e) {
                e.printStackTrace();
                return value;
            }
        });

        transformedStream.to(processedTopic, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        //CreateConnector에서 사용할 source topic명 반환
        return processedTopic;
    }
}