package com.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {"com.kafka.service", "com.kafka", "com.kafka.connector"})
public class KafkaStreamsApp {
    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsApp.class, args);
    }
}