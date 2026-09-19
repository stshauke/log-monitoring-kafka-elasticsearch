package com.learn.logs;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.learn.logs.service.LogGeneratorService;

@SpringBootApplication
@EnableScheduling // necessaire pour le generateur de logs automatique
public class LogMonitoringApplication {

    public static void main(String[] args) {
        SpringApplication.run(LogMonitoringApplication.class, args);
    }

    @Bean
    public NewTopic appLogsTopic() {
        return TopicBuilder.name(LogGeneratorService.TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
