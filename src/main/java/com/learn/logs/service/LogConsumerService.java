package com.learn.logs.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.logs.model.LogEvent;
import com.learn.logs.repository.LogEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class LogConsumerService {

    private final LogEventRepository repository;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = LogGeneratorService.TOPIC, groupId = "log-monitoring-group")
    public void consume(String message) {
        try {
            LogEvent event = objectMapper.readValue(message, LogEvent.class);
            repository.save(event);
        } catch (Exception e) {
            log.error("Erreur lors de l'indexation du log", e);
        }
    }
}
