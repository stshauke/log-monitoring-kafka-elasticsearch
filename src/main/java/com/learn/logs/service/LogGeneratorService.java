package com.learn.logs.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.logs.model.LogEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class LogGeneratorService {

    public static final String TOPIC = "app-logs-topic";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final Random random = new Random();

    // Services et endpoints fictifs, pour simuler un vrai système distribué
    private static final List<String> SERVICES = List.of(
            "payment-service", "order-service", "auth-service", "inventory-service", "notification-service"
    );

    private static final List<String> ENDPOINTS = List.of(
            "/api/checkout", "/api/orders", "/api/login", "/api/products", "/api/notify", "/api/refund"
    );

    private static final List<String> METHODS = List.of("GET", "POST", "PUT", "DELETE");

    /**
     * Genere un lot de logs toutes les 3 secondes.
     * Injecte volontairement des anomalies (erreurs 500, latences elevees)
     * dans environ 10% des cas pour rendre le dashboard de monitoring interessant.
     */
    @Scheduled(fixedRate = 3000)
    public void generateLogs() {
        int batchSize = 3 + random.nextInt(5); // 3 a 7 logs par batch

        for (int i = 0; i < batchSize; i++) {
            LogEvent event = buildRandomEvent();
            try {
                String json = objectMapper.writeValueAsString(event);
                kafkaTemplate.send(TOPIC, json);
            } catch (Exception e) {
                log.error("Erreur lors de la serialisation du log", e);
            }
        }
        log.info("📤 {} logs generes et envoyes a Kafka", batchSize);
    }

    private LogEvent buildRandomEvent() {
        boolean isAnomaly = random.nextInt(100) < 10; // 10% d'anomalies

        String service = SERVICES.get(random.nextInt(SERVICES.size()));
        String endpoint = ENDPOINTS.get(random.nextInt(ENDPOINTS.size()));
        String method = METHODS.get(random.nextInt(METHODS.size()));

        int statusCode;
        long responseTime;
        String level;

        if (isAnomaly) {
            statusCode = random.nextBoolean() ? 500 : 503;
            responseTime = 800 + random.nextInt(2000); // requete lente
            level = "ERROR";
        } else {
            statusCode = 200;
            responseTime = 20 + random.nextInt(300); // requete normale
            level = "INFO";
        }

        return new LogEvent(
                UUID.randomUUID().toString(),
                service,
                endpoint,
                method,
                statusCode,
                responseTime,
                level,
                UUID.randomUUID().toString().substring(0, 8),
                Instant.now()
        );
    }
}
