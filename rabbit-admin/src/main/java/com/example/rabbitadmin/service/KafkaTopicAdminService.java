package com.example.rabbitadmin.service;

import org.apache.kafka.clients.admin.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaTopicAdminService {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicAdminService.class);

    private final KafkaAdmin kafkaAdmin;

    public KafkaTopicAdminService(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    private AdminClient getAdminClient() {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    public void createTopic(String name, int partitions, short replicationFactor) {
        try (AdminClient client = getAdminClient()) {
            NewTopic newTopic = new NewTopic(name, partitions, replicationFactor);
            client.createTopics(Collections.singletonList(newTopic)).all().get(10, TimeUnit.SECONDS);
            log.info("[KAFKA-ADMIN] Topico '{}' creado exitosamente con {} particiones y {} replicas",
                    name, partitions, replicationFactor);
        } catch (Exception e) {
            log.error("[KAFKA-ADMIN] Error al crear topico '{}': {}", name, e.getMessage());
            throw new RuntimeException("Error al crear topico en Kafka: " + e.getMessage(), e);
        }
    }

    public List<Map<String, Object>> listTopics() {
        try (AdminClient client = getAdminClient()) {
            Set<String> names = client.listTopics().names().get(10, TimeUnit.SECONDS);
            DescribeTopicsResult describeResult = client.describeTopics(names);
            Map<String, TopicDescription> descriptions = describeResult.allTopicNames().get(10, TimeUnit.SECONDS);

            List<Map<String, Object>> result = new ArrayList<>();
            for (Map.Entry<String, TopicDescription> entry : descriptions.entrySet()) {
                TopicDescription desc = entry.getValue();
                Map<String, Object> topicInfo = new HashMap<>();
                topicInfo.put("name", desc.name());
                topicInfo.put("partitionsCount", desc.partitions().size());
                topicInfo.put("isInternal", desc.isInternal());
                int replicaCount = desc.partitions().isEmpty() ? 0 : desc.partitions().get(0).replicas().size();
                topicInfo.put("replicationFactor", replicaCount);
                result.add(topicInfo);
            }
            return result;
        } catch (Exception e) {
            log.error("[KAFKA-ADMIN] Error al listar topicos: {}", e.getMessage());
            throw new RuntimeException("Error al listar topicos de Kafka: " + e.getMessage(), e);
        }
    }

    public void deleteTopic(String name) {
        try (AdminClient client = getAdminClient()) {
            client.deleteTopics(Collections.singletonList(name)).all().get(10, TimeUnit.SECONDS);
            log.info("[KAFKA-ADMIN] Topico '{}' eliminado", name);
        } catch (Exception e) {
            log.error("[KAFKA-ADMIN] Error al eliminar topico '{}': {}", name, e.getMessage());
            throw new RuntimeException("Error al eliminar topico de Kafka: " + e.getMessage(), e);
        }
    }
}
