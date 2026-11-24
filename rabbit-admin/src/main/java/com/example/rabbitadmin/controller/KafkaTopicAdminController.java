package com.example.rabbitadmin.controller;

import com.example.rabbitadmin.dto.KafkaTopicRequest;
import com.example.rabbitadmin.service.KafkaTopicAdminService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/admin/kafka/topics")
public class KafkaTopicAdminController {

    private final KafkaTopicAdminService kafkaTopicAdminService;

    public KafkaTopicAdminController(KafkaTopicAdminService kafkaTopicAdminService) {
        this.kafkaTopicAdminService = kafkaTopicAdminService;
    }

    @PostMapping
    public ResponseEntity<String> createTopic(@Valid @RequestBody KafkaTopicRequest request) {
        kafkaTopicAdminService.createTopic(
                request.getName(),
                request.getPartitions(),
                request.getReplicationFactor()
        );
        return ResponseEntity.status(HttpStatus.CREATED)
                .body("Topico '" + request.getName() + "' creado exitosamente en Kafka ("
                        + request.getPartitions() + " particiones, "
                        + request.getReplicationFactor() + " replicas)");
    }

    @GetMapping
    public ResponseEntity<List<Map<String, Object>>> listTopics() {
        return ResponseEntity.ok(kafkaTopicAdminService.listTopics());
    }

    @DeleteMapping("/{name}")
    public ResponseEntity<String> deleteTopic(@PathVariable String name) {
        kafkaTopicAdminService.deleteTopic(name);
        return ResponseEntity.ok("Topico '" + name + "' eliminado exitosamente");
    }
}
