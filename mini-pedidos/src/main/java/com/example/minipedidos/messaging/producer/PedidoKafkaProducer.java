package com.example.minipedidos.messaging.producer;

import com.example.minipedidos.config.KafkaConfig;
import com.example.minipedidos.model.MetricaEvent;
import com.example.minipedidos.model.PedidoEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * Productor de eventos para Apache Kafka.
 * Publica eventos de negocio y metricas operacionales con particionamiento por clave.
 */
@Component
public class PedidoKafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(PedidoKafkaProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PedidoKafkaProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Envia evento de pedido al topico 'pedidos.eventos' usando pedidoId como partition key.
     * Esto garantiza que todos los eventos de un mismo pedido queden en la misma particion (orden estricto).
     */
    public CompletableFuture<SendResult<String, Object>> enviarEventoPedido(PedidoEvent evento) {
        String key = evento.getPedidoId() != null ? String.valueOf(evento.getPedidoId()) : evento.getEventId();

        log.info("[KAFKA-PRODUCER] Publicando evento pedidoId={} en topico '{}' con key='{}'",
                evento.getPedidoId(), KafkaConfig.TOPIC_PEDIDOS_EVENTOS, key);

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(
                KafkaConfig.TOPIC_PEDIDOS_EVENTOS,
                key,
                evento
        );

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("[KAFKA-PRODUCER] [EXITO] Evento enviado a Topic='{}', Particion={}, Offset={}",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("[KAFKA-PRODUCER] [ERROR] Fallo al publicar evento pedidoId={}: {}",
                        evento.getPedidoId(), ex.getMessage());
            }
        });

        return future;
    }

    /**
     * Envia telemetria/metricas operacionales en tiempo real al topico 'pedidos.metricas'.
     */
    public CompletableFuture<SendResult<String, Object>> enviarMetrica(MetricaEvent metrica) {
        String key = metrica.getNombre();

        log.info("[KAFKA-PRODUCER] Publicando metrica '{}'={} en topico '{}'",
                metrica.getNombre(), metrica.getValor(), KafkaConfig.TOPIC_PEDIDOS_METRICAS);

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(
                KafkaConfig.TOPIC_PEDIDOS_METRICAS,
                key,
                metrica
        );

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("[KAFKA-PRODUCER] [EXITO] Metrica enviada a Topic='{}', Particion={}, Offset={}",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("[KAFKA-PRODUCER] [ERROR] Fallo al publicar metrica: {}", ex.getMessage());
            }
        });

        return future;
    }
}
