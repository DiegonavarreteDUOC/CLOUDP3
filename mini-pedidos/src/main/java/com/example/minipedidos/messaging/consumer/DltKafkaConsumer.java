package com.example.minipedidos.messaging.consumer;

import com.example.minipedidos.config.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consumidor Kafka: Observabilidad de Fallos en Dead Letter Topic (DLT).
 * Pertenece al Consumer Group 'grupo-dlt-monitoring'.
 * Recibe los mensajes que agotaron los reintentos para analisis post-mortem.
 */
@Component
public class DltKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(DltKafkaConsumer.class);

    private final AtomicInteger fallosRegistrados = new AtomicInteger(0);

    @KafkaListener(
            topics = KafkaConfig.TOPIC_PEDIDOS_DLT,
            groupId = "grupo-dlt-monitoring"
    )
    public void procesarMensajeDlt(
            @Payload Object payload,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        int fallos = fallosRegistrados.incrementAndGet();

        log.error("[KAFKA-DLT-ALERTA] [#Fallo={}] Mensaje recuperado en DLT '{}' | Particion={} | Offset={}: {}",
                fallos, KafkaConfig.TOPIC_PEDIDOS_DLT, partition, offset, payload);
    }

    public int getFallosRegistrados() {
        return fallosRegistrados.get();
    }
}
