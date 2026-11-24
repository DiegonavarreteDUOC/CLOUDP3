package com.example.minipedidos.messaging.consumer;

import com.example.minipedidos.config.KafkaConfig;
import com.example.minipedidos.model.PedidoEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consumidor Kafka: Auditoria y Trazabilidad en Tiempo Real.
 * Pertenece al Consumer Group independiente 'grupo-auditoria-tiempo-real'.
 * Lee el mismo topico 'pedidos.eventos' con su propio cursor de offset.
 */
@Component
public class AuditoriaKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(AuditoriaKafkaConsumer.class);

    private final AtomicInteger totalAuditados = new AtomicInteger(0);

    @KafkaListener(
            topics = KafkaConfig.TOPIC_PEDIDOS_EVENTOS,
            groupId = "grupo-auditoria-tiempo-real"
    )
    public void auditarEventoPedido(
            @Payload PedidoEvent evento,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp
    ) {
        int count = totalAuditados.incrementAndGet();

        log.info("[KAFKA-CONSUMER-AUDITORIA] [#Auditoria={}] [Particion={} | Offset={}] PedidoId={}, Cliente='{}', Tipo='{}', Origen='{}'",
                count, partition, offset, evento.getPedidoId(), evento.getCliente(), evento.getTipoEvento(), evento.getOrigen());
    }

    public int getTotalAuditados() {
        return totalAuditados.get();
    }
}
