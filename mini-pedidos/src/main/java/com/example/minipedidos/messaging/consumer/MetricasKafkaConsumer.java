package com.example.minipedidos.messaging.consumer;

import com.example.minipedidos.config.KafkaConfig;
import com.example.minipedidos.model.MetricaEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consumidor Kafka: Monitoreo y Metricas Operacionales en Tiempo Real.
 * Pertenece al Consumer Group 'grupo-metricas-stream'.
 * Consume eventos del topico 'pedidos.metricas'.
 */
@Component
public class MetricasKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(MetricasKafkaConsumer.class);

    private final AtomicInteger contadorMetricas = new AtomicInteger(0);

    @KafkaListener(
            topics = KafkaConfig.TOPIC_PEDIDOS_METRICAS,
            groupId = "grupo-metricas-stream"
    )
    public void procesarMetrica(
            @Payload MetricaEvent metrica,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        int count = contadorMetricas.incrementAndGet();

        log.info("[KAFKA-CONSUMER-METRICAS] [#Metrica={}] [Particion={} | Offset={}] Nombre='{}', Valor={} {}, Origen='{}'",
                count, partition, offset, metrica.getNombre(), metrica.getValor(), metrica.getUnidad(), metrica.getServicio());
    }

    public int getContadorMetricas() {
        return contadorMetricas.get();
    }
}
