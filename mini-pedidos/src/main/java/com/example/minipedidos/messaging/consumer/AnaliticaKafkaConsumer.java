package com.example.minipedidos.messaging.consumer;

import com.example.minipedidos.config.KafkaConfig;
import com.example.minipedidos.model.PedidoEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Consumidor Kafka: Analitica de Ventas en Tiempo Real.
 * Pertenece al Consumer Group 'grupo-analitica-ventas'.
 * Procesa flujo de eventos para metricas comerciales y proyecciones.
 */
@Component
public class AnaliticaKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(AnaliticaKafkaConsumer.class);

    private final AtomicInteger totalPedidosProcesados = new AtomicInteger(0);
    private final AtomicLong totalUnidadesVendidas = new AtomicLong(0);

    @KafkaListener(
            topics = KafkaConfig.TOPIC_PEDIDOS_EVENTOS,
            groupId = "grupo-analitica-ventas"
    )
    public void procesarAnaliticaVentas(
            @Payload PedidoEvent evento,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic
    ) {
        log.info("[KAFKA-CONSUMER-ANALITICA] [Topic='{}' | Particion={} | Offset={}] Procesando evento: pedidoId={}, producto='{}', cantidad={}",
                topic, partition, offset, evento.getPedidoId(), evento.getProducto(), evento.getCantidad());

        // Simulacion de Poison Pill para probar Dead Letter Topic (DLT)
        if ("POISON_PILL".equalsIgnoreCase(evento.getProducto()) || "ERROR_DLT".equalsIgnoreCase(evento.getProducto())) {
            log.warn("[KAFKA-CONSUMER-ANALITICA] Mensaje venenoso detectado (producto={}) -> Forzando excepcion para activar DLT",
                    evento.getProducto());
            throw new RuntimeException("Simulacion de error critico de deserializacion/procesamiento para prueba DLT");
        }

        int pedidos = totalPedidosProcesados.incrementAndGet();
        long unidades = totalUnidadesVendidas.addAndGet(evento.getCantidad() != null ? evento.getCantidad() : 1);

        log.info("[KAFKA-CONSUMER-ANALITICA] [OK] Acumulado comercial en tiempo real: Pedidos={}, Unidades={}",
                pedidos, unidades);
    }

    public int getTotalPedidosProcesados() {
        return totalPedidosProcesados.get();
    }

    public long getTotalUnidadesVendidas() {
        return totalUnidadesVendidas.get();
    }
}
