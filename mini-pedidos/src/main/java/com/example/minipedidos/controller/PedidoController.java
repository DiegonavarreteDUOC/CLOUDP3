package com.example.minipedidos.controller;

import com.example.minipedidos.messaging.consumer.AnaliticaKafkaConsumer;
import com.example.minipedidos.messaging.consumer.AuditoriaKafkaConsumer;
import com.example.minipedidos.messaging.producer.PedidoKafkaProducer;
import com.example.minipedidos.messaging.producer.PedidoProducer;
import com.example.minipedidos.model.MetricaEvent;
import com.example.minipedidos.model.Pedido;
import com.example.minipedidos.model.PedidoEvent;
import com.example.minipedidos.service.PedidoSqsProducer;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Controlador REST del dominio Pedidos.
 * Integra orquestacion hibrida de mensajeria:
 * - RabbitMQ: Colas transaccionales y enrutamiento con DLQ.
 * - AWS SQS (ElasticMQ): Buffer asincrono en la nube.
 * - Apache Kafka: Event Streaming en tiempo real, analitica y auditoria multi-consumer.
 */
@RestController
@RequestMapping("/pedidos")
public class PedidoController {

    private final PedidoProducer pedidoProducer;
    private final PedidoSqsProducer pedidoSqsProducer;
    private final PedidoKafkaProducer pedidoKafkaProducer;
    private final AnaliticaKafkaConsumer analiticaKafkaConsumer;
    private final AuditoriaKafkaConsumer auditoriaKafkaConsumer;

    public PedidoController(PedidoProducer pedidoProducer,
                            PedidoSqsProducer pedidoSqsProducer,
                            PedidoKafkaProducer pedidoKafkaProducer,
                            AnaliticaKafkaConsumer analiticaKafkaConsumer,
                            AuditoriaKafkaConsumer auditoriaKafkaConsumer) {
        this.pedidoProducer = pedidoProducer;
        this.pedidoSqsProducer = pedidoSqsProducer;
        this.pedidoKafkaProducer = pedidoKafkaProducer;
        this.analiticaKafkaConsumer = analiticaKafkaConsumer;
        this.auditoriaKafkaConsumer = auditoriaKafkaConsumer;
    }

    /**
     * Crea un nuevo pedido y lo distribuye a traves de la arquitectura hibrida:
     * RabbitMQ (transaccional) + SQS (buffer) + Kafka (streaming de eventos).
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> crearPedido(@RequestBody @Valid Pedido pedido) {
        System.out.println("[API] Pedido recibido para distribucion hibrida: " + pedido);

        // 1. RabbitMQ: Enrutamiento directo y topico hacia colas con DLQ
        pedidoProducer.enviarPedido(pedido);
        pedidoProducer.enviarNotificacion(pedido);
        pedidoProducer.enviarReporte(pedido);

        // 2. AWS SQS (ElasticMQ)
        pedidoSqsProducer.enviarPedido(pedido);

        // 3. Apache Kafka: Event Streaming en tiempo real (Particionado por ID de pedido)
        PedidoEvent eventoKafka = new PedidoEvent(
                pedido.getId(),
                pedido.getProducto(),
                pedido.getCantidad(),
                pedido.getCliente(),
                "PEDIDO_CREADO"
        );
        pedidoKafkaProducer.enviarEventoPedido(eventoKafka);

        // 4. Apache Kafka: Metrica de telemetria en tiempo real
        MetricaEvent metrica = new MetricaEvent(
                "TIEMPO_RESPUESTA_ORDEN",
                Math.round((Math.random() * 15.0 + 5.0) * 100.0) / 100.0,
                "ms"
        );
        pedidoKafkaProducer.enviarMetrica(metrica);

        Map<String, Object> response = new HashMap<>();
        response.put("status", "SUCCESS");
        response.put("mensaje", "Pedido procesado en RabbitMQ, AWS SQS y Apache Kafka Streaming");
        response.put("pedidoId", pedido.getId());
        response.put("kafkaEventId", eventoKafka.getEventId());

        return ResponseEntity.ok(response);
    }

    /**
     * Consulta el estado de las proyecciones y analitica en memoria calculada desde el stream de Kafka.
     */
    @GetMapping("/analitica-en-vivo")
    public ResponseEntity<Map<String, Object>> obtenerAnaliticaEnVivo() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalPedidosProcesadosAnalitica", analiticaKafkaConsumer.getTotalPedidosProcesados());
        stats.put("totalUnidadesVendidas", analiticaKafkaConsumer.getTotalUnidadesVendidas());
        stats.put("totalEventosAuditados", auditoriaKafkaConsumer.getTotalAuditados());
        return ResponseEntity.ok(stats);
    }
}
