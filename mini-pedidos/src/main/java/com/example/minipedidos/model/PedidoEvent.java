package com.example.minipedidos.model;

import java.io.Serializable;
import java.util.UUID;

/**
 * Evento de dominio para streaming en Apache Kafka.
 * Representa cualquier cambio de estado en el ciclo de vida de un pedido.
 */
public class PedidoEvent implements Serializable {

    private String eventId;
    private Long pedidoId;
    private String producto;
    private Integer cantidad;
    private String cliente;
    private String tipoEvento; // PEDIDO_CREADO, PEDIDO_PAGADO, PEDIDO_ANULADO
    private long timestamp;
    private String origen;

    public PedidoEvent() {
    }

    public PedidoEvent(Long pedidoId, String producto, Integer cantidad, String cliente, String tipoEvento) {
        this.eventId = UUID.randomUUID().toString();
        this.pedidoId = pedidoId;
        this.producto = producto;
        this.cantidad = cantidad;
        this.cliente = cliente;
        this.tipoEvento = tipoEvento;
        this.timestamp = System.currentTimeMillis();
        this.origen = "mini-pedidos-core";
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public Long getPedidoId() {
        return pedidoId;
    }

    public void setPedidoId(Long pedidoId) {
        this.pedidoId = pedidoId;
    }

    public String getProducto() {
        return producto;
    }

    public void setProducto(String producto) {
        this.producto = producto;
    }

    public Integer getCantidad() {
        return cantidad;
    }

    public void setCantidad(Integer cantidad) {
        this.cantidad = cantidad;
    }

    public String getCliente() {
        return cliente;
    }

    public void setCliente(String cliente) {
        this.cliente = cliente;
    }

    public String getTipoEvento() {
        return tipoEvento;
    }

    public void setTipoEvento(String tipoEvento) {
        this.tipoEvento = tipoEvento;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getOrigen() {
        return origen;
    }

    public void setOrigen(String origen) {
        this.origen = origen;
    }

    @Override
    public String toString() {
        return "PedidoEvent{" +
                "eventId='" + eventId + '\'' +
                ", pedidoId=" + pedidoId +
                ", producto='" + producto + '\'' +
                ", cantidad=" + cantidad +
                ", cliente='" + cliente + '\'' +
                ", tipoEvento='" + tipoEvento + '\'' +
                ", timestamp=" + timestamp +
                ", origen='" + origen + '\'' +
                '}';
    }
}
