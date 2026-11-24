package com.example.minipedidos.model;

import java.io.Serializable;
import java.util.UUID;

/**
 * Evento de métrica operativa y telemetría en tiempo real para Kafka.
 */
public class MetricaEvent implements Serializable {

    private String metricaId;
    private String nombre;
    private Double valor;
    private String unidad;
    private long timestamp;
    private String servicio;

    public MetricaEvent() {
    }

    public MetricaEvent(String nombre, Double valor, String unidad) {
        this.metricaId = UUID.randomUUID().toString();
        this.nombre = nombre;
        this.valor = valor;
        this.unidad = unidad;
        this.timestamp = System.currentTimeMillis();
        this.servicio = "mini-pedidos";
    }

    public String getMetricaId() {
        return metricaId;
    }

    public void setMetricaId(String metricaId) {
        this.metricaId = metricaId;
    }

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public Double getValor() {
        return valor;
    }

    public void setValor(Double valor) {
        this.valor = valor;
    }

    public String getUnidad() {
        return unidad;
    }

    public void setUnidad(String unidad) {
        this.unidad = unidad;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getServicio() {
        return servicio;
    }

    public void setServicio(String servicio) {
        this.servicio = servicio;
    }

    @Override
    public String toString() {
        return "MetricaEvent{" +
                "metricaId='" + metricaId + '\'' +
                ", nombre='" + nombre + '\'' +
                ", valor=" + valor +
                ", unidad='" + unidad + '\'' +
                ", timestamp=" + timestamp +
                ", servicio='" + servicio + '\'' +
                '}';
    }
}
