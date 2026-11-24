package com.example.rabbitadmin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public class KafkaTopicRequest {

    @NotBlank(message = "El nombre del topico no puede estar vacio")
    private String name;

    @NotNull(message = "El numero de particiones es obligatorio")
    @Min(value = 1, message = "Debe tener al menos 1 particion")
    @Max(value = 50, message = "No se permiten mas de 50 particiones")
    private Integer partitions;

    @NotNull(message = "El factor de replica es obligatorio")
    @Min(value = 1, message = "El factor de replica debe ser al menos 1")
    @Max(value = 3, message = "El cluster soporta maximo 3 replicas")
    private Short replicationFactor;

    public KafkaTopicRequest() {
    }

    public KafkaTopicRequest(String name, Integer partitions, Short replicationFactor) {
        this.name = name;
        this.partitions = partitions;
        this.replicationFactor = replicationFactor;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getPartitions() {
        return partitions;
    }

    public void setPartitions(Integer partitions) {
        this.partitions = partitions;
    }

    public Short getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(Short replicationFactor) {
        this.replicationFactor = replicationFactor;
    }
}
