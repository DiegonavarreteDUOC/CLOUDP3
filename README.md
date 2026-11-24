# CLOUDP3 - Arquitectura Cloud Native: Apache Kafka Streaming + RabbitMQ Cluster + AWS SQS

**Asignatura:** Desarrollo Cloud Native I (DSY1107) - Duoc UC  
**Estudiante:** Diego Navarrete  
**Evaluación:** Encargo 5 (EP5) & Presentación Oral (EP6)  

---

## 📌 Descripción del Proyecto
Este repositorio contiene la solución completa de la **Prueba 3 (EP5 / EP6)**, incorporando una plataforma de **Event Streaming en tiempo real con Apache Kafka** dentro del ecosistema distribuido de microservicios desarrollado previamente (Java 21, Spring Boot, RabbitMQ en Clúster de 2 Nodos, AWS SQS simulado y AWS Cognito).

La solución implementa una **arquitectura híbrida de mensajería** donde:
1. **RabbitMQ (Clúster 2 Nodos HA):** Gestiona colas transaccionales de trabajo puntual (`pedidos`, `notificaciones`, `reportes`) con enrutamiento directo y de tópico, confirmación manual (`ACK/NACK`) y Dead Letter Queues (DLQ).
2. **AWS SQS (ElasticMQ):** Provee un buffer asíncrono desacoplado en la nube.
3. **Apache Kafka (Clúster de 3 Brokers):** Gestiona el streaming continuo de eventos de dominio inmutables para analítica en tiempo real, auditoría multi-consumidor y telemetría operativa.

---

## 🏗️ Topología del Clúster de Mensajería (Docker Compose)
El clúster está compuesto por 8 contenedores interconectados en una red dedicada (`cloud-net`):

| Servicio | Imagen / Tecnología | Puertos Host | Función |
| :--- | :--- | :--- | :--- |
| `zookeeper` | `cp-zookeeper:7.5.0` | 2181 | Coordinación de metadatos del clúster Kafka |
| `kafka1` | `cp-kafka:7.5.0` | 9092 | Broker 1 (Líder / Réplica de particiones) |
| `kafka2` | `cp-kafka:7.5.0` | 9093 | Broker 2 (Líder / Réplica de particiones) |
| `kafka3` | `cp-kafka:7.5.0` | 9094 | Broker 3 (Líder / Réplica de particiones) |
| `kafka-ui` | `provectuslabs/kafka-ui` | 8085 | Panel visual de observabilidad para Kafka |
| `rabbitmq1` | `rabbitmq:4.2-management` | 5672, 15672 | Nodo maestro del clúster RabbitMQ |
| `rabbitmq2` | `rabbitmq:4.2-management` | 5673, 15673 | Nodo esclavo del clúster RabbitMQ |
| `elasticmq` | `softwaremill/elasticmq` | 9324 | Simulador local de AWS SQS |

---

## 📊 Tópicos de Apache Kafka (Rúbrica EP5 / EP6)
Todos los tópicos fueron creados y verificados con **3 particiones y factor de réplica 3**:

1. **`pedidos.eventos`**:
   - Particiones: 3 | Réplicas: 3
   - Clave de partición: `pedidoId` (Garantiza orden estricto de eventos por pedido en la misma partición).
   - Retención: 86.400.000 ms (24 horas).
   - Grupos de consumidores: `grupo-analitica-ventas`, `grupo-auditoria-tiempo-real`.
2. **`pedidos.metricas`**:
   - Particiones: 3 | Réplicas: 3
   - Clave de partición: Nombre de la métrica (`TIEMPO_RESPUESTA_ORDEN`).
   - Retención: 604.800.000 ms (7 días).
   - Grupo de consumidores: `grupo-metricas-stream`.
3. **`pedidos.eventos.DLT`**:
   - Particiones: 3 | Réplicas: 3
   - Dead Letter Topic para aislar mensajes venenosos (*Poison Pills*) tras agotar la política de reintentos (`DefaultErrorHandler`).
   - Grupo de consumidores: `grupo-dlt-monitoring`.

---

## 👥 Grupos de Consumidores (Consumer Groups)
- **`grupo-analitica-ventas`**: Agrega métricas comerciales en tiempo real (total de pedidos procesados y unidades vendidas).
- **`grupo-auditoria-tiempo-real`**: Lee el mismo tópico `pedidos.eventos` de forma independiente manteniendo su propio cursor de offset para trazabilidad.
- **`grupo-metricas-stream`**: Monitorea las métricas operacionales emitidas por los productores.
- **`grupo-dlt-monitoring`**: Recibe y registra alertas críticas cuando un evento falla y es derivado al DLT.

---

## 🚀 Puesta en Marcha

### 1. Iniciar Infraestructura
```powershell
cd scripts
.\iniciar-servicios.ps1
```

### 2. Iniciar Microservicios Spring Boot (Java 21)
```powershell
# En terminal 1: Microservicio Principal de Pedidos
cd mini-pedidos
.\mvnw.cmd spring-boot:run

# En terminal 2: Administrador de Tópicos y Colas
cd rabbit-admin
.\mvnw.cmd spring-boot:run
```

### 3. Enlaces de Observabilidad y APIs
- **Kafka-UI:** `http://localhost:8085`
- **RabbitMQ Management:** `http://localhost:15672` (guest / guest)
- **API Pedidos:** `http://localhost:8080/pedidos`
- **API Analítica en Vivo:** `http://localhost:8080/pedidos/analitica-en-vivo`
- **API Admin Tópicos Kafka:** `http://localhost:8081/admin/kafka/topics`
