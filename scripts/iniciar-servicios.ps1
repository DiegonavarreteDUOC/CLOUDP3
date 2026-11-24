# Script de orquestacion para CLOUDP3
# Levanta Kafka (3 Brokers + Zookeeper + Kafka-UI), RabbitMQ Cluster (2 Nodos) y ElasticMQ SQS

Write-Host "=========================================================="
Write-Host "INICIANDO INFRAESTRUCTURA CLOUDP3 (KAFKA + RABBITMQ + SQS)"
Write-Host "=========================================================="

# 1. Iniciar contenedores
docker compose up -d

Write-Host "`nEsperando 20 segundos a que los servicios inicien..."
Start-Sleep -Seconds 20

# 2. Sincronizar cluster RabbitMQ
Write-Host "`nConfigurando Cluster de RabbitMQ (rabbit@rabbitmq2 -> rabbit@rabbitmq1)..."
docker exec rabbitmq2 rabbitmqctl stop_app
docker exec rabbitmq2 rabbitmqctl reset
docker exec rabbitmq2 rabbitmqctl join_cluster rabbit@rabbitmq1
docker exec rabbitmq2 rabbitmqctl start_app

Write-Host "`n=========================================================="
Write-Host "ESTADO FINAL DE LOS SERVICIOS DOCKER:"
Write-Host "=========================================================="
docker compose ps

Write-Host "`n[OK] Servicios listos:"
Write-Host "• Kafka UI:        http://localhost:8085"
Write-Host "• RabbitMQ Nodo 1: http://localhost:15672 (guest/guest)"
Write-Host "• RabbitMQ Nodo 2: http://localhost:15673 (guest/guest)"
Write-Host "• ElasticMQ SQS:   http://localhost:9324"
Write-Host "• Kafka Brokers:   localhost:9092, localhost:9093, localhost:9094"
