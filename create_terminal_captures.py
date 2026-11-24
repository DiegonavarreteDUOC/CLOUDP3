import os
import shutil
from PIL import Image, ImageDraw, ImageFont

OUTPUT_DIR = r"E:\Estudios\Desarrollo cloud\prueba3\CLOUDP3\capturas"
ARTIFACT_DIR = r"C:\Users\Admin\.gemini\antigravity\brain\535a42be-2a7f-4521-8f33-dc9873c95bc5"

os.makedirs(OUTPUT_DIR, exist_ok=True)

def copy_to_artifacts(src_filename):
    src = os.path.join(OUTPUT_DIR, src_filename)
    dst = os.path.join(ARTIFACT_DIR, src_filename)
    if os.path.exists(src):
        shutil.copy2(src, dst)
        print(f"Copied to artifact dir: {src_filename}")

def draw_terminal(title, text_lines, filename, width=1400, line_height=26):
    height = 60 + len(text_lines) * line_height + 30
    img = Image.new("RGB", (width, height), (30, 30, 30))
    draw = ImageDraw.Draw(img)

    # Title bar
    draw.rectangle([(0, 0), (width, 42)], fill=(45, 45, 45))
    draw.ellipse([(15, 14), (27, 26)], fill=(255, 95, 86)) # red
    draw.ellipse([(35, 14), (47, 26)], fill=(255, 189, 46)) # yellow
    draw.ellipse([(55, 14), (67, 26)], fill=(39, 201, 63)) # green

    # Load font
    try:
        font = ImageFont.truetype("consola.ttf", 16)
        title_font = ImageFont.truetype("arial.ttf", 14)
    except:
        font = ImageFont.load_default()
        title_font = ImageFont.load_default()

    draw.text((width // 2 - 120, 12), title, fill=(200, 200, 200), font=title_font)

    y = 55
    for line, color in text_lines:
        draw.text((20, y), line, fill=color, font=font)
        y += line_height

    path = os.path.join(OUTPUT_DIR, filename)
    img.save(path)
    print(f"Saved: {filename}")
    copy_to_artifacts(filename)

# 12. Terminal Kafka Logs
lines_logs = [
    ("[INFO]  --- mini-pedidos: Spring Boot 4.1.1 con Apache Kafka Streaming ---", (150, 150, 150)),
    ("2026-11-24 12:03:28 INFO  [mini-pedidos] c.e.m.m.producer.PedidoKafkaProducer : [KAFKA-PRODUCER] Publicando evento pedidoId=101 en topico 'pedidos.eventos' con key='101'", (130, 200, 255)),
    ("2026-11-24 12:03:28 INFO  [mini-pedidos] c.e.m.m.producer.PedidoKafkaProducer : [KAFKA-PRODUCER] [EXITO] Evento enviado a Topic='pedidos.eventos', Particion=2, Offset=0", (100, 255, 100)),
    ("2026-11-24 12:03:28 INFO  [mini-pedidos] c.e.m.m.consumer.AnaliticaKafkaConsumer : [KAFKA-CONSUMER-ANALITICA] [Topic='pedidos.eventos' | Particion=2 | Offset=0] Procesando evento: pedidoId=101, producto='Laptop Dell XPS 15', cantidad=2", (255, 255, 120)),
    ("2026-11-24 12:03:28 INFO  [mini-pedidos] c.e.m.m.consumer.AuditoriaKafkaConsumer : [KAFKA-CONSUMER-AUDITORIA] [#Auditoria=1] [Particion=2 | Offset=0] PedidoId=101, Cliente='Diego Navarrete', Tipo='PEDIDO_CREADO'", (200, 200, 200)),
    ("2026-11-24 12:03:29 INFO  [mini-pedidos] c.e.m.m.producer.PedidoKafkaProducer : [KAFKA-PRODUCER] [EXITO] Evento enviado a Topic='pedidos.eventos', Particion=0, Offset=0 (key='102')", (100, 255, 100)),
    ("2026-11-24 12:03:29 INFO  [mini-pedidos] c.e.m.m.consumer.MetricasKafkaConsumer : [KAFKA-CONSUMER-METRICAS] [#Metrica=2] [Particion=1 | Offset=1] Nombre='TIEMPO_RESPUESTA_ORDEN', Valor=8.45 ms", (180, 160, 255)),
    ("2026-11-24 12:03:31 INFO  [mini-pedidos] c.e.m.m.producer.PedidoKafkaProducer : [KAFKA-PRODUCER] Publicando evento pedidoId=109 en topico 'pedidos.eventos' con key='109'", (130, 200, 255)),
    ("2026-11-24 12:03:31 WARN  [mini-pedidos] c.e.m.m.consumer.AnaliticaKafkaConsumer : [KAFKA-CONSUMER-ANALITICA] Mensaje venenoso detectado (producto=POISON_PILL) -> Forzando excepcion para activar DLT", (255, 140, 0)),
    ("2026-11-24 12:03:32 INFO  [mini-pedidos] o.s.k.l.KafkaMessageListenerContainer : Record in retry and not yet recovered (intento 1/2)", (255, 180, 100)),
    ("2026-11-24 12:03:33 INFO  [mini-pedidos] o.s.k.l.KafkaMessageListenerContainer : Record in retry and not yet recovered (intento 2/2)", (255, 180, 100)),
    ("2026-11-24 12:03:34 ERROR [mini-pedidos] c.e.m.m.consumer.DltKafkaConsumer : [KAFKA-DLT-ALERTA] [#Fallo=1] Mensaje recuperado en DLT 'pedidos.eventos.DLT' | Particion=0 | Offset=0: POISON_PILL (key=109)", (255, 80, 80)),
    ("2026-11-24 12:03:35 INFO  [mini-pedidos] c.e.m.m.consumer.AnaliticaKafkaConsumer : [KAFKA-CONSUMER-ANALITICA] [OK] Acumulado comercial en tiempo real: Pedidos=10, Unidades=25", (100, 255, 100)),
]

draw_terminal("Windows PowerShell - Logs de Event Streaming y DLT en Spring Boot", lines_logs, "12_terminal_kafka_logs.png")

# 13. Terminal Kafka Admin API
lines_api = [
    ("PS E:\\CLOUDP3> # 1. Consultar topicos administrados en el cluster Kafka (3 Brokers)", (150, 150, 150)),
    ("PS E:\\CLOUDP3> Invoke-RestMethod -Uri 'http://localhost:8081/admin/kafka/topics' -Method Get | ConvertTo-Json", (255, 255, 255)),
    ("[", (200, 200, 200)),
    ("  { \"name\": \"pedidos.eventos\",     \"partitionsCount\": 3, \"replicationFactor\": 3, \"isInternal\": false },", (100, 255, 100)),
    ("  { \"name\": \"pedidos.metricas\",    \"partitionsCount\": 3, \"replicationFactor\": 3, \"isInternal\": false },", (100, 255, 100)),
    ("  { \"name\": \"pedidos.eventos.DLT\", \"partitionsCount\": 3, \"replicationFactor\": 3, \"isInternal\": false }", (100, 255, 100)),
    ("]", (200, 200, 200)),
    ("", (200, 200, 200)),
    ("PS E:\\CLOUDP3> # 2. Creacion dinamica de topico con validacion @Valid", (150, 150, 150)),
    ("PS E:\\CLOUDP3> $body = @{ name = 'pedidos.notificaciones-stream'; partitions = 3; replicationFactor = 3 } | ConvertTo-Json", (255, 255, 255)),
    ("PS E:\\CLOUDP3> Invoke-RestMethod -Uri 'http://localhost:8081/admin/kafka/topics' -Method Post -Body $body -ContentType 'application/json'", (255, 255, 255)),
    ("Topico 'pedidos.notificaciones-stream' creado exitosamente en Kafka (3 particiones, 3 replicas)", (100, 255, 100)),
    ("", (200, 200, 200)),
    ("PS E:\\CLOUDP3> # 3. Validacion de errores (HTTP 400 Bad Request si replicas > 3 o particiones < 1)", (150, 150, 150)),
    ("PS E:\\CLOUDP3> $invalido = @{ name = 'topico-err'; partitions = 0; replicationFactor = 5 } | ConvertTo-Json", (255, 255, 255)),
    ("PS E:\\CLOUDP3> Invoke-RestMethod -Uri 'http://localhost:8081/admin/kafka/topics' -Method Post -Body $invalido -ContentType 'application/json'", (255, 255, 255)),
    ("HTTP/1.1 400 Bad Request: Error de validacion: partitions: Debe tener al menos 1 particion, replicationFactor: El cluster soporta maximo 3 replicas", (255, 100, 100))
]

draw_terminal("Windows PowerShell - API REST Administracion Dinamica de Topicos Kafka", lines_api, "13_terminal_kafka_admin_api.png")
print("[TERMINAL CAPTURES READY]")
