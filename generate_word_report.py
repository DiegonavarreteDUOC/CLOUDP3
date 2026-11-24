import os
import shutil
from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.oxml import OxmlElement, parse_xml
from docx.oxml.ns import nsdecls, qn

DOCX_PATH = r"E:\Estudios\Desarrollo cloud\prueba3\Informe_Evidencias_EP5_Kafka.docx"
CAPTURA_DIR = r"E:\Estudios\Desarrollo cloud\prueba3\CLOUDP3\capturas"
ARTIFACT_DIR = r"C:\Users\Admin\.gemini\antigravity\brain\535a42be-2a7f-4521-8f33-dc9873c95bc5"

doc = Document()

# Set standard margins
for section in doc.sections:
    section.top_margin = Inches(1)
    section.bottom_margin = Inches(1)
    section.left_margin = Inches(1)
    section.right_margin = Inches(1)

def set_cell_background(cell, fill_hex):
    tcPr = cell._tc.get_or_add_tcPr()
    tcPr.append(parse_xml(f'<w:shd {nsdecls("w")} w:fill="{fill_hex}"/>'))

def add_header(title, level=1):
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(14)
    p.paragraph_format.space_after = Pt(6)
    p.paragraph_format.keep_with_next = True
    run = p.add_run(title)
    run.bold = True
    if level == 1:
        run.font.size = Pt(16)
        run.font.color.rgb = RGBColor(24, 76, 120) # Blue
    elif level == 2:
        run.font.size = Pt(13)
        run.font.color.rgb = RGBColor(40, 116, 166)
    else:
        run.font.size = Pt(11.5)
        run.font.color.rgb = RGBColor(50, 50, 50)
    return p

def add_callout(text, title="Punto Clave de Arquitectura:"):
    tbl = doc.add_table(rows=1, cols=1)
    tbl.alignment = WD_TABLE_ALIGNMENT.CENTER
    cell = tbl.cell(0, 0)
    cell.width = Inches(6.5)
    set_cell_background(cell, "F0F4F8")
    p = cell.paragraphs[0]
    p.paragraph_format.space_before = Pt(4)
    p.paragraph_format.space_after = Pt(4)
    r_title = p.add_run(f"📌 {title} ")
    r_title.bold = True
    r_title.font.color.rgb = RGBColor(24, 76, 120)
    r_text = p.add_run(text)
    r_text.font.size = Pt(9.5)
    doc.add_paragraph().paragraph_format.space_after = Pt(4)

def add_image_evidence(img_name, caption):
    img_path = os.path.join(CAPTURA_DIR, img_name)
    if os.path.exists(img_path):
        p_img = doc.add_paragraph()
        p_img.alignment = WD_ALIGN_PARAGRAPH.CENTER
        p_img.paragraph_format.space_before = Pt(8)
        p_img.paragraph_format.space_after = Pt(3)
        p_img.paragraph_format.keep_with_next = True
        run_img = p_img.add_run()
        run_img.add_picture(img_path, width=Inches(6.2))
        
        p_cap = doc.add_paragraph()
        p_cap.alignment = WD_ALIGN_PARAGRAPH.CENTER
        p_cap.paragraph_format.space_after = Pt(10)
        run_cap = p_cap.add_run(f"Evidencia Visual: {caption}")
        run_cap.font.size = Pt(8.5)
        run_cap.font.italic = True
        run_cap.font.color.rgb = RGBColor(100, 100, 100)
    else:
        p_err = doc.add_paragraph(f"[Imagen no encontrada: {img_name}]")
        p_err.runs[0].font.color.rgb = RGBColor(200, 0, 0)

# ========================================================
# PORTADA Y TITULO
# ========================================================
p_main = doc.add_paragraph()
p_main.alignment = WD_ALIGN_PARAGRAPH.CENTER
p_main.paragraph_format.space_before = Pt(18)
p_main.paragraph_format.space_after = Pt(4)
r_inst = p_main.add_run("DUOC UC - ESCUELA DE INFORMÁTICA Y TELECOMUNICACIONES\n")
r_inst.bold = True
r_inst.font.size = Pt(12)
r_inst.font.color.rgb = RGBColor(70, 70, 70)

r_title = p_main.add_run("INFORME DE EVIDENCIAS TÉCNICAS - ENCARGO 5 (EP5)\n")
r_title.bold = True
r_title.font.size = Pt(20)
r_title.font.color.rgb = RGBColor(24, 76, 120)

r_sub = p_main.add_run("Plataforma de Event Streaming con Apache Kafka, RabbitMQ en Cluster y Microservicios Spring Boot\n")
r_sub.font.size = Pt(13)
r_sub.font.color.rgb = RGBColor(40, 116, 166)

p_meta = doc.add_paragraph()
p_meta.alignment = WD_ALIGN_PARAGRAPH.CENTER
p_meta.paragraph_format.space_after = Pt(20)
p_meta.add_run("Asignatura: Desarrollo Cloud Native I (DSY1107)\nEstudiante: Diego Navarrete\nFecha: Noviembre 2026 | Repositorio GitHub: DiegonavarreteDUOC/CLOUDP3\n").font.size = Pt(10)

# ========================================================
# 1. RESUMEN EJECUTIVO Y ARQUITECTURA HÍBRIDA
# ========================================================
add_header("1. Resumen Ejecutivo y Arquitectura Híbrida de Mensajería", 1)

doc.add_paragraph(
    "El presente informe documenta la implementación del Encargo 5 (EP5), correspondiente a la incorporación "
    "de Apache Kafka como plataforma de Event Streaming y Analítica en tiempo real dentro del ecosistema de microservicios "
    "Cloud Native previamente desarrollado (Java 21, Spring Boot, RabbitMQ en Cluster, AWS SQS simulado y autenticación AWS Cognito). "
    "El objetivo central radica en orquestar una arquitectura de mensajería híbrida donde coexistan armónicamente "
    "el procesamiento transaccional de colas (RabbitMQ) y el flujo continuo e inmutable de eventos de negocio (Apache Kafka)."
)

add_callout(
    "Coexistencia Tecnológica: RabbitMQ actúa como message broker tradicional para tareas puntuales y enrutamiento transaccional "
    "(pedidos, notificaciones, reportes con manual ACK y DLQ), mientras Apache Kafka actúa como registro distribuido (commit log) "
    "para eventos de dominio de alta concurrencia, analítica en vivo y auditoría histórica multi-consumidor.",
    "Fundamento Arquitectónico"
)

# ========================================================
# 2. INDICADOR 1: INFRAESTRUCTURA DE CLUSTER APACHE KAFKA
# ========================================================
add_header("2. Indicador 1: Despliegue de Apache Kafka (Cluster de 3 Brokers)", 1)

doc.add_paragraph(
    "Para garantizar alta disponibilidad, tolerancia a particiones de red y el cumplimiento estricto del factor de replicación 3 "
    "requerido en la pauta de evaluación, se implementó un clúster distribuido compuesto por Zookeeper y 3 brokers de Kafka (kafka1, kafka2, kafka3), "
    "orquestados mediante Docker Compose y expuestos tanto en la red interna bridge como en los puertos host 9092, 9093 y 9094. "
    "Se integró la herramienta de observabilidad Kafka-UI en el puerto 8085."
)

add_image_evidence("01_kafka_cluster_brokers.png", "Kafka-UI: Clúster de 3 Brokers (kafka1, kafka2, kafka3) 100% operativos y sincronizados")

doc.add_paragraph(
    "Cada broker mantiene su propia configuración de listeners (PLAINTEXT interno para comunicación inter-broker y PLAINTEXT_HOST para clientes externos), "
    "garantizando que cualquier fallo o caída de un nodo permita la continuidad operacional inmediata gracias al mecanismo de In-Sync Replicas (ISR)."
)

# ========================================================
# 3. INDICADOR 2: TÓPICOS, PARTICIONES Y POLÍTICAS DE RETENCIÓN
# ========================================================
add_header("3. Indicador 2: Configuración de Tópicos (3 Particiones y 3 Réplicas)", 1)

doc.add_paragraph(
    "Siguiendo los requerimientos de la rúbrica y la presentación EP6, se declararon de forma programática mediante Spring Boot y AdminClient "
    "los siguientes tópicos de negocio, cada uno configurado con 3 particiones y un factor de replicación de 3:"
)

tbl_topicos = doc.add_table(rows=4, cols=4)
tbl_topicos.alignment = WD_TABLE_ALIGNMENT.CENTER
headers = ["Nombre del Tópico", "Particiones", "Réplicas", "Política de Retención y Uso"]
for i, h in enumerate(headers):
    c = tbl_topicos.cell(0, i)
    set_cell_background(c, "184C78")
    p = c.paragraphs[0]
    r = p.add_run(h)
    r.bold = True
    r.font.color.rgb = RGBColor(255, 255, 255)
    r.font.size = Pt(9.5)

data_topicos = [
    ("pedidos.eventos", "3", "3", "retention.ms=86.400.000 (24 hrs) | Ciclo de vida y streaming de pedidos"),
    ("pedidos.metricas", "3", "3", "retention.ms=604.800.000 (7 días) | Telemetría operacional y latencias"),
    ("pedidos.eventos.DLT", "3", "3", "retention.ms=604.800.000 (7 días) | Dead Letter Topic para errores y poison pills")
]

for row_idx, row in enumerate(data_topicos, start=1):
    for col_idx, val in enumerate(row):
        c = tbl_topicos.cell(row_idx, col_idx)
        if row_idx % 2 == 0:
            set_cell_background(c, "F5F8FA")
        p = c.paragraphs[0]
        r = p.add_run(val)
        r.font.size = Pt(8.5)

doc.add_paragraph().paragraph_format.space_after = Pt(6)

add_image_evidence("02_kafka_topics_list.png", "Kafka-UI: Listado de tópicos confirmando 3 particiones y factor de réplica 3 por cada tópico")
add_image_evidence("03_kafka_topic_eventos_overview.png", "Kafka-UI: Detalle del tópico 'pedidos.eventos' mostrando la distribución de líderes en Brokers 1, 2 y 3")

# ========================================================
# 4. INDICADOR 3: PRODUCTORES DE EVENTOS Y PARTICIONAMIENTO
# ========================================================
add_header("4. Indicador 3: Productores de Eventos en Spring Boot y Claves de Partición", 1)

doc.add_paragraph(
    "Se diseñó la clase PedidoKafkaProducer utilizando KafkaTemplate<String, Object>. Para asegurar la idempotencia y confiabilidad, "
    "se habilitó 'enable.idempotence=true' y 'acks=all'. Además, un aspecto crítico de la solución es la estrategia de asignación de claves: "
    "se utiliza el ID del pedido (pedidoId) como 'Partition Key'. De esta manera, el algoritmo de particionamiento por hash asegura que todos los eventos "
    "asociados a un mismo pedido (creación, pago, despacho, anulación) aterricen en la misma partición física, garantizando orden estricto (FIFO por clave) "
    "sin sacrificar el paralelismo general del clúster."
)

add_image_evidence("04_kafka_topic_eventos_messages.png", "Kafka-UI: Mensajes en 'pedidos.eventos' distribuidos en particiones 0, 1 y 2 con sus respectivas claves")

# ========================================================
# 5. INDICADOR 4: GRUPOS DE CONSUMIDORES INDEPENDIENTES Y OFFSETS
# ========================================================
add_header("5. Indicador 4: Grupos de Consumidores Independientes y Manejo de Offsets", 1)

doc.add_paragraph(
    "A diferencia de RabbitMQ donde el consumo es destructivo (un mensaje consumido y confirmado desaparece de la cola), en Kafka el evento "
    "permanece inmutable en el log según la política de retención. Esto permite que múltiples subsistemas lean el mismo flujo de datos a ritmos "
    "completamente independientes, cada uno gestionando su propio puntero de lectura (Offset):"
)

doc.add_paragraph(
    "• grupo-analitica-ventas: Consume 'pedidos.eventos' para calcular proyecciones comerciales y unidades vendidas en tiempo real.\n"
    "• grupo-auditoria-tiempo-real: Consume 'pedidos.eventos' en paralelo para generar pistas de auditoría y trazabilidad de seguridad.\n"
    "• grupo-metricas-stream: Consume 'pedidos.metricas' para monitorear SLAs y tiempos de procesamiento del servicio.\n"
    "• grupo-dlt-monitoring: Monitorea el Dead Letter Topic para alertas de fallos en producción."
)

add_image_evidence("05_kafka_consumer_groups.png", "Kafka-UI: Lista de Consumer Groups registrados y en estado STABLE")
add_image_evidence("06_kafka_consumer_group_detail.png", "Kafka-UI: Detalle de 'grupo-analitica-ventas' con Offsets por partición y Consumer Lag = 0")

# ========================================================
# 6. INDICADOR 5: ORQUESTACIÓN HÍBRIDA (RABBITMQ + KAFKA)
# ========================================================
add_header("6. Indicador 5: Orquestación Híbrida de Mensajería", 1)

doc.add_paragraph(
    "Para cumplir a cabalidad con la arquitectura cloud native, se mantuvieron y validaron los componentes de RabbitMQ configurados en Prueba 2. "
    "El cluster RabbitMQ de 2 nodos (rabbit@rabbitmq1 y rabbit@rabbitmq2) opera simultáneamente procesando el enrutamiento directo y de tópico "
    "hacia las colas 'pedidos', 'notificaciones' y 'reportes', respaldadas por sus correspondientes colas de cartas muertas (DLQs) y políticas HA."
)

add_image_evidence("09_rabbitmq_cluster_overview.png", "RabbitMQ Management: Clúster de 2 Nodos activos y saludables en simultáneo")
add_image_evidence("11_rabbitmq_queues_dlqs.png", "RabbitMQ Management: 3 Colas transaccionales + 3 Dead Letter Queues configuradas")

# ========================================================
# 7. INDICADOR 6: RESILIENCIA, RETRIES Y DEAD LETTER TOPIC (DLT)
# ========================================================
add_header("7. Indicador 6: Manejo de Errores, Resiliencia y Dead Letter Topic (DLT)", 1)

doc.add_paragraph(
    "En sistemas de Event Streaming, un mensaje malformado o 'Poison Pill' puede bloquear la partición completa si el consumidor entra en un bucle infinito de reintentos. "
    "Para mitigar este riesgo, se configuró un DefaultErrorHandler con DeadLetterPublishingRecoverer y una política de retroceso fijo (FixedBackOff de 2 reintentos con intervalo de 1 seg). "
    "Cuando un registro supera los intentos máximos, se publica automáticamente en el tópico 'pedidos.eventos.DLT' preservando los encabezados de diagnóstico "
    "(tópico de origen, partición original, offset original y excepción capturada)."
)

add_image_evidence("07_kafka_topic_dlt_messages.png", "Kafka-UI: Mensaje venenoso (POISON_PILL) aislado exitosamente en el Dead Letter Topic")
add_image_evidence("12_terminal_kafka_logs.png", "Terminal: Registro de reintentos automáticos y recuperación por parte del consumidor DLT")

# ========================================================
# 8. INDICADOR 7: ADMINISTRACIÓN REST Y COMPARATIVA TÉCNICA
# ========================================================
add_header("8. Indicador 7: Microservicio de Administración y Comparativa de Arquitectura", 1)

doc.add_paragraph(
    "El microservicio administrador fue ampliado para incluir soporte integral de Kafka mediante endpoints REST expuestos en '/admin/kafka/topics'. "
    "Permite listar tópicos existentes, crear nuevos tópicos con validación estricta (@Valid en número de particiones y factor de réplica) "
    "y eliminar tópicos dinámicamente. Cuando un cliente intenta ingresar valores fuera de rango (como réplicas superiores al número de brokers), "
    "el servicio responde con HTTP 400 Bad Request y un mensaje descriptivo."
)

add_image_evidence("13_terminal_kafka_admin_api.png", "Terminal: Invocación de API REST de administración Kafka y validación de errores HTTP 400")

doc.add_paragraph(
    "A continuación se sintetiza la justificación técnica de la selección de cada tecnología de mensajería implementada:"
)

tbl_comp = doc.add_table(rows=5, cols=4)
tbl_comp.alignment = WD_TABLE_ALIGNMENT.CENTER
headers_comp = ["Criterio de Evaluación", "Apache Kafka", "RabbitMQ (Cluster)", "AWS SQS"]
for i, h in enumerate(headers_comp):
    c = tbl_comp.cell(0, i)
    set_cell_background(c, "184C78")
    p = c.paragraphs[0]
    r = p.add_run(h)
    r.bold = True
    r.font.color.rgb = RGBColor(255, 255, 255)
    r.font.size = Pt(9)

data_comp = [
    ("Modelo de Mensajería", "Log append-only distribuido (Pull)", "Smart broker / Dumb consumer (Push)", "Cola gestionada simple (Poll)"),
    ("Persistencia del Mensaje", "Inmutable y configurable (días/meses)", "Se elimina tras confirmación (ACK)", "Temporal (hasta 14 días)"),
    ("Múltiples Consumidores", "Excelente (Consumer Groups paralelos)", "Requiere exchanges fanout/topic", "Requiere SNS Fanout"),
    ("Caso de Uso en la Solución", "Analítica en tiempo real y Event Sourcing", "Enrutamiento transaccional de negocio", "Buffer desacoplado en la nube")
]

for row_idx, row in enumerate(data_comp, start=1):
    for col_idx, val in enumerate(row):
        c = tbl_comp.cell(row_idx, col_idx)
        if row_idx % 2 == 0:
            set_cell_background(c, "F5F8FA")
        p = c.paragraphs[0]
        r = p.add_run(val)
        r.font.size = Pt(8)

# ========================================================
# 9. CONCLUSIÓN Y ENLACES DE ENTREGA
# ========================================================
add_header("9. Conclusiones y Datos de Entrega", 1)

doc.add_paragraph(
    "La incorporación de Apache Kafka enriqueció la arquitectura microservicios, transformándola en una plataforma "
    "completamente reactiva y orientada a eventos (Event-Driven Architecture). La combinación de RabbitMQ para tareas puntuales "
    "con garantías transaccionales y Kafka para el streaming de datos masivos representa el estándar de la industria para aplicaciones "
    "cloud native de alta escala."
)

doc.add_paragraph(
    "• Repositorio Oficial en GitHub: https://github.com/DiegonavarreteDUOC/CLOUDP3\n"
    "• Infraestructura Docker: 3 Brokers Kafka + 1 Zookeeper + 1 Kafka-UI + 2 Nodos RabbitMQ + 1 SQS ElasticMQ (8 Contenedores).\n"
    "• Tópicos Verificados: pedidos.eventos (3p/3r), pedidos.metricas (3p/3r), pedidos.eventos.DLT (3p/3r)."
)

doc.save(DOCX_PATH)
print(f"[OK] Documento Word generado en: {DOCX_PATH}")

# Copy to artifacts
dst_art = os.path.join(ARTIFACT_DIR, "Informe_Evidencias_EP5_Kafka.docx")
shutil.copy2(DOCX_PATH, dst_art)
print(f"[OK] Copiado a directorio de artefactos: {dst_art}")
