import time
import os
import shutil
from playwright.sync_api import sync_playwright

OUTPUT_DIR = r"E:\Estudios\Desarrollo cloud\prueba3\CLOUDP3\capturas"
ARTIFACT_DIR = r"C:\Users\Admin\.gemini\antigravity\brain\535a42be-2a7f-4521-8f33-dc9873c95bc5"

os.makedirs(OUTPUT_DIR, exist_ok=True)

def copy_to_artifacts(src_filename):
    src = os.path.join(OUTPUT_DIR, src_filename)
    dst = os.path.join(ARTIFACT_DIR, src_filename)
    if os.path.exists(src):
        shutil.copy2(src, dst)
        print(f"Copied to artifact dir: {src_filename}")

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    context = browser.new_context(viewport={'width': 1600, 'height': 900})
    page = context.new_page()

    print("=== CAPTURANDO KAFKA-UI ===")

    # 1. Kafka Cluster Overview / Brokers
    try:
        page.goto("http://localhost:8085/ui/clusters/local-cluster/brokers", timeout=25000)
        page.wait_for_selector("table", timeout=15000)
        page.wait_for_timeout(2000)
        fn = "01_kafka_cluster_brokers.png"
        page.screenshot(path=os.path.join(OUTPUT_DIR, fn), full_page=False)
        print(f"OK: {fn}")
        copy_to_artifacts(fn)
    except Exception as e:
        print(f"Error brokers: {e}")

    # 2. Kafka Topics List (3 particiones, 3 replicas)
    try:
        page.goto("http://localhost:8085/ui/clusters/local-cluster/all-topics", timeout=25000)
        page.wait_for_selector("table", timeout=15000)
        page.wait_for_timeout(2000)
        fn = "02_kafka_topics_list.png"
        page.screenshot(path=os.path.join(OUTPUT_DIR, fn), full_page=False)
        print(f"OK: {fn}")
        copy_to_artifacts(fn)
    except Exception as e:
        print(f"Error topics: {e}")

    # 3. Kafka Topic Detail: pedidos.eventos
    try:
        page.goto("http://localhost:8085/ui/clusters/local-cluster/all-topics/pedidos.eventos", timeout=25000)
        page.wait_for_selector("table", timeout=15000)
        page.wait_for_timeout(2000)
        fn = "03_kafka_topic_eventos_overview.png"
        page.screenshot(path=os.path.join(OUTPUT_DIR, fn), full_page=False)
        print(f"OK: {fn}")
        copy_to_artifacts(fn)
    except Exception as e:
        print(f"Error topic eventos: {e}")

    # 4. Kafka Topic Messages: pedidos.eventos (Mensajes particionados y claves)
    try:
        page.goto("http://localhost:8085/ui/clusters/local-cluster/all-topics/pedidos.eventos/messages", timeout=25000)
        page.wait_for_selector("table", timeout=15000)
        page.wait_for_timeout(2500)
        fn = "04_kafka_topic_eventos_messages.png"
        page.screenshot(path=os.path.join(OUTPUT_DIR, fn), full_page=False)
        print(f"OK: {fn}")
        copy_to_artifacts(fn)
    except Exception as e:
        print(f"Error messages: {e}")

    # 5. Kafka Consumer Groups (grupos activos, lag, offsets)
    try:
        page.goto("http://localhost:8085/ui/clusters/local-cluster/consumer-groups", timeout=25000)
        page.wait_for_selector("table", timeout=15000)
        page.wait_for_timeout(2000)
        fn = "05_kafka_consumer_groups.png"
        page.screenshot(path=os.path.join(OUTPUT_DIR, fn), full_page=False)
        print(f"OK: {fn}")
        copy_to_artifacts(fn)
    except Exception as e:
        print(f"Error consumer groups: {e}")

    # 6. Consumer Group Detail: grupo-analitica-ventas
    try:
        page.goto("http://localhost:8085/ui/clusters/local-cluster/consumer-groups/grupo-analitica-ventas", timeout=25000)
        page.wait_for_selector("table", timeout=15000)
        page.wait_for_timeout(2000)
        fn = "06_kafka_consumer_group_detail.png"
        page.screenshot(path=os.path.join(OUTPUT_DIR, fn), full_page=False)
        print(f"OK: {fn}")
        copy_to_artifacts(fn)
    except Exception as e:
        print(f"Error consumer group detail: {e}")

    # 7. Topic DLT Messages: pedidos.eventos.DLT
    try:
        page.goto("http://localhost:8085/ui/clusters/local-cluster/all-topics/pedidos.eventos.DLT/messages", timeout=25000)
        page.wait_for_selector("table", timeout=15000)
        page.wait_for_timeout(2000)
        fn = "07_kafka_topic_dlt_messages.png"
        page.screenshot(path=os.path.join(OUTPUT_DIR, fn), full_page=False)
        print(f"OK: {fn}")
        copy_to_artifacts(fn)
    except Exception as e:
        print(f"Error DLT messages: {e}")

    # 8. Kafka Topic Metricas Overview
    try:
        page.goto("http://localhost:8085/ui/clusters/local-cluster/all-topics/pedidos.metricas", timeout=25000)
        page.wait_for_selector("table", timeout=15000)
        page.wait_for_timeout(2000)
        fn = "08_kafka_topic_metricas_overview.png"
        page.screenshot(path=os.path.join(OUTPUT_DIR, fn), full_page=False)
        print(f"OK: {fn}")
        copy_to_artifacts(fn)
    except Exception as e:
        print(f"Error topic metricas: {e}")

    print("\n=== CAPTURANDO RABBITMQ MANAGEMENT ===")

    # RabbitMQ Login & Overview
    try:
        page.goto("http://localhost:15672", timeout=25000)
        page.wait_for_timeout(1500)
        if page.locator("input[name='username']").is_visible():
            page.fill("input[name='username']", "guest")
            page.fill("input[name='password']", "guest")
            page.click("input[type='submit']")
            page.wait_for_timeout(3000)

        # 9. RabbitMQ Overview (Cluster 2 Nodos)
        fn = "09_rabbitmq_cluster_overview.png"
        page.screenshot(path=os.path.join(OUTPUT_DIR, fn), full_page=False)
        print(f"OK: {fn}")
        copy_to_artifacts(fn)

        # 10. RabbitMQ Nodes
        page.goto("http://localhost:15672/#/nodes", timeout=25000)
        page.wait_for_timeout(3000)
        fn = "10_rabbitmq_cluster_nodes.png"
        page.screenshot(path=os.path.join(OUTPUT_DIR, fn), full_page=False)
        print(f"OK: {fn}")
        copy_to_artifacts(fn)

        # 11. RabbitMQ Queues (Colas + DLQs)
        page.goto("http://localhost:15672/#/queues", timeout=25000)
        page.wait_for_timeout(3000)
        fn = "11_rabbitmq_queues_dlqs.png"
        page.screenshot(path=os.path.join(OUTPUT_DIR, fn), full_page=False)
        print(f"OK: {fn}")
        copy_to_artifacts(fn)

    except Exception as e:
        print(f"Error RabbitMQ: {e}")

    browser.close()
    print("\n[FIN] Capturas completadas con exito.")
