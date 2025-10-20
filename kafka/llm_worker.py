import os
import json
import time
from typing import Dict, Any

# Librerías Externas
from confluent_kafka import Consumer, Producer
# ⚠️ Asegúrate de que las credenciales y conexiones sean correctas para tu Tarea ⚠️
from google import genai
import psycopg2
import redis

# ======================================================================
# 1. CONFIGURACIÓN DESDE VARIABLES DE ENTORNO
# ======================================================================
# (Todas estas variables se definieron en el docker-compose.yml)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "llm_requests")
KAFKA_RESPONSE_TOPIC = os.getenv("KAFKA_RESPONSES_TOPIC", "llm_responses")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "llm_dlq")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
REDIS_HOST = os.getenv("REDIS_HOST", "redis_cache")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin123")
POSTGRES_DB = os.getenv("POSTGRES_DB", "mydatabase")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AIzaSy...")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

GROUP_ID = "llm-processor-group"

# ======================================================================
# 2. CLIENTES
# ======================================================================

def get_llm_client():
    """Inicializa el cliente Gemini."""
    if not GEMINI_API_KEY or GEMINI_API_KEY == "AIzaSy...":
        print("❌ ADVERTENCIA: Usando cliente Gemini simulado. Reemplaza la clave.")
        return None 
    try:
        return genai.Client(api_key=GEMINI_API_KEY)
    except Exception as e:
        print(f"❌ Error al iniciar Gemini Client: {e}")
        return None

def initialize_db_connections():
    """Inicializa conexiones a PostgreSQL y Redis."""
    try:
        # PostgreSQL
        pg_conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        # Redis
        redis_conn = redis.Redis(host=REDIS_HOST, decode_responses=True)
        redis_conn.ping() # Prueba la conexión
        print(f"✅ Conexiones a DB (PG, Redis) establecidas.")
        return pg_conn, redis_conn
    except Exception as e:
        print(f"❌ Error al conectar a DB/Cache: {e}")
        # En un worker real, podrías reintentar o simplemente loguear el fallo.
        return None, None


# ======================================================================
# 3. LÓGICA DEL WORKER
# ======================================================================

def call_llm_api(prompt: str, llm_client: Any) -> str:
    """Llama a la API de Gemini o usa una simulación."""
    if llm_client:
        try:
            response = llm_client.models.generate_content(
                model=GEMINI_MODEL, 
                contents=prompt
            )
            return response.text
        except Exception as e:
            print(f"❌ Error en la llamada a Gemini: {e}")
            return "ERROR_LLM_API_CALL"
    else:
        # Simulación si la API Key es incorrecta o el cliente falló al iniciar
        time.sleep(1) 
        return f"SIMULACIÓN: Respuesta de calidad para el prompt '{prompt[:50]}...'"

def process_message(msg_value: bytes, producer: Producer, llm_client: Any, pg_conn: Any, redis_conn: Any):
    """Procesa un mensaje recibido, llama al LLM y produce la respuesta."""
    try:
        input_data: Dict[str, Any] = json.loads(msg_value.decode('utf-8'))
        
        request_id = input_data.get('id', int(time.time() * 1000))
        user_prompt = input_data.get('prompt', 'Pregunta sin texto')
        
        print(f"➡️ Procesando solicitud ID {request_id}. Prompt: {user_prompt[:30]}...")
        
        # 1. Llamada al LLM
        llm_response_text = call_llm_api(user_prompt, llm_client)
        
        # 2. Construir mensaje de salida para Flink (tópico llm_responses)
        output_data = {
            'id': request_id,
            'original_prompt': user_prompt,
            'llm_answer': llm_response_text,
            'timestamp': time.time()
        }
        
        # 3. Producir respuesta para el tópico de Flink
        producer.produce(
            topic=KAFKA_RESPONSE_TOPIC,
            value=json.dumps(output_data).encode('utf-8'),
            key=str(request_id).encode('utf-8')
        )
        producer.flush()
        
        print(f"✅ Solicitud ID {request_id} completada y enviada a Flink.")
        
        # 4. Opcional: Almacenar en PostgreSQL
        # if pg_conn:
        #     # Lógica para insertar el resultado en la tabla de PostgreSQL
        #     pass

    except json.JSONDecodeError:
        print(f"⚠️ Error al decodificar JSON. Mensaje enviado a DLQ: {msg_value}")
        producer.produce(topic=KAFKA_DLQ_TOPIC, value=msg_value)
        producer.flush()
    except Exception as e:
        print(f"❌ Error fatal al procesar la solicitud: {e}. Enviando a DLQ.")
        producer.produce(topic=KAFKA_DLQ_TOPIC, value=msg_value)
        producer.flush()

# ======================================================================
# 4. BUCLE PRINCIPAL
# ======================================================================

if __name__ == "__main__":
    consumer = initialize_kafka_consumer()
    producer = initialize_kafka_producer()
    llm_client = get_llm_client()
    pg_conn, redis_conn = initialize_db_connections()

    try:
        while True:
            msg = consumer.poll(1.0) 

            if msg is None:
                continue
            if msg.error():
                if msg.error().fatal():
                    print(f"Fatal error: {msg.error()}")
                    break
                continue

            process_message(msg.value(), producer, llm_client, pg_conn, redis_conn)
            
            # Commit para confirmar que el mensaje fue procesado
            consumer.commit(message=msg, asynchronous=True)
            
    except KeyboardInterrupt:
        print("\nWorker detenido por el usuario.")
    finally:
        consumer.close()
        producer.flush()
        if pg_conn:
            pg_conn.close()
        print("Conexiones cerradas. Worker finalizado.")