import hashlib
import json
import os
import uuid
from typing import Optional, Literal

import psycopg2
import redis
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# --- CONFIGURACIÓN ---
# Postgres
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "mydatabase")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin123")

# Redis
REDIS_HOST = os.getenv("REDIS_HOST", "redis_cache")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "604800"))

# Configuración de Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "llm_requests") # Tópico de entrada de la pregunta
# Tópico de salida final de Flink/Respuesta Writer
KAFKA_VALIDATION_TOPIC = os.getenv("KAFKA_VALIDATION_TOPIC", "validation_results") 

KAFKA_PRODUCER_TIMEOUT_MS = 5000 

app = FastAPI(title="QA LLM API", version="1.0")

# --- Modelos Pydantic ---

class Row(BaseModel):
    score: int = Field(ge=0, le=10)
    title: str
    body: Optional[str] = None
    answer: str

class AskRequest(BaseModel):
    question: str = Field(..., min_length=5, example="¿Qué son los sistemas distribuidos?")

class AskResponse(BaseModel):
    status: Literal["accepted", "completed", "error"]
    source: Literal["cache", "db", "queue", "error"]
    row: Row
    request_id: str
    message: Optional[str] = None

# --- Conexiones y Utilidades ---

_conexion_db = None
def obtener_conexion_db():
    """Obtiene (y mantiene) una conexión global a Postgres."""
    global _conexion_db
    if _conexion_db is None or getattr(_conexion_db, "closed", 1) != 0:
        try:
            _conexion_db = psycopg2.connect(
                host=POSTGRES_HOST,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                connect_timeout=5
            )
            _conexion_db.autocommit = True
        except Exception:
             # Si no se puede conectar a DB, lanzamos la excepción para que el healthcheck/endpoint falle.
             raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Database connection failed.")
    return _conexion_db

_kafka_producer = None
def obtener_productor_kafka():
    """Inicializa o retorna el productor de Kafka."""
    global _kafka_producer
    if _kafka_producer is None:
        try:
            _kafka_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(0, 10),
                request_timeout_ms=KAFKA_PRODUCER_TIMEOUT_MS,
                retries=3
            )
        except (NoBrokersAvailable, Exception):
            return None
    return _kafka_producer

redis_cliente = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def clave_cache_para(pregunta: str) -> str:
    """Genera la clave de caché usando hash SHA256 de la pregunta normalizada."""
    normalizada = " ".join(pregunta.strip().lower().split())
    hash_hex = hashlib.sha256(normalizada.encode("utf-8")).hexdigest()
    return f"qa:{hash_hex}"

def buscar_en_cache(pregunta: str) -> Optional[Row]:
    """Busca una respuesta en Redis."""
    try:
        data = redis_cliente.get(clave_cache_para(pregunta))
        if data:
            return Row.model_validate_json(data)
    except Exception as e:
        print(f"Error al buscar en Redis: {e}")
    return None

def buscar_en_db(pregunta: str) -> Optional[Row]:
    """Busca una respuesta en PostgreSQL."""
    try:
        obtener_conexion_db()
        with _conexion_db.cursor() as cursor:
            cursor.execute(
                """
                SELECT score, title, body, answer 
                FROM querys 
                WHERE LOWER(title) = LOWER(%s)
                ORDER BY score DESC 
                LIMIT 1
                """,
                (pregunta,)
            )
            resultado = cursor.fetchone()
            if resultado:
                return Row(score=resultado[0], title=resultado[1], body=resultado[2], answer=resultado[3])
    except Exception as e:
        print(f"Error al buscar en Postgres: {e}")
    return None

# --- Endpoints ---

@app.get("/health", response_model=dict)
def health():
    """Verifica el estado de todos los servicios (Redis, Postgres, Kafka)."""
    estado_ok = True
    mensajes = {}
    
    # Chequeo 1: Redis
    try:
        redis_cliente.ping()
        mensajes["redis"] = "OK"
    except Exception:
        mensajes["redis"] = "ERROR (Verifica redis_cache:6379)"
        estado_ok = False
        
    # Chequeo 2: Postgres
    try:
        obtener_conexion_db().cursor().execute("SELECT 1")
        mensajes["postgres"] = "OK"
    except Exception:
        mensajes["postgres"] = "ERROR (Verifica postgres:5432)"
        estado_ok = False
        
    # Chequeo 3: Kafka
    kafka = obtener_productor_kafka()
    kafka_status = "ERROR (Verifica el broker:29092)"
    if kafka:
        try:
            # Chequea los dos tópicos principales del pipeline
            kafka.partitions_for_topic(KAFKA_TOPIC) 
            kafka.partitions_for_topic(KAFKA_VALIDATION_TOPIC) 
            mensajes["kafka"] = "OK"
        except Exception:
            mensajes["kafka"] = kafka_status
            estado_ok = False
    else:
        estado_ok = False
        mensajes["kafka"] = kafka_status

    return {"ok": estado_ok, "services": mensajes}


@app.post("/ask", response_model=AskResponse)
def ask_question(request: AskRequest):
    """
    Endpoint principal. Intenta Cache -> DB -> Kafka Queue (Asíncrono).
    """
    pregunta = request.question.strip()
    
    # 1. Búsqueda en caché (Redis)
    respuesta_cache = buscar_en_cache(pregunta)
    if respuesta_cache:
        print("Encontrado en Cache.")
        return AskResponse(
            status="completed",
            source="cache",
            row=respuesta_cache,
            request_id="cached",
            message="Respuesta recuperada desde la caché."
        )

    # 2. Búsqueda en base de datos (Postgres)
    respuesta_db = buscar_en_db(pregunta)
    if respuesta_db:
        print("Encontrado en DB. Escribiendo a Cache.")
        # Escribir en caché para futuras consultas
        try:
            redis_cliente.setex(clave_cache_para(pregunta), CACHE_TTL_SECONDS, respuesta_db.model_dump_json())
        except Exception as e:
            print(f"Error al escribir en caché después de consulta DB: {e}")
            
        return AskResponse(
            status="completed",
            source="db",
            row=respuesta_db,
            request_id="db_read",
            message="Respuesta recuperada desde la base de datos."
        )

    # 3. Enviar a la cola (Kafka) si no se encontró
    kafka_prod = obtener_productor_kafka()
    if not kafka_prod:
        # 🚨 CORRECCIÓN DE INDENTACIÓN APLICADA AQUÍ
        raise HTTPException(status_code=503, detail="El servicio de mensajería (Kafka) no está disponible.")

    # Generar un ID único para trazas en el worker
    request_id = str(uuid.uuid4())
    
    print(f"Pregunta nueva. Enviando a Kafka (ID: {request_id}) y respondiendo inmediatamente...")

    try:
        # A. Producir el mensaje en Kafka (Tópico de entrada)
        kafka_prod.send(
            KAFKA_TOPIC, 
            {'question': pregunta, 'request_id': request_id}
        ) 
        # Aseguramos el envío al vaciar el buffer
        kafka_prod.flush(timeout=KAFKA_PRODUCER_TIMEOUT_MS) 
        
    except KafkaError as e:
        print(f"Error CRÍTICO al enviar a Kafka: {e}")
        raise HTTPException(status_code=503, detail="El broker de Kafka no aceptó el mensaje. Falló el encolamiento.")
    except Exception as e:
        print(f"Error desconocido al enviar a Kafka: {e}")
        raise HTTPException(status_code=503, detail="Fallo de conexión con Kafka.")


    # B. Retornar la respuesta de ACEPTACIÓN (Asíncrono)
    fila_en_cola = Row(
        score=0, 
        title=pregunta, 
        body=None, 
        answer="La respuesta está siendo generada y validada de forma asíncrona. Vuelva a consultar usando la misma pregunta en breve."
    )
    
    return AskResponse(
        status="accepted",
        source="queue",
        row=fila_en_cola,
        request_id=request_id,
        message="Solicitud encolada exitosamente para procesamiento asíncrono."
    )