from pyflink.table import TableEnvironment, EnvironmentSettings, DataTypes, Schema, TableDescriptor

# 1. Configuración del entorno de Flink
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# Opcional: Configurar el paralelo y el tiempo de espera
t_env.get_config().set("execution.checkpointing.interval", "10s")
t_env.get_config().set("parallelism.default", "1")

# Nota: El conector Kafka JAR fue incluido en el Dockerfile (Paso 2)

# =====================================================================
# 2. DEFINICIÓN DE TABLA DE ENTRADA (CONSUMIDOR KAFKA)
# Topic: llm_responses (Viene del LLM Worker)
# =====================================================================
# Debes ajustar el Schema (DataTypes) para que coincida con lo que produce tu LLM Worker
t_env.create_temporary_table(
    'llm_responses_table',
    TableDescriptor.for_connector('kafka')
    .schema(
        Schema.new_builder()
        .column("id", DataTypes.BIGINT()) # Ejemplo de un ID
        .column("title", DataTypes.STRING()) # Ejemplo de un título
        .column("llm_answer", DataTypes.STRING()) # Respuesta del LLM
        .column_by_metadata("proc_time", DataTypes.TIMESTAMP_LTZ(3), 'proctime', is_physical=False) # Para procesamiento de tiempo
        .build()
    )
    .option('topic', 'llm_responses')
    .option('properties.bootstrap.servers', 'kafka:9092') # ⬅️ Dirección interna
    .option('properties.group.id', 'flink-qa-consumer-group')
    .option('format', 'json') # Ajusta esto a tu formato de serialización
    .option('scan.startup.mode', 'earliest-offset')
    .build()
)

# =====================================================================
# 3. DEFINICIÓN DE TABLA DE SALIDA (PRODUCTOR KAFKA)
# Topic: validation_results (Resultado del Job de Flink)
# =====================================================================
t_env.create_temporary_table(
    'validation_results_table',
    TableDescriptor.for_connector('kafka')
    .schema(
        Schema.new_builder()
        .column("request_id", DataTypes.BIGINT())
        .column("quality_score", DataTypes.INT())
        .column("validated_at", DataTypes.TIMESTAMP_LTZ(3))
        .build()
    )
    .option('topic', 'validation_results')
    .option('properties.bootstrap.servers', 'kafka:9092') # ⬅️ Dirección interna
    .option('format', 'json') # Ajusta esto a tu formato de serialización
    .build()
)

# =====================================================================
# 4. LÓGICA DE PROCESAMIENTO
# =====================================================================

# 4.1. Leer de la tabla de entrada
llm_responses_stream = t_env.from_path("llm_responses_table")

# 4.2. Aplicar lógica de calidad (Esto es un ejemplo conceptual)
# Aquí deberías registrar y usar tu UDF (User Defined Function)
results = llm_responses_stream.select(
    llm_responses_stream.id.alias("request_id"),
    # Aquí iría la lógica para calcular la puntuación
    (llm_responses_stream.id % 5 + 1).alias("quality_score"), # Ejemplo: un score aleatorio entre 1 y 5
    llm_responses_stream.proc_time.alias("validated_at")
)

# =====================================================================
# 5. EJECUCIÓN DEL JOB
# =====================================================================

# Insertar el resultado en la tabla de salida (validation_results)
results.execute_insert("validation_results_table").wait()

print("Job Flink de Calidad de Respuestas iniciado exitosamente.")