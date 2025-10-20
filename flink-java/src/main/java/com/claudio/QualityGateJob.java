package com.claudio;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class QualityGateJob {

    private static final String KAFKA_BROKERS = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "broker:29092");
    private static final String INPUT_TOPIC = System.getenv().getOrDefault("KAFKA_INPUT_TOPIC", "llm_responses");
    private static final String OUTPUT_TOPIC = System.getenv().getOrDefault("KAFKA_OUTPUT_TOPIC", "validation_results");

    public static void main(String[] args) throws Exception {
        // 1. Configurar el entorno de Flink Table API (modo streaming)
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        System.out.println("INFO: Configurando Job Flink 'Quality Gate' en Java...");

        // 2. Definir la tabla de entrada (Source) para llm_responses
        String createSourceTableSql = String.format(
                "CREATE TABLE llm_responses_table (\n" +
                "  request_id STRING,\n" +
                "  question STRING,\n" +
                "  answer STRING,\n" +
                "  retry_count INT\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '%s',\n" +
                "  'properties.bootstrap.servers' = '%s',\n" +
                "  'properties.group.id' = 'flink-response-consumer-group',\n" +
                "  'format' = 'json',\n" +
                "  'json.fail-on-missing-field' = 'false',\n" +
                "  'scan.startup.mode' = 'earliest-offset'\n" +
                ")", INPUT_TOPIC, KAFKA_BROKERS);

        // 3. Definir la tabla de salida (Sink) para validation_results
        String createSinkTableSql = String.format(
                "CREATE TABLE validation_results_table (\n" +
                "  score INT,\n" +
                "  title STRING,\n" +
                "  body STRING,\n" +
                "  answer STRING,\n" +
                "  request_id STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '%s',\n" +
                "  'properties.bootstrap.servers' = '%s',\n" +
                "  'value.format' = 'json'\n" +
                ")", OUTPUT_TOPIC, KAFKA_BROKERS);

        tEnv.executeSql(createSourceTableSql);
        tEnv.executeSql(createSinkTableSql);

        // 4. Lógica de Negocio (Asignar score=6 y mapear campos)
        String insertSql = String.format(
                "INSERT INTO validation_results_table\n" +
                "SELECT\n" +
                "  6 AS score,\n" +
                "  question AS title,\n" +
                "  CAST(NULL AS STRING) AS body,\n" +
                "  answer,\n" +
                "  request_id\n" +
                "FROM llm_responses_table\n" +
                "WHERE 6 >= 6"
        );

        // 5. Ejecutar la Inserción y mantener el job vivo
        tEnv.executeSql(insertSql).await();
        
        System.out.println(String.format("INFO: Job Flink 'Quality Gate' iniciado. Consumiendo de '%s' y produciendo a '%s'.", INPUT_TOPIC, OUTPUT_TOPIC));
    }
} 
