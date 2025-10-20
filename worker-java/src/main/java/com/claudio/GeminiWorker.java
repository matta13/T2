package com.claudio;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig; // <<<<<<<<<<<<< Nueva Importación
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class GeminiWorker {

    private static final String KAFKA_BROKERS = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "broker:29092");
    private static final String INPUT_TOPIC = System.getenv().getOrDefault("KAFKA_INPUT_TOPIC", "llm_requests");
    private static final String RESPONSE_TOPIC = System.getenv().getOrDefault("KAFKA_RESPONSES_TOPIC", "llm_responses");
    private static final Gson gson = new Gson();

    public static void main(String[] args) {
        System.out.println("Iniciando GeminiWorker en Java...");
        new GeminiWorker().run();
    }

    private void run() {
        // 1. Configuración del Consumidor de Kafka (llm_requests)
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "java-worker-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); 
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(INPUT_TOPIC));

            // 2. Configuración del Productor de Kafka (llm_responses)
            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
            // LÍNEAS CORREGIDAS: Usamos ProducerConfig para los serializadores
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            try (Producer<String, String> producer = new KafkaProducer<>(producerProps)) {
                
                System.out.println("Esperando mensajes en el tópico: " + INPUT_TOPIC);

                while (true) {
                    consumer.poll(Duration.ofMillis(100)).forEach(record -> {
                        String payloadString = record.value();
                        try {
                            JsonObject payload = JsonParser.parseString(payloadString).getAsJsonObject();
                            String requestId = payload.get("request_id").getAsString();
                            String question = payload.get("question").getAsString();

                            System.out.println(String.format("INFO: Mensaje recibido. ID: %s, Pregunta: %s", requestId, question));

                            // 3. LÓGICA CENTRAL: SIMULACIÓN DE CONSULTA GEMINI
                            String simulatedAnswer = String.format("Respuesta generada en Java para: %s", question);

                            // 4. Preparar payload para Flink
                            JsonObject flinkPayload = new JsonObject();
                            flinkPayload.addProperty("request_id", requestId);
                            flinkPayload.addProperty("question", question);
                            flinkPayload.addProperty("answer", simulatedAnswer);
                            flinkPayload.addProperty("retry_count", payload.has("retry_count") ? payload.get("retry_count").getAsInt() : 0);

                            // 5. Enviar a Kafka (llm_responses)
                            ProducerRecord<String, String> responseRecord = new ProducerRecord<>(
                                    RESPONSE_TOPIC,
                                    requestId,
                                    gson.toJson(flinkPayload)
                            );
                            producer.send(responseRecord);
                            System.out.println(String.format("INFO: Respuesta enviada a Flink. ID: %s", requestId));
                            
                        } catch (Exception e) {
                            System.err.println("ERROR: Falló el procesamiento del mensaje: " + e.getMessage());
                            e.printStackTrace();
                        }
                    });

                    producer.flush(); 
                }
            } catch (Exception e) {
                System.err.println("ERROR CRÍTICO: Fallo al conectar o usar el productor de Kafka. Reintentando en 5s...");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}