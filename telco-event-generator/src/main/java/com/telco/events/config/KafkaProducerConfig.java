@Configuration
@EnableKafka
@Slf4j
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.producer.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        
        // Basic Kafka configuration
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        
        // Performance configuration
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        
        // Schema Registry configuration
        configProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        configProps.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        configProps.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, true);
        
        // Error handling
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(producerFactory());
        
        // Add error handling
        template.setProducerListener(new ProducerListener<String, Object>() {
            @Override
            public void onSuccess(ProducerRecord<String, Object> producerRecord, 
                                 RecordMetadata recordMetadata) {
                log.debug("Message sent successfully to topic {} partition {} offset {}", 
                         recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
            }

            @Override
            public void onError(ProducerRecord<String, Object> producerRecord, 
                               RecordMetadata recordMetadata, Exception exception) {
                log.error("Error sending message to topic {}: {}", 
                         producerRecord.topic(), exception.getMessage(), exception);
            }
        });
        
        return template;
    }

    @Bean
    public NewTopic callEventsTopic(@Value("${event-generator.topics.call-events}") String topicName) {
        return TopicBuilder.name(topicName)
                .partitions(12)
                .replicas(3)
                .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")
                .config(TopicConfig.RETENTION_MS_CONFIG, "604800000") // 7 days
                .build();
    }

    @Bean
    public NewTopic sessionEventsTopic(@Value("${event-generator.topics.session-events}") String topicName) {
        return TopicBuilder.name(topicName)
                .partitions(12)
                .replicas(3)
                .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")
                .config(TopicConfig.RETENTION_MS_CONFIG, "604800000")
                .build();
    }

    @Bean
    public NewTopic networkEventsTopic(@Value("${event-generator.topics.network-events}") String topicName) {
        return TopicBuilder.name(topicName)
                .partitions(6)
                .replicas(3)
                .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")
                .config(TopicConfig.RETENTION_MS_CONFIG, "86400000") // 1 day
                .build();
    }
}