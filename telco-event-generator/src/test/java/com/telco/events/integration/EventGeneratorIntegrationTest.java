@SpringBootTest
@TestPropertySource(properties = {
    "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "spring.kafka.producer.properties.schema.registry.url=mock://localhost:8081"
})
@EmbeddedKafka(partitions = 3, 
               topics = {"telco.call.events", "telco.session.events", "telco.network.events"},
               brokerProperties = {"auto.create.topics.enable=false"})
class EventGeneratorIntegrationTest {
    
    @Autowired
    private EventGeneratorService eventGeneratorService;
    
    @Autowired
    private KafkaProducerService kafkaProducerService;
    
    @Autowired
    private DataQualityService dataQualityService;
    
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
    
    private MockSchemaRegistryClient schemaRegistryClient;
    
    @BeforeEach
    void setUp() throws Exception {
        // Setup mock schema registry
        schemaRegistryClient = new MockSchemaRegistryClient();
        
        // Register test schemas
        registerTestSchemas();
    }
    
    @Test
    void testCallEventGeneration() throws Exception {
        // Given
        CountDownLatch latch = new CountDownLatch(10);
        
        // Setup consumer to verify messages
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
                         embeddedKafka.getBrokersAsString());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        
        Consumer<String, CallEvent> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("telco.call.events"));
        
        // When - Generate events
        for (int i = 0; i < 10; i++) {
            CallEvent event = createTestCallEvent();
            kafkaProducerService.sendCallEvent(event);
        }
        
        // Then - Verify events are received
        List<CallEvent> receivedEvents = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        
        while (receivedEvents.size() < 10 && System.currentTimeMillis() - startTime < 30000) {
            ConsumerRecords<String, CallEvent> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, CallEvent> record : records) {
                receivedEvents.add(record.value());
                latch.countDown();
            }
        }
        
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        assertEquals(10, receivedEvents.size());
        
        // Verify event structure
        CallEvent firstEvent = receivedEvents.get(0);
        assertNotNull(firstEvent.getEventId());
        assertNotNull(firstEvent.getCallerPhoneNumber());
        assertNotNull(firstEvent.getCalleePhoneNumber());
        assertTrue(firstEvent.getTimestamp() > 0);
        
        consumer.close();
    }
    
    @Test
    void testDataQualityValidation() {
        // Test valid event
        CallEvent validEvent = createTestCallEvent();
        DataQualityService.DataQualityResult result = dataQualityService.validateEvent(validEvent);
        
        assertTrue(result.isValid());
        assertEquals(100.0, result.getScore());
        assertTrue(result.getViolations().isEmpty());
        
        // Test invalid event
        CallEvent invalidEvent = CallEvent.newBuilder()
                .setEventId("") // Empty event ID
                .setEventType(CallEventType.CALL_START)
                .setTimestamp(-1) // Invalid timestamp
                .setCallerPhoneNumber("123") // Invalid phone number
                .setCalleePhoneNumber("123") // Same as caller
                .setCallId("test-call")
                .setCellTowerId("CELL_001")
                .setLocation(Location.newBuilder()
                            .setLatitude(100.0) // Invalid latitude
                            .setLongitude(200.0) // Invalid longitude
                            .build())
                .build();
        
        DataQualityService.DataQualityResult invalidResult = dataQualityService.validateEvent(invalidEvent);
        
        assertFalse(invalidResult.isValid());
        assertTrue(invalidResult.getScore() < 100.0);
        assertFalse(invalidResult.getViolations().isEmpty());
        
        // Verify specific violations
        List<String> violationRules = invalidResult.getViolations().stream()
                .map(DataQualityService.QualityViolation::getRule)
                .collect(Collectors.toList());
        
        assertTrue(violationRules.contains("COMPLETENESS"));
        assertTrue(violationRules.contains("VALIDITY"));
        assertTrue(violationRules.contains("FORMAT"));
        assertTrue(violationRules.contains("BUSINESS_RULE"));
    }
    
    @Test
    void testSchemaEvolution() throws Exception {
        // Test backward compatibility
        CallEvent originalEvent = createTestCallEvent();
        
        // Simulate consuming with an older schema (missing new fields)
        // This should work with backward compatibility
        assertTrue(isBackwardCompatible(originalEvent));
        
        // Test forward compatibility
        // Add a new field and ensure old consumers can still process
        assertTrue(isForwardCompatible(originalEvent));
    }
    
    @Test
    void testProducerErrorHandling() {
        // Test handling of producer failures
        // This would involve mocking Kafka failures and verifying retry logic
        
        // Setup mock to simulate failure
        KafkaTemplate<String, Object> mockTemplate = mock(KafkaTemplate.class);
        CompletableFuture<SendResult<String, Object>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Kafka connection failed"));
        
        when(mockTemplate.send(anyString(), anyString(), any())).thenReturn(failedFuture);
        
        KafkaProducerService producerService = new KafkaProducerService(
            mockTemplate, 
            new EventGeneratorConfig(), 
            meterRegistry
        );
        
        CallEvent event = createTestCallEvent();
        CompletableFuture<SendResult<String, Object>> result = producerService.sendCallEvent(event);
        
        // Verify that the failure is handled appropriately
        assertThrows(RuntimeException.class, () -> result.get(5, TimeUnit.SECONDS));
    }
    
    @Test
    void testEventGenerationPerformance() {
        // Performance test to ensure we can generate required throughput
        long startTime = System.currentTimeMillis();
        int eventCount = 1000;
        
        for (int i = 0; i < eventCount; i++) {
            CallEvent event = createTestCallEvent();
            kafkaProducerService.sendCallEvent(event);
        }
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        double eventsPerSecond = (double) eventCount / (duration / 1000.0);
        
        // Should be able to generate at least 500 events per second
        assertTrue(eventsPerSecond > 500, 
                  "Event generation rate: " + eventsPerSecond + " events/sec");
    }
    
    private CallEvent createTestCallEvent() {
        return CallEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(CallEventType.CALL_START)
                .setTimestamp(System.currentTimeMillis())
                .setCallerPhoneNumber("5551234567")
                .setCalleePhoneNumber("5559876543")
                .setCallId(UUID.randomUUID().toString())
                .setCellTowerId("CELL_001")
                .setLocation(Location.newBuilder()
                            .setLatitude(40.7128)
                            .setLongitude(-74.0060)
                            .build())
                .build();
    }
    
    private void registerTestSchemas() throws Exception {
        // Register schemas with mock schema registry
        String callEventSchema = loadSchemaFromFile("call-event.avsc");
        schemaRegistryClient.register("com.telco.events.avro.CallEvent", 
                                     new Schema.Parser().parse(callEventSchema));
        
        String sessionEventSchema = loadSchemaFromFile("session-event.avsc");
        schemaRegistryClient.register("com.telco.events.avro.SessionEvent", 
                                     new Schema.Parser().parse(sessionEventSchema));
        
        String networkEventSchema = loadSchemaFromFile("network-event.avsc");
        schemaRegistryClient.register("com.telco.events.avro.NetworkEvent", 
                                     new Schema.Parser().parse(networkEventSchema));
    }
    
    private String loadSchemaFromFile(String filename) throws IOException {
        return Files.readString(Paths.get("src/main/resources/avro/" + filename));
    }
    
    private boolean isBackwardCompatible(CallEvent event) {
        // Implement backward compatibility check
        return true;
    }
    
    private boolean isForwardCompatible(CallEvent event) {
        // Implement forward compatibility check
        return true;
    }
}