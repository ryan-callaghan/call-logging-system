@Service
@Slf4j
public class KafkaProducerService {
    
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final EventGeneratorConfig config;
    private final MeterRegistry meterRegistry;
    
    private final Counter messagesSent;
    private final Counter messagesFailure;
    private final Timer sendTimer;
    
    public KafkaProducerService(KafkaTemplate<String, Object> kafkaTemplate, 
                               EventGeneratorConfig config,
                               MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.config = config;
        this.meterRegistry = meterRegistry;
        
        // Initialize metrics
        this.messagesSent = Counter.builder("telco.kafka.messages.sent")
                .register(meterRegistry);
        this.messagesFailure = Counter.builder("telco.kafka.messages.failed")
                .register(meterRegistry);
        this.sendTimer = Timer.builder("telco.kafka.send.time")
                .register(meterRegistry);
    }
    
    public CompletableFuture<SendResult<String, Object>> sendCallEvent(CallEvent event) {
        String key = generatePartitionKey(event.getCallerPhoneNumber());
        return sendEvent(config.getTopics().getCallEvents(), key, event, "call");
    }
    
    public CompletableFuture<SendResult<String, Object>> sendSessionEvent(SessionEvent event) {
        String key = generatePartitionKey(event.getPhoneNumber());
        return sendEvent(config.getTopics().getSessionEvents(), key, event, "session");
    }
    
    public CompletableFuture<SendResult<String, Object>> sendNetworkEvent(NetworkEvent event) {
        String key = event.getCellTowerId();
        return sendEvent(config.getTopics().getNetworkEvents(), key, event, "network");
    }
    
    private CompletableFuture<SendResult<String, Object>> sendEvent(String topic, String key, 
                                                                   Object event, String eventType) {
        Timer.Sample sample = Timer.start(meterRegistry);
        
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, key, event);
        
        future.whenComplete((result, throwable) -> {
            sample.stop(sendTimer);
            
            if (throwable != null) {
                log.error("Failed to send {} event to topic {}: {}", eventType, topic, 
                         throwable.getMessage(), throwable);
                messagesFailure.increment(Tags.of("type", eventType, "topic", topic));
                
                // Implement retry logic or dead letter queue here
                handleSendFailure(topic, key, event, throwable);
            } else {
                log.debug("Successfully sent {} event to topic {} partition {} offset {}", 
                         eventType, result.getRecordMetadata().topic(), 
                         result.getRecordMetadata().partition(), 
                         result.getRecordMetadata().offset());
                messagesSent.increment(Tags.of("type", eventType, "topic", topic));
            }
        });
        
        return future;
    }
    
    private String generatePartitionKey(String phoneNumber) {
        // Use consistent hashing based on phone number to ensure
        // all events for a phone number go to the same partition
        return String.valueOf(phoneNumber.hashCode());
    }
    
    private void handleSendFailure(String topic, String key, Object event, Throwable throwable) {
        // Log to dead letter queue or implement retry with exponential backoff
        log.error("Event send failure - Topic: {}, Key: {}, Error: {}", topic, key, throwable.getMessage());
        
        // You could implement:
        // 1. Retry with exponential backoff
        // 2. Send to dead letter queue
        // 3. Store in database for later retry
        // 4. Alert monitoring systems
    }
}