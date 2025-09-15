@Service
@Slf4j
public class SchemaValidationService {
    
    private final CachedSchemaRegistryClient schemaRegistryClient;
    private final Map<String, Schema> schemaCache = new ConcurrentHashMap<>();
    private final MeterRegistry meterRegistry;
    
    private final Counter validationSuccess;
    private final Counter validationFailure;
    
    public SchemaValidationService(@Value("${spring.kafka.producer.properties.schema.registry.url}") String schemaRegistryUrl,
                                 MeterRegistry meterRegistry) {
        this.schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 1000);
        this.meterRegistry = meterRegistry;
        
        this.validationSuccess = Counter.builder("telco.schema.validation.success")
                .register(meterRegistry);
        this.validationFailure = Counter.builder("telco.schema.validation.failure")
                .register(meterRegistry);
        
        // Pre-load schemas
        loadSchemas();
    }
    
    private void loadSchemas() {
        try {
            loadSchema("com.telco.events.avro.CallEvent");
            loadSchema("com.telco.events.avro.SessionEvent");
            loadSchema("com.telco.events.avro.NetworkEvent");
            log.info("Successfully loaded all Avro schemas");
        } catch (Exception e) {
            log.error("Failed to load schemas: {}", e.getMessage(), e);
        }
    }
    
    private void loadSchema(String schemaName) throws Exception {
        SchemaMetadata metadata = schemaRegistryClient.getLatestSchemaMetadata(schemaName);
        Schema schema = new Schema.Parser().parse(metadata.getSchema());
        schemaCache.put(schemaName, schema);
        log.info("Loaded schema for {}, version {}", schemaName, metadata.getVersion());
    }
    
    public boolean validateEvent(Object event) {
        try {
            String schemaName = event.getClass().getName();
            Schema schema = schemaCache.get(schemaName);
            
            if (schema == null) {
                log.warn("Schema not found for {}", schemaName);
                validationFailure.increment(Tags.of("schema", schemaName, "reason", "schema_not_found"));
                return false;
            }
            
            // Validate the event against the schema
            if (event instanceof SpecificRecord) {
                SpecificRecord record = (SpecificRecord) event;
                Schema eventSchema = record.getSchema();
                
                if (!eventSchema.equals(schema)) {
                    log.warn("Schema mismatch for {}", schemaName);
                    validationFailure.increment(Tags.of("schema", schemaName, "reason", "schema_mismatch"));
                    return false;
                }
                
                // Additional validation logic can be added here
                validateBusinessRules(record);
                
                validationSuccess.increment(Tags.of("schema", schemaName));
                return true;
            }
            
            validationFailure.increment(Tags.of("schema", schemaName, "reason", "not_specific_record"));
            return false;
            
        } catch (Exception e) {
            log.error("Error validating event {}: {}", event.getClass().getName(), e.getMessage(), e);
            validationFailure.increment(Tags.of("schema", event.getClass().getName(), "reason", "validation_error"));
            return false;
        }
    }
    
    private void validateBusinessRules(SpecificRecord record) {
        // Implement business rule validations
        if (record instanceof CallEvent) {
            validateCallEvent((CallEvent) record);
        } else if (record instanceof SessionEvent) {
            validateSessionEvent((SessionEvent) record);
        } else if (record instanceof NetworkEvent) {
            validateNetworkEvent((NetworkEvent) record);
        }
    }
    
    private void validateCallEvent(CallEvent event) {
        // Business rule validations for call events
        if (event.getCallerPhoneNumber().equals(event.getCalleePhoneNumber())) {
            throw new ValidationException("Caller and callee phone numbers cannot be the same");
        }
        
        if (event.getEventType() == CallEventType.CALL_END && event.getDuration() == null) {
            throw new ValidationException("Call end event must have duration");
        }
        
        if (event.getEventType() == CallEventType.CALL_START && event.getDuration() != null) {
            throw new ValidationException("Call start event should not have duration");
        }
    }
    
    private void validateSessionEvent(SessionEvent event) {
        // Business rule validations for session events
        if (event.getEventType() == SessionEventType.SESSION_END && event.getDataUsage() == null) {
            throw new ValidationException("Session end event must have data usage");
        }
        
        if (event.getNetworkQuality().getSignalStrength() > -30 || 
            event.getNetworkQuality().getSignalStrength() < -120) {
            throw new ValidationException("Invalid signal strength value");
        }
    }
    
    private void validateNetworkEvent(NetworkEvent event) {
        // Business rule validations for network events
        NetworkMetrics metrics = event.getMetrics();
        
        if (metrics.getLatency() < 0 || metrics.getLatency() > 1000) {
            throw new ValidationException("Invalid latency value");
        }
        
        if (metrics.getPacketLoss() < 0 || metrics.getPacketLoss() > 1) {
            throw new ValidationException("Packet loss must be between 0 and 1");
        }
        
        if (metrics.getJitter() < 0) {
            throw new ValidationException("Jitter cannot be negative");
        }
    }
    
    public static class ValidationException extends RuntimeException {
        public ValidationException(String message) {
            super(message);
        }
    }
}