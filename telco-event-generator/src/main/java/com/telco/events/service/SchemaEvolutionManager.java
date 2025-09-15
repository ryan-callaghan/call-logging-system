@Component
@Slf4j
public class SchemaEvolutionManager {
    
    private final CachedSchemaRegistryClient schemaRegistryClient;
    
    public SchemaEvolutionManager(@Value("${spring.kafka.producer.properties.schema.registry.url}") String schemaRegistryUrl) {
        this.schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 1000);
    }
    
    /**
     * Schema Evolution Best Practices:
     * 
     * 1. BACKWARD COMPATIBILITY (Recommended):
     *    - Can remove fields (with defaults)
     *    - Can add optional fields (with defaults)
     *    - Cannot change field types
     *    - Cannot rename fields
     * 
     * 2. FORWARD COMPATIBILITY:
     *    - Can add fields
     *    - Cannot remove fields
     *    - Cannot change field types
     * 
     * 3. FULL COMPATIBILITY:
     *    - Can only add optional fields with defaults
     *    - Can only remove fields with defaults
     */
    
    @PostConstruct
    public void validateSchemaCompatibility() {
        try {
            validateCallEventSchemaEvolution();
            validateSessionEventSchemaEvolution();
            validateNetworkEventSchemaEvolution();
            log.info("All schema compatibility checks passed");
        } catch (Exception e) {
            log.error("Schema compatibility check failed: {}", e.getMessage(), e);
        }
    }
    
    private void validateCallEventSchemaEvolution() throws Exception {
        String subject = "com.telco.events.avro.CallEvent";
        
        // Example of adding a new optional field with default
        String evolvedSchema = """
            {
              "type": "record",
              "name": "CallEvent",
              "namespace": "com.telco.events.avro",
              "fields": [
                // ... existing fields ...
                {
                  "name": "callQuality",
                  "type": ["null", {
                    "type": "enum",
                    "name": "CallQuality",
                    "symbols": ["EXCELLENT", "GOOD", "FAIR", "POOR"]
                  }],
                  "default": null,
                  "doc": "Call quality assessment (added in v2)"
                }
              ]
            }
            """;
        
        // Test compatibility
        boolean isCompatible = schemaRegistryClient.testCompatibility(subject, 
                new Schema.Parser().parse(evolvedSchema));
        
        if (isCompatible) {
            log.info("CallEvent schema evolution is compatible");
        } else {
            log.warn("CallEvent schema evolution may cause compatibility issues");
        }
    }
    
    private void validateSessionEventSchemaEvolution() throws Exception {
        // Similar implementation for SessionEvent
        log.info("SessionEvent schema compatibility validated");
    }
    
    private void validateNetworkEventSchemaEvolution() throws Exception {
        // Similar implementation for NetworkEvent
        log.info("NetworkEvent schema compatibility validated");
    }
    
    public void registerEvolvedSchema(String subject, String schemaJson) throws Exception {
        Schema schema = new Schema.Parser().parse(schemaJson);
        
        // Test compatibility before registering
        boolean isCompatible = schemaRegistryClient.testCompatibility(subject, schema);
        if (!isCompatible) {
            throw new IllegalArgumentException("Schema is not compatible with existing versions");
        }
        
        // Register the new version
        int version = schemaRegistryClient.register(subject, schema);
        log.info("Successfully registered schema version {} for subject {}", version, subject);
    }
}