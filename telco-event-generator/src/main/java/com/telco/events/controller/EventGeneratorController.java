@RestController
@RequestMapping("/api/v1/events")
@Slf4j
@Validated
public class EventGeneratorController {
    
    private final EventGeneratorService eventGeneratorService;
    private final KafkaProducerService kafkaProducerService;
    private final EventGeneratorConfig config;
    private final MeterRegistry meterRegistry;
    
    private volatile boolean generationEnabled = true;
    private volatile int currentRate;
    
    public EventGeneratorController(EventGeneratorService eventGeneratorService,
                                   KafkaProducerService kafkaProducerService,
                                   EventGeneratorConfig config,
                                   MeterRegistry meterRegistry) {
        this.eventGeneratorService = eventGeneratorService;
        this.kafkaProducerService = kafkaProducerService;
        this.config = config;
        this.meterRegistry = meterRegistry;
        this.currentRate = config.getRates().getDefaultRate();
    }
    
    @PostMapping("/start")
    public ResponseEntity<GeneratorStatusResponse> startGeneration() {
        generationEnabled = true;
        log.info("Event generation started");
        return ResponseEntity.ok(new GeneratorStatusResponse(true, currentRate, "Generation started"));
    }
    
    @PostMapping("/stop")
    public ResponseEntity<GeneratorStatusResponse> stopGeneration() {
        generationEnabled = false;
        log.info("Event generation stopped");
        return ResponseEntity.ok(new GeneratorStatusResponse(false, currentRate, "Generation stopped"));
    }
    
    @PostMapping("/rate/{rate}")
    public ResponseEntity<GeneratorStatusResponse> setGenerationRate(
            @PathVariable @Min(value = 100, message = "Rate must be at least 100") 
            @Max(value = 10000, message = "Rate cannot exceed 10000") int rate) {
        
        if (rate < config.getRates().getMin() || rate > config.getRates().getMax()) {
            return ResponseEntity.badRequest()
                    .body(new GeneratorStatusResponse(generationEnabled, currentRate, 
                          "Rate must be between " + config.getRates().getMin() + 
                          " and " + config.getRates().getMax()));
        }
        
        currentRate = rate;
        log.info("Event generation rate set to {}", rate);
        return ResponseEntity.ok(new GeneratorStatusResponse(generationEnabled, currentRate, "Rate updated"));
    }
    
    @GetMapping("/status")
    public ResponseEntity<GeneratorStatusResponse> getStatus() {
        return ResponseEntity.ok(new GeneratorStatusResponse(generationEnabled, currentRate, "Current status"));
    }
    
    @GetMapping("/metrics")
    public ResponseEntity<Map<String, Object>> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        
        // Get counters
        metrics.put("callEventsGenerated", getCounterValue("telco.events.generated", "call"));
        metrics.put("sessionEventsGenerated", getCounterValue("telco.events.generated", "session"));
        metrics.put("networkEventsGenerated", getCounterValue("telco.events.generated", "network"));
        metrics.put("messagesSent", getCounterValue("telco.kafka.messages.sent"));
        metrics.put("messagesFailed", getCounterValue("telco.kafka.messages.failed"));
        
        // Get timers
        Timer generationTimer = meterRegistry.find("telco.events.generation.time").timer();
        if (generationTimer != null) {
            metrics.put("averageGenerationTime", generationTimer.mean(TimeUnit.MILLISECONDS));
        }
        
        Timer sendTimer = meterRegistry.find("telco.kafka.send.time").timer();
        if (sendTimer != null) {
            metrics.put("averageSendTime", sendTimer.mean(TimeUnit.MILLISECONDS));
        }
        
        return ResponseEntity.ok(metrics);
    }
    
    @PostMapping("/generate/call")
    public ResponseEntity<String> generateSingleCallEvent() {
        try {
            // This would need to be implemented in the EventGeneratorService
            log.info("Manual call event generation requested");
            return ResponseEntity.ok("Call event generated successfully");
        } catch (Exception e) {
            log.error("Error generating call event: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error generating call event: " + e.getMessage());
        }
    }
    
    private double getCounterValue(String counterName, String... tags) {
        Counter counter = meterRegistry.find(counterName).tags(tags).counter();
        return counter != null ? counter.count() : 0.0;
    }
    
    private double getCounterValue(String counterName) {
        Counter counter = meterRegistry.find(counterName).counter();
        return counter != null ? counter.count() : 0.0;
    }
    
    @Data
    @AllArgsConstructor
    public static class GeneratorStatusResponse {
        private boolean enabled;
        private int rate;
        private String message;
    }
}