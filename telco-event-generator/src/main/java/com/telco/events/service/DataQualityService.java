@Service
@Slf4j
public class DataQualityService {
    
    private final MeterRegistry meterRegistry;
    
    // Quality metrics
    private final Counter validEvents;
    private final Counter invalidEvents;
    private final Gauge dataQualityScore;
    private final Timer validationTime;
    
    // Quality rules tracking
    private final Map<String, AtomicLong> qualityRuleViolations = new ConcurrentHashMap<>();
    private final AtomicLong totalEventsProcessed = new AtomicLong();
    private final AtomicLong totalQualityScore = new AtomicLong();
    
    public DataQualityService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        
        this.validEvents = Counter.builder("telco.data.quality.valid.events")
                .register(meterRegistry);
        this.invalidEvents = Counter.builder("telco.data.quality.invalid.events")
                .register(meterRegistry);
        this.dataQualityScore = Gauge.builder("telco.data.quality.score")
                .register(meterRegistry, this, DataQualityService::getCurrentQualityScore);
        this.validationTime = Timer.builder("telco.data.quality.validation.time")
                .register(meterRegistry);
    }
    
    public DataQualityResult validateEvent(Object event) {
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try {
            DataQualityResult result = performValidation(event);
            
            if (result.isValid()) {
                validEvents.increment();
            } else {
                invalidEvents.increment();
                // Track specific rule violations
                result.getViolations().forEach(violation -> 
                    qualityRuleViolations.computeIfAbsent(violation.getRule(), 
                                                         k -> new AtomicLong()).incrementAndGet()
                );
            }
            
            // Update overall quality score
            updateQualityScore(result);
            
            return result;
            
        } finally {
            sample.stop(validationTime);
        }
    }
    
    private DataQualityResult performValidation(Object event) {
        List<QualityViolation> violations = new ArrayList<>();
        
        if (event instanceof CallEvent) {
            violations.addAll(validateCallEvent((CallEvent) event));
        } else if (event instanceof SessionEvent) {
            violations.addAll(validateSessionEvent((SessionEvent) event));
        } else if (event instanceof NetworkEvent) {
            violations.addAll(validateNetworkEvent((NetworkEvent) event));
        }
        
        return new DataQualityResult(violations.isEmpty(), violations, calculateEventScore(violations));
    }
    
    private List<QualityViolation> validateCallEvent(CallEvent event) {
        List<QualityViolation> violations = new ArrayList<>();
        
        // Completeness checks
        if (StringUtils.isBlank(event.getEventId())) {
            violations.add(new QualityViolation("COMPLETENESS", "Event ID is missing", "eventId"));
        }
        if (StringUtils.isBlank(event.getCallerPhoneNumber())) {
            violations.add(new QualityViolation("COMPLETENESS", "Caller phone number is missing", "callerPhoneNumber"));
        }
        if (StringUtils.isBlank(event.getCalleePhoneNumber())) {
            violations.add(new QualityViolation("COMPLETENESS", "Callee phone number is missing", "calleePhoneNumber"));
        }
        
        // Format validation
        if (!isValidPhoneNumber(event.getCallerPhoneNumber())) {
            violations.add(new QualityViolation("FORMAT", "Invalid caller phone number format", "callerPhoneNumber"));
        }
        if (!isValidPhoneNumber(event.getCalleePhoneNumber())) {
            violations.add(new QualityViolation("FORMAT", "Invalid callee phone number format", "calleePhoneNumber"));
        }
        
        // Business rule validation
        if (event.getCallerPhoneNumber().equals(event.getCalleePhoneNumber())) {
            violations.add(new QualityViolation("BUSINESS_RULE", "Caller and callee cannot be the same", "phoneNumbers"));
        }
        
        // Timestamp validation
        if (event.getTimestamp() <= 0) {
            violations.add(new QualityViolation("VALIDITY", "Invalid timestamp", "timestamp"));
        }
        
        long currentTime = System.currentTimeMillis();
        if (event.getTimestamp() > currentTime + 300000) { // 5 minutes in future
            violations.add(new QualityViolation("VALIDITY", "Timestamp is too far in the future", "timestamp"));
        }
        
        // Call-specific validation
        if (event.getEventType() == CallEventType.CALL_END) {
            if (event.getDuration() == null || event.getDuration() <= 0) {
                violations.add(new QualityViolation("BUSINESS_RULE", "Call end event must have positive duration", "duration"));
            }
            if (event.getDuration() != null && event.getDuration() > 86400) { // 24 hours
                violations.add(new QualityViolation("REASONABLENESS", "Call duration seems unreasonably long", "duration"));
            }
        }
        
        // Location validation
        if (event.getLocation() != null) {
            double lat = event.getLocation().getLatitude();
            double lon = event.getLocation().getLongitude();
            
            if (lat < -90 || lat > 90) {
                violations.add(new QualityViolation("VALIDITY", "Invalid latitude", "location.latitude"));
            }
            if (lon < -180 || lon > 180) {
                violations.add(new QualityViolation("VALIDITY", "Invalid longitude", "location.longitude"));
            }
        }
        
        return violations;
    }
    
    private List<QualityViolation> validateSessionEvent(SessionEvent event) {
        List<QualityViolation> violations = new ArrayList<>();
        
        // Completeness checks
        if (StringUtils.isBlank(event.getEventId())) {
            violations.add(new QualityViolation("COMPLETENESS", "Event ID is missing", "eventId"));
        }
        if (StringUtils.isBlank(event.getPhoneNumber())) {
            violations.add(new QualityViolation("COMPLETENESS", "Phone number is missing", "phoneNumber"));
        }
        if (StringUtils.isBlank(event.getSessionId())) {
            violations.add(new QualityViolation("COMPLETENESS", "Session ID is missing", "sessionId"));
        }
        if (StringUtils.isBlank(event.getImsi())) {
            violations.add(new QualityViolation("COMPLETENESS", "IMSI is missing", "imsi"));
        }
        
        // Format validation
        if (!isValidPhoneNumber(event.getPhoneNumber())) {
            violations.add(new QualityViolation("FORMAT", "Invalid phone number format", "phoneNumber"));
        }
        if (!isValidIMSI(event.getImsi())) {
            violations.add(new QualityViolation("FORMAT", "Invalid IMSI format", "imsi"));
        }
        
        // Network quality validation
        if (event.getNetworkQuality() != null) {
            NetworkQuality quality = event.getNetworkQuality();
            
            if (quality.getSignalStrength() > -30 || quality.getSignalStrength() < -120) {
                violations.add(new QualityViolation("VALIDITY", "Invalid signal strength", "networkQuality.signalStrength"));
            }
            if (quality.getBandwidth() <= 0) {
                violations.add(new QualityViolation("VALIDITY", "Invalid bandwidth", "networkQuality.bandwidth"));
            }
            if (!isValidNetworkType(quality.getNetworkType())) {
                violations.add(new QualityViolation("VALIDITY", "Invalid network type", "networkQuality.networkType"));
            }
        }
        
        // Session-specific validation
        if (event.getEventType() == SessionEventType.SESSION_END) {
            if (event.getDataUsage() == null || event.getDataUsage() < 0) {
                violations.add(new QualityViolation("BUSINESS_RULE", "Session end event must have valid data usage", "dataUsage"));
            }
        }
        
        return violations;
    }
    
    private List<QualityViolation> validateNetworkEvent(NetworkEvent event) {
        List<QualityViolation> violations = new ArrayList<>();
        
        // Completeness checks
        if (StringUtils.isBlank(event.getEventId())) {
            violations.add(new QualityViolation("COMPLETENESS", "Event ID is missing", "eventId"));
        }
        if (StringUtils.isBlank(event.getCellTowerId())) {
            violations.add(new QualityViolation("COMPLETENESS", "Cell tower ID is missing", "cellTowerId"));
        }
        
        // Metrics validation
        if (event.getMetrics() != null) {
            NetworkMetrics metrics = event.getMetrics();
            
            if (metrics.getLatency() < 0 || metrics.getLatency() > 1000) {
                violations.add(new QualityViolation("VALIDITY", "Latency out of reasonable range", "metrics.latency"));
            }
            if (metrics.getPacketLoss() < 0 || metrics.getPacketLoss() > 1) {
                violations.add(new QualityViolation("VALIDITY", "Packet loss must be between 0 and 1", "metrics.packetLoss"));
            }
            if (metrics.getJitter() < 0 || metrics.getJitter() > 100) {
                violations.add(new QualityViolation("VALIDITY", "Jitter out of reasonable range", "metrics.jitter"));
            }
            if (metrics.getThroughput() <= 0) {
                violations.add(new QualityViolation("VALIDITY", "Throughput must be positive", "metrics.throughput"));
            }
            if (metrics.getActiveConnections() < 0) {
                violations.add(new QualityViolation("VALIDITY", "Active connections cannot be negative", "metrics.activeConnections"));
            }
            
            // Reasonableness checks
            if (metrics.getLatency() > 500) {
                violations.add(new QualityViolation("REASONABLENESS", "Very high latency detected", "metrics.latency"));
            }
            if (metrics.getPacketLoss() > 0.1) {
                violations.add(new QualityViolation("REASONABLENESS", "Very high packet loss detected", "metrics.packetLoss"));
            }
        }
        
        return violations;
    }
    
    // Helper validation methods
    private boolean isValidPhoneNumber(String phoneNumber) {
        if (StringUtils.isBlank(phoneNumber)) return false;
        return phoneNumber.matches("\\d{10}") && phoneNumber.length() == 10;
    }
    
    private boolean isValidIMSI(String imsi) {
        if (StringUtils.isBlank(imsi)) return false;
        return imsi.matches("\\d{15}") && imsi.length() == 15;
    }
    
    private boolean isValidNetworkType(String networkType) {
        return Arrays.asList("2G", "3G", "4G", "5G", "LTE").contains(networkType);
    }
    
    private double calculateEventScore(List<QualityViolation> violations) {
        if (violations.isEmpty()) return 100.0;
        
        // Weight violations by severity
        double penalty = violations.stream()
                .mapToDouble(this::getViolationPenalty)
                .sum();
        
        return Math.max(0.0, 100.0 - penalty);
    }
    
    private double getViolationPenalty(QualityViolation violation) {
        return switch (violation.getRule()) {
            case "COMPLETENESS" -> 20.0;
            case "FORMAT" -> 15.0;
            case "VALIDITY" -> 15.0;
            case "BUSINESS_RULE" -> 25.0;
            case "REASONABLENESS" -> 5.0;
            default -> 10.0;
        };
    }
    
    private void updateQualityScore(DataQualityResult result) {
        totalEventsProcessed.incrementAndGet();
        totalQualityScore.addAndGet((long) result.getScore());
    }
    
    private double getCurrentQualityScore() {
        long processed = totalEventsProcessed.get();
        if (processed == 0) return 100.0;
        return (double) totalQualityScore.get() / processed;
    }
    
    public DataQualityReport generateQualityReport() {
        long processed = totalEventsProcessed.get();
        double avgScore = getCurrentQualityScore();
        long validCount = (long) validEvents.count();
        long invalidCount = (long) invalidEvents.count();
        
        Map<String, Long> violationCounts = new HashMap<>(qualityRuleViolations.size());
        qualityRuleViolations.forEach((rule, count) -> violationCounts.put(rule, count.get()));
        
        return DataQualityReport.builder()
                .totalEventsProcessed(processed)
                .validEventsCount(validCount)
                .invalidEventsCount(invalidCount)
                .averageQualityScore(avgScore)
                .qualityPercentage((double) validCount / processed * 100)
                .violationsByRule(violationCounts)
                .generatedAt(Instant.now())
                .build();
    }
    
    // Data classes
    @Data
    @AllArgsConstructor
    public static class DataQualityResult {
        private boolean valid;
        private List<QualityViolation> violations;
        private double score;
    }
    
    @Data
    @AllArgsConstructor
    public static class QualityViolation {
        private String rule;
        private String message;
        private String field;
    }
    
    @Data
    @Builder
    public static class DataQualityReport {
        private long totalEventsProcessed;
        private long validEventsCount;
        private long invalidEventsCount;
        private double averageQualityScore;
        private double qualityPercentage;
        private Map<String, Long> violationsByRule;
        private Instant generatedAt;
    }
}