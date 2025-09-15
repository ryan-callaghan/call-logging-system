@Service
@Slf4j
public class EventGeneratorService {
    
    private final KafkaProducerService kafkaProducerService;
    private final EventGeneratorConfig config;
    private final Random random = new Random();
    private final MeterRegistry meterRegistry;
    
    private final Counter callEventsGenerated;
    private final Counter sessionEventsGenerated;
    private final Counter networkEventsGenerated;
    private final Timer eventGenerationTimer;
    
    // Active call sessions tracking
    private final Map<String, CallSession> activeCalls = new ConcurrentHashMap<>();
    private final Map<String, DataSession> activeSessions = new ConcurrentHashMap<>();
    
    // Cell tower IDs pool
    private final List<String> cellTowerIds = IntStream.range(1, 1001)
            .mapToObj(i -> "CELL_" + String.format("%04d", i))
            .collect(Collectors.toList());
    
    public EventGeneratorService(KafkaProducerService kafkaProducerService, 
                               EventGeneratorConfig config,
                               MeterRegistry meterRegistry) {
        this.kafkaProducerService = kafkaProducerService;
        this.config = config;
        this.meterRegistry = meterRegistry;
        
        // Initialize metrics
        this.callEventsGenerated = Counter.builder("telco.events.generated")
                .tag("type", "call")
                .register(meterRegistry);
        this.sessionEventsGenerated = Counter.builder("telco.events.generated")
                .tag("type", "session")
                .register(meterRegistry);
        this.networkEventsGenerated = Counter.builder("telco.events.generated")
                .tag("type", "network")
                .register(meterRegistry);
        this.eventGenerationTimer = Timer.builder("telco.events.generation.time")
                .register(meterRegistry);
    }
    
    @Scheduled(fixedRateString = "${event-generator.generation-interval:1000}")
    public void generateEvents() {
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try {
            // Generate call events (30% of total)
            generateCallEvents(Math.max(1, config.getRates().getDefaultRate() * 30 / 100));
            
            // Generate session events (40% of total)
            generateSessionEvents(Math.max(1, config.getRates().getDefaultRate() * 40 / 100));
            
            // Generate network events (30% of total)
            generateNetworkEvents(Math.max(1, config.getRates().getDefaultRate() * 30 / 100));
            
        } finally {
            sample.stop(eventGenerationTimer);
        }
    }
    
    private void generateCallEvents(int count) {
        for (int i = 0; i < count; i++) {
            try {
                CallEvent event = shouldGenerateCallEnd() ? generateCallEndEvent() : generateCallStartEvent();
                if (event != null) {
                    kafkaProducerService.sendCallEvent(event);
                    callEventsGenerated.increment();
                }
            } catch (Exception e) {
                log.error("Error generating call event: {}", e.getMessage(), e);
            }
        }
    }
    
    private void generateSessionEvents(int count) {
        for (int i = 0; i < count; i++) {
            try {
                SessionEvent event = shouldGenerateSessionEnd() ? generateSessionEndEvent() : generateSessionStartEvent();
                if (event != null) {
                    kafkaProducerService.sendSessionEvent(event);
                    sessionEventsGenerated.increment();
                }
            } catch (Exception e) {
                log.error("Error generating session event: {}", e.getMessage(), e);
            }
        }
    }
    
    private void generateNetworkEvents(int count) {
        for (int i = 0; i < count; i++) {
            try {
                NetworkEvent event = generateNetworkEvent();
                kafkaProducerService.sendNetworkEvent(event);
                networkEventsGenerated.increment();
            } catch (Exception e) {
                log.error("Error generating network event: {}", e.getMessage(), e);
            }
        }
    }
    
    private CallEvent generateCallStartEvent() {
        String callId = UUID.randomUUID().toString();
        String callerPhone = generatePhoneNumber();
        String calleePhone = generatePhoneNumber();
        String cellTowerId = getRandomCellTowerId();
        Location location = generateLocation();
        
        // Track active call
        activeCalls.put(callId, new CallSession(callId, callerPhone, System.currentTimeMillis()));
        
        return CallEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(CallEventType.CALL_START)
                .setTimestamp(System.currentTimeMillis())
                .setCallerPhoneNumber(callerPhone)
                .setCalleePhoneNumber(calleePhone)
                .setCallId(callId)
                .setCellTowerId(cellTowerId)
                .setLocation(location)
                .build();
    }
    
    private CallEvent generateCallEndEvent() {
        if (activeCalls.isEmpty()) {
            return null;
        }
        
        CallSession session = activeCalls.values().iterator().next();
        activeCalls.remove(session.getCallId());
        
        long duration = (System.currentTimeMillis() - session.getStartTime()) / 1000;
        
        return CallEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(CallEventType.CALL_END)
                .setTimestamp(System.currentTimeMillis())
                .setCallerPhoneNumber(session.getCallerPhone())
                .setCalleePhoneNumber(generatePhoneNumber()) // Simplified
                .setCallId(session.getCallId())
                .setDuration(duration)
                .setCellTowerId(getRandomCellTowerId())
                .setLocation(generateLocation())
                .build();
    }
    
    private SessionEvent generateSessionStartEvent() {
        String sessionId = UUID.randomUUID().toString();
        String phoneNumber = generatePhoneNumber();
        String imsi = generateIMSI();
        
        // Track active session
        activeSessions.put(sessionId, new DataSession(sessionId, phoneNumber, System.currentTimeMillis()));
        
        return SessionEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(SessionEventType.SESSION_START)
                .setTimestamp(System.currentTimeMillis())
                .setPhoneNumber(phoneNumber)
                .setSessionId(sessionId)
                .setImsi(imsi)
                .setApn(getRandomAPN())
                .setNetworkQuality(generateNetworkQuality())
                .build();
    }
    
    private SessionEvent generateSessionEndEvent() {
        if (activeSessions.isEmpty()) {
            return null;
        }
        
        DataSession session = activeSessions.values().iterator().next();
        activeSessions.remove(session.getSessionId());
        
        long dataUsage = random.nextLong(100_000_000L); // Up to 100MB
        
        return SessionEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(SessionEventType.SESSION_END)
                .setTimestamp(System.currentTimeMillis())
                .setPhoneNumber(session.getPhoneNumber())
                .setSessionId(session.getSessionId())
                .setImsi(generateIMSI())
                .setApn(getRandomAPN())
                .setNetworkQuality(generateNetworkQuality())
                .setDataUsage(dataUsage)
                .build();
    }
    
    private NetworkEvent generateNetworkEvent() {
        String cellTowerId = getRandomCellTowerId();
        
        double baseLatency = config.getNetwork().getBaseLatency();
        double latency = baseLatency + (random.nextGaussian() * 10); // Normal distribution
        double packetLoss = random.nextDouble() < config.getNetwork().getPacketLossProbability() 
                ? random.nextDouble() * 0.05 : 0.0; // 0-5% when packet loss occurs
        double jitter = random.nextDouble() * config.getNetwork().getMaxJitter();
        long throughput = (long) (1_000_000_000 + (random.nextGaussian() * 100_000_000)); // ~1Gbps ± 100Mbps
        int activeConnections = random.nextInt(1000) + 100;
        
        // Determine alert level
        AlertLevel alertLevel = AlertLevel.NORMAL;
        if (latency > 100 || packetLoss > 0.02) {
            alertLevel = AlertLevel.WARNING;
        }
        if (latency > 200 || packetLoss > 0.05) {
            alertLevel = AlertLevel.CRITICAL;
        }
        
        NetworkMetrics metrics = NetworkMetrics.newBuilder()
                .setLatency(Math.max(0, latency))
                .setPacketLoss(Math.max(0, Math.min(1, packetLoss)))
                .setJitter(Math.max(0, jitter))
                .setThroughput(Math.max(0, throughput))
                .setActiveConnections(activeConnections)
                .build();
        
        return NetworkEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setTimestamp(System.currentTimeMillis())
                .setCellTowerId(cellTowerId)
                .setMetrics(metrics)
                .setAlertLevel(alertLevel)
                .build();
    }
    
    // Helper methods
    private boolean shouldGenerateCallEnd() {
        return !activeCalls.isEmpty() && random.nextDouble() < 0.3;
    }
    
    private boolean shouldGenerateSessionEnd() {
        return !activeSessions.isEmpty() && random.nextDouble() < 0.2;
    }
    
    private String generatePhoneNumber() {
        long min = config.getPhoneNumber().getMin();
        long max = config.getPhoneNumber().getMax();
        return String.valueOf(random.nextLong(max - min) + min);
    }
    
    private String getRandomCellTowerId() {
        return cellTowerIds.get(random.nextInt(cellTowerIds.size()));
    }
    
    private Location generateLocation() {
        // Generate realistic US coordinates
        double latitude = 25.0 + random.nextDouble() * 24.0; // 25-49 North
        double longitude = -125.0 + random.nextDouble() * 60.0; // -125 to -65 West
        return Location.newBuilder()
                .setLatitude(latitude)
                .setLongitude(longitude)
                .build();
    }
    
    private String generateIMSI() {
        return "310" + String.format("%012d", random.nextLong(1_000_000_000_000L));
    }
    
    private String getRandomAPN() {
        String[] apns = {"internet", "mms", "wap", "vzwinternet", "broadband"};
        return apns[random.nextInt(apns.length)];
    }
    
    private NetworkQuality generateNetworkQuality() {
        int signalStrength = -120 + random.nextInt(70); // -120 to -50 dBm
        long bandwidth = 1_000_000L + random.nextLong(99_000_000L); // 1-100 Mbps
        String[] networkTypes = {"4G", "5G", "3G"};
        String networkType = networkTypes[random.nextInt(networkTypes.length)];
        
        return NetworkQuality.newBuilder()
                .setSignalStrength(signalStrength)
                .setBandwidth(bandwidth)
                .setNetworkType(networkType)
                .build();
    }
    
    // Inner classes for session tracking
    @Data
    @AllArgsConstructor
    private static class CallSession {
        private String callId;
        private String callerPhone;
        private long startTime;
    }
    
    @Data
    @AllArgsConstructor
    private static class DataSession {
        private String sessionId;
        private String phoneNumber;
        private long startTime;
    }
}