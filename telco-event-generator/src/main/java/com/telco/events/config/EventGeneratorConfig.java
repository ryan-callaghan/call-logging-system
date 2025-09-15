@Configuration
@ConfigurationProperties(prefix = "event-generator")
@Data
public class EventGeneratorConfig {
    
    private Topics topics = new Topics();
    private Rates rates = new Rates();
    private PhoneNumber phoneNumber = new PhoneNumber();
    private Network network = new Network();
    
    @Data
    public static class Topics {
        private String callEvents = "telco.call.events";
        private String sessionEvents = "telco.session.events";
        private String networkEvents = "telco.network.events";
    }
    
    @Data
    public static class Rates {
        private int defaultRate = 1000;
        private int max = 10000;
        private int min = 100;
    }
    
    @Data
    public static class PhoneNumber {
        private long min = 1000000000L;
        private long max = 9999999999L;
    }
    
    @Data
    public static class Network {
        private double baseLatency = 50.0;
        private double maxJitter = 20.0;
        private double packetLossProbability = 0.01;
    }
}