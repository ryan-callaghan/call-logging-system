#!/bin/bash

set -e

echo "Verifying Telco Event Generation Setup..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check service health
check_service() {
    local url=$1
    local service_name=$2
    
    if curl -f "$url" > /dev/null 2>&1; then
        echo -e "✅ ${GREEN}$service_name is healthy${NC}"
        return 0
    else
        echo -e "❌ ${RED}$service_name is not responding${NC}"
        return 1
    fi
}

# Function to check topic exists
check_topic() {
    local topic=$1
    
    if docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 --list | grep -q "$topic"; then
        echo -e "✅ ${GREEN}Topic $topic exists${NC}"
        return 0
    else
        echo -e "❌ ${RED}Topic $topic does not exist${NC}"
        return 1
    fi
}

# Function to check schema is registered
check_schema() {
    local schema_subject=$1
    
    if curl -f "http://localhost:8081/subjects/$schema_subject/versions" > /dev/null 2>&1; then
        echo -e "✅ ${GREEN}Schema $schema_subject is registered${NC}"
        return 0
    else
        echo -e "❌ ${RED}Schema $schema_subject is not registered${NC}"
        return 1
    fi
}

echo -e "${YELLOW}Checking service health...${NC}"
check_service "http://localhost:2181" "Zookeeper"
check_service "http://localhost:9092" "Kafka Broker 1"
check_service "http://localhost:9093" "Kafka Broker 2" 
check_service "http://localhost:9094" "Kafka Broker 3"
check_service "http://localhost:8081/subjects" "Schema Registry"
check_service "http://localhost:8081/actuator/health" "Event Generator"
check_service "http://localhost:8080" "Kafka UI"
check_service "http://localhost:9090" "Prometheus"

echo -e "\n${YELLOW}Checking Kafka topics...${NC}"
check_topic "telco.call.events"
check_topic "telco.session.events"
check_topic "telco.network.events"

echo -e "\n${YELLOW}Checking schema registration...${NC}"
check_schema "com.telco.events.avro.CallEvent"
check_schema "com.telco.events.avro.SessionEvent"
check_schema "com.telco.events.avro.NetworkEvent"

echo -e "\n${YELLOW}Testing event generation...${NC}"

# Start event generation
echo "Starting event generation..."
curl -X POST http://localhost:8081/api/v1/events/start > /dev/null 2>&1

# Wait a bit for events to be generated
sleep 5

# Check metrics
echo "Checking event generation metrics..."
METRICS=$(curl -s http://localhost:8081/api/v1/events/metrics)

if echo "$METRICS" | grep -q "callEventsGenerated"; then
    echo -e "✅ ${GREEN}Call events are being generated${NC}"
else
    echo -e "❌ ${RED}Call events are not being generated${NC}"
fi

if echo "$METRICS" | grep -q "sessionEventsGenerated"; then
    echo -e "✅ ${GREEN}Session events are being generated${NC}"
else
    echo -e "❌ ${RED}Session events are not being generated${NC}"
fi

if echo "$METRICS" | grep -q "networkEventsGenerated"; then
    echo -e "✅ ${GREEN}Network events are being generated${NC}"
else
    echo -e "❌ ${RED}Network events are not being generated${NC}"
fi

echo -e "\n${YELLOW}Checking Kafka message consumption...${NC}"

# Check if messages are being produced to Kafka topics
for topic in "telco.call.events" "telco.session.events" "telco.network.events"; do
    MESSAGE_COUNT=$(docker exec kafka1 kafka-console-consumer --bootstrap-server localhost:9092 --topic "$topic" --timeout-ms 3000 --from-beginning 2>/dev/null | wc -l || echo "0")
    
    if [ "$MESSAGE_COUNT" -gt 0 ]; then
        echo -e "✅ ${GREEN}Topic $topic has $MESSAGE_COUNT messages${NC}"
    else
        echo -e "❌ ${RED}Topic $topic has no messages${NC}"
    fi
done

echo -e "\n${YELLOW}Checking data quality metrics...${NC}"

# Get data quality metrics from the application
QUALITY_RESPONSE=$(curl -s http://localhost:8081/actuator/metrics/telco.data.quality.score)

if echo "$QUALITY_RESPONSE" | grep -q "measurements"; then
    QUALITY_SCORE=$(echo "$QUALITY_RESPONSE" | grep -o '"value":[0-9.]*' | cut -d':' -f2)
    echo -e "✅ ${GREEN}Data quality score: $QUALITY_SCORE${NC}"
else
    echo -e "❌ ${RED}Data quality metrics not available${NC}"
fi

echo -e "\n${YELLOW}Setup verification complete!${NC}"
echo -e "\n${GREEN}You can now:${NC}"
echo "1. View Kafka UI at: http://localhost:8080"
echo "2. View Prometheus metrics at: http://localhost:9090"
echo "3. Check event generator status at: http://localhost:8081/api/v1/events/status"
echo "4. View application metrics at: http://localhost:8081/actuator/metrics"