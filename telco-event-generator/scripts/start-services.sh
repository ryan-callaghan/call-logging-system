#!/bin/bash

set -e

echo "Starting Telco Event Generation Infrastructure..."

# Function to wait for service to be ready
wait_for_service() {
    local url=$1
    local service_name=$2
    local max_attempts=30
    local attempt=1
    
    echo "Waiting for $service_name to be ready..."
    while [ $attempt -le $max_attempts ]; do
        if curl -f "$url" > /dev/null 2>&1; then
            echo "$service_name is ready!"
            return 0
        fi
        echo "Attempt $attempt/$max_attempts failed. Retrying in 10 seconds..."
        sleep 10
        attempt=$((attempt + 1))
    done
    
    echo "ERROR: $service_name failed to start within expected time"
    return 1
}

# Start infrastructure services
echo "Starting Kafka infrastructure..."
docker-compose up -d zookeeper kafka1 kafka2 kafka3

# Wait for Kafka to be ready
wait_for_service "http://localhost:9092" "Kafka"

# Start Schema Registry
echo "Starting Schema Registry..."
docker-compose up -d schema-registry

# Wait for Schema Registry to be ready
wait_for_service "http://localhost:8081/subjects" "Schema Registry"

# Register schemas
echo "Registering Avro schemas..."
./register-schemas.sh

# Start monitoring services
echo "Starting monitoring services..."
docker-compose up -d prometheus kafka-ui

# Wait for services to be ready
wait_for_service "http://localhost:9090" "Prometheus"
wait_for_service "http://localhost:8080" "Kafka UI"

# Build and start the event generator
echo "Building event generator application..."
mvn clean package -DskipTests

echo "Starting event generator service..."
java -jar target/event-generator-1.0.0.jar &

# Wait for event generator to be ready
wait_for_service "http://localhost:8081/actuator/health" "Event Generator"

echo "All services started successfully!"
echo ""
echo "Service URLs:"
echo "- Event Generator: http://localhost:8081"
echo "- Kafka UI: http://localhost:8080"
echo "- Prometheus: http://localhost:9090"
echo "- Schema Registry: http://localhost:8081"
echo ""
echo "To start event generation:"
echo "curl -X POST http://localhost:8081/api/v1/events/start"