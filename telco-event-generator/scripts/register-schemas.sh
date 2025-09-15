#!/bin/bash

SCHEMA_REGISTRY_URL="http://localhost:8081"
AVRO_SCHEMA_DIR="src/main/resources/avro"

echo "Registering Avro schemas with Schema Registry..."

# Function to register schema
register_schema() {
    local schema_file=$1
    local subject=$2
    
    echo "Registering schema from $schema_file for subject $subject..."
    
    curl -X POST \
         -H "Content-Type: application/vnd.schemaregistry.v1+json" \
         --data "{\"schema\": $(jq tostring < $schema_file)}" \
         "$SCHEMA_REGISTRY_URL/subjects/$subject/versions"
    
    echo -e "\n"
}

# Wait for Schema Registry to be ready
echo "Waiting for Schema Registry to be ready..."
until curl -f "$SCHEMA_REGISTRY_URL/subjects" > /dev/null 2>&1; do
    echo "Schema Registry not ready, waiting..."
    sleep 5
done

echo "Schema Registry is ready!"

# Register schemas
register_schema "$AVRO_SCHEMA_DIR/call-event.avsc" "com.telco.events.avro.CallEvent"
register_schema "$AVRO_SCHEMA_DIR/session-event.avsc" "com.telco.events.avro.SessionEvent"
register_schema "$AVRO_SCHEMA_DIR/network-event.avsc" "com.telco.events.avro.NetworkEvent"

echo "Schema registration completed!"

# List all registered schemas
echo "Registered schemas:"
curl -X GET "$SCHEMA_REGISTRY_URL/subjects"
echo -e "\n"