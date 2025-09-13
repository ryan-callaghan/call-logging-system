# Call Logging System

A distributed microservices-based call logging and analytics system built with Spring Boot, Kafka, and Elasticsearch.

## Architecture Overview

This system processes call events in real-time, stores them for fast search and analytics, and provides REST APIs for querying call data.

### Services
- **event-generator**: Simulates call events from phone systems
- **event-processor**: Processes events from Kafka and stores in Elasticsearch  
- **query-service**: REST API for searching and retrieving call data
- **monitoring-service**: Health checks and system metrics

### Infrastructure
- **Apache Kafka**: Event streaming and message brokering
- **Elasticsearch**: Search engine and analytics  
- **PostgreSQL**: User management and metadata storage
- **Docker**: Containerized development environment

## Quick Start

### Prerequisites
- Docker Desktop
- Java 17+
- Maven 3.6+

### Start Infrastructure
```bash
cd docker-compose
docker-compose up -d
