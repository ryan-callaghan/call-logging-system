Write-Host "Testing Call Logging Infrastructure..." -ForegroundColor Green

# Test PostgreSQL
Write-Host "`n1. Testing PostgreSQL..." -ForegroundColor Yellow
docker-compose exec -T postgres psql -U admin -d calllogging -c "SELECT COUNT(*) as user_count FROM users;"

# Test Elasticsearch
Write-Host "`n2. Testing Elasticsearch..." -ForegroundColor Yellow
try {
    $esHealth = Invoke-RestMethod -Uri "http://localhost:9200/_cluster/health"
    Write-Host "Elasticsearch Status: $($esHealth.status)" -ForegroundColor Green
} catch {
    Write-Host "Elasticsearch Error: $($_.Exception.Message)" -ForegroundColor Red
}

# Test Kafka
Write-Host "`n3. Testing Kafka..." -ForegroundColor Yellow
docker-compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 | Select-String "localhost:9092"

# Test Kibana
Write-Host "`n4. Testing Kibana..." -ForegroundColor Yellow
try {
    $response = Invoke-WebRequest -Uri "http://localhost:5601/api/status" -UseBasicParsing
    if ($response.StatusCode -eq 200) {
        Write-Host "Kibana: Available" -ForegroundColor Green
    }
} catch {
    Write-Host "Kibana: Not ready yet" -ForegroundColor Red
}

Write-Host "`nInfrastructure test complete!" -ForegroundColor Green
Write-Host "`nAccess URLs:"
Write-Host "- Elasticsearch: http://localhost:9200"
Write-Host "- Kibana: http://localhost:5601"  
Write-Host "- Kafka UI: http://localhost:8080"
Write-Host "- PostgreSQL: localhost:5432 (admin/password123)"