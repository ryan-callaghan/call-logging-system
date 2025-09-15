FROM openjdk:17-jdk-slim

LABEL maintainer="telco-team@company.com"
LABEL version="1.0.0"
LABEL description="Telco Event Generator Service"

# Create app directory
WORKDIR /app

# Copy application jar
COPY target/event-generator-1.0.0.jar app.jar

# Create non-root user
RUN groupadd -r telco && useradd -r -g telco telco
RUN chown -R telco:telco /app
USER telco

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8081/actuator/health || exit 1

# Expose port
EXPOSE 8081

# JVM options
ENV JAVA_OPTS="-Xmx2g -Xms1g -XX:+UseG1GC -XX:G1HeapRegionSize=16m -XX:+UseStringDeduplication"

# Run application
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]