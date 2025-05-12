# Logs Kafka Streamer

A Spring Boot application that streams data from CSV files to Kafka topics. The application provides REST endpoints to start and stop the streaming process.

## Features

- Stream data from CSV files to Kafka topics
- Start and stop streaming via REST endpoints
- Check streaming status via REST endpoint
- Clean architecture with dependency injection

## Requirements

- Java 17 or higher
- Maven
- Kafka running on localhost:29092 (configurable in application.properties)

## Building the Application

```bash
mvn clean package
```

This will create an executable JAR file in the `target` directory.

## Running the Application

```bash
java -jar target/logs-kafka-streamer-1.0-SNAPSHOT.jar
```

The application will start on port 8080 by default (configurable in application.properties).

## REST Endpoints

### Start Streaming

```
POST /api/streaming/start?inputFolder={folderPath}
```

Starts streaming data from CSV files in the specified folder to Kafka topics.

Example:
```
POST http://localhost:8080/api/streaming/start?inputFolder=C:/path/to/csv/files
```

Response:
```json
{
  "status": "success",
  "message": "Streaming started successfully"
}
```

### Stop Streaming

```
POST /api/streaming/stop
```

Stops the current streaming process.

Example:
```
POST http://localhost:8080/api/streaming/stop
```

Response:
```json
{
  "status": "success",
  "message": "Streaming stopped successfully"
}
```

### Get Streaming Status

```
GET /api/streaming/status
```

Returns the current status of streaming.

Example:
```
GET http://localhost:8080/api/streaming/status
```

Response:
```json
{
  "status": "success",
  "streaming": true,
  "message": "Streaming is active"
}
```

## Configuration

The application can be configured in the `src/main/resources/application.properties` file:

```properties
# Server configuration
server.port=8080

# Logging configuration
logging.level.root=INFO
logging.level.dis=DEBUG

# Application name
spring.application.name=logs-kafka-streamer

# Kafka configuration
kafka.bootstrap-servers=localhost:29092
```