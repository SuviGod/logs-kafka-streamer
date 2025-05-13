package dis.controller;

import dis.service.CsvService;
import dis.service.StreamingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST controller for managing streaming operations.
 */
@RestController
@RequestMapping("/api")
public class StreamingController {

    private static final Logger logger = LoggerFactory.getLogger(StreamingController.class);
    private final StreamingService streamingService;
    private final CsvService csvService;

    @Autowired
    public StreamingController(StreamingService streamingService, CsvService csvService) {
        this.streamingService = streamingService;
        this.csvService = csvService;
    }

    /**
     * Start streaming data from CSV files to Kafka topics.
     *
     * @param inputFolder the folder containing CSV files to stream
     * @return response with status of the operation
     */
    @PostMapping("/streaming/start")
    public ResponseEntity<Map<String, Object>> startStreaming(@RequestParam String inputFolder) {
        logger.info("Received request to start streaming from folder: {}", inputFolder);

        Map<String, Object> response = new HashMap<>();
        boolean started = streamingService.startStreaming(inputFolder);

        if (started) {
            response.put("status", "success");
            response.put("message", "Streaming started successfully");
            return ResponseEntity.ok(response);
        } else {
            response.put("status", "error");
            response.put("message", "Failed to start streaming, it might be already in progress");
            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * Stop the current streaming process.
     *
     * @return response with status of the operation
     */
    @PostMapping("/streaming/stop")
    public ResponseEntity<Map<String, Object>> stopStreaming() {
        logger.info("Received request to stop streaming");

        Map<String, Object> response = new HashMap<>();
        boolean stopped = streamingService.stopStreaming();

        if (stopped) {
            response.put("status", "success");
            response.put("message", "Streaming stopped successfully");
            return ResponseEntity.ok(response);
        } else {
            response.put("status", "error");
            response.put("message", "No active streaming to stop");
            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * Get the current status of streaming.
     *
     * @return response with streaming status
     */
    @GetMapping("/streaming/status")
    public ResponseEntity<Map<String, Object>> getStreamingStatus() {
        logger.info("Received request to get streaming status");

        Map<String, Object> response = new HashMap<>();
        boolean isStreaming = streamingService.isStreaming();

        response.put("status", "success");
        response.put("streaming", isStreaming);
        response.put("message", isStreaming ? "Streaming is active" : "No active streaming");

        return ResponseEntity.ok(response);
    }

    /**
     * Read a CSV file and return its rows as a list of strings.
     *
     * @param filePath the path to the CSV file
     * @return list of strings, each representing a row in the CSV file
     */
    @GetMapping("/csv")
    public List<String> readCsvFile(@RequestParam String filePath) {
        logger.info("Received request to read CSV file: {}", filePath);
        return csvService.readCsvFile(filePath);
    }

    /**
     * Find a file ending with "_players.csv" in the given folder and read its rows.
     *
     * @param folderPath the path to the folder containing the CSV file
     * @return list of strings, each representing a row in the CSV file
     */
    @GetMapping("/players")
    public ResponseEntity<List<String>> readPlayersFile(@RequestParam String folderPath) {
        logger.info("Received request to read players file from folder: {}", folderPath);
        return ResponseEntity.ok(csvService.readPlayersFile(folderPath));
    }
}