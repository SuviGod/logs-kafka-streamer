package dis.domain;

/**
 * Interface for the streaming service that defines the core business logic
 * for streaming data from CSV files to Kafka topics.
 */
public interface StreamingService {
    
    /**
     * Starts streaming data from CSV files to Kafka topics.
     * 
     * @param inputFolder the folder containing CSV files to stream
     * @return true if streaming started successfully, false otherwise
     */
    boolean startStreaming(String inputFolder);
    
    /**
     * Stops the current streaming process.
     * 
     * @return true if streaming was stopped successfully, false if there was no active streaming
     */
    boolean stopStreaming();
    
    /**
     * Checks if streaming is currently active.
     * 
     * @return true if streaming is active, false otherwise
     */
    boolean isStreaming();
}