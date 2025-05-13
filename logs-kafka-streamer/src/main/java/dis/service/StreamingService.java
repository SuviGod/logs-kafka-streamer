package dis.service;

import dis.infrastructure.KafkaProducerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of the StreamingService that streams data from CSV files to Kafka topics.
 */
@Service
public class StreamingService {

    private static final Logger logger = LoggerFactory.getLogger(StreamingService.class);
    private final KafkaProducerService kafkaProducerService;
    private final AtomicBoolean isStreaming = new AtomicBoolean(false);
    private ExecutorService executorService;

    @Autowired
    public StreamingService(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    public boolean startStreaming(String inputFolder) {
        if (isStreaming.get()) {
            logger.warn("Streaming is already in progress");
            return false;
        }

        executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            try {
                streamData(inputFolder);
            } catch (Exception e) {
                logger.error("Error during streaming: {}", e.getMessage(), e);
            } finally {
                isStreaming.set(false);
                kafkaProducerService.closeProducer();
            }
        });

        isStreaming.set(true);
        return true;
    }

    public boolean stopStreaming() {
        if (!isStreaming.get()) {
            logger.warn("No active streaming to stop");
            return false;
        }

        executorService.shutdownNow();
        isStreaming.set(false);
        kafkaProducerService.closeProducer();
        logger.info("Streaming stopped");
        return true;
    }

    public boolean isStreaming() {
        return isStreaming.get();
    }

    private void streamData(String inputFolder) {
        logger.debug("Using input folder: {}", inputFolder);

        // Collect all CSV files in the input folder
        File[] logFiles = new File(inputFolder)
                .listFiles((dir, name) ->
                        name.endsWith("_kills.csv") || name.endsWith("_damages.csv"));
        if (logFiles == null || logFiles.length == 0) {
            logger.info("No CSV files found in the folder: {}", inputFolder);
            return;
        }

        BufferedReader[] readers = null;
        String[] topics = new String[logFiles.length];
        long startTime = System.nanoTime();

        try {
            kafkaProducerService.createProducer();
            readers = new BufferedReader[logFiles.length];

            // Initialize readers and topics
            for (int i = 0; i < readers.length; i++) {
                try {
                    readers[i] = new BufferedReader(new FileReader(logFiles[i]));
                    String[] parts = logFiles[i].getName().split("_");
                    topics[i] = "game." + parts[parts.length - 1].split("\\.")[0].trim();
                    logger.info("Going to stream data from file: {}, topic: {}", logFiles[i], topics[i]);
                } catch (IOException e) {
                    logger.error(e.getMessage());
                }
            }

            int ticks = 0;
            int ticksStepDelay = 1000 / 128;
            int counter = 0;

            // Skip headers
            for (BufferedReader reader : readers) {
                reader.readLine();
            }

            // Read first line from each file
            String[] lines = new String[readers.length];
            for (int i = 0; i < readers.length; i++) {
                lines[i] = readers[i].readLine();
            }

            // Main streaming loop
            while (isStreaming.get() && Arrays.stream(lines).anyMatch(Objects::nonNull)) {
                for (int i = 0; i < lines.length; i++) {
                    if (lines[i] == null) {
                        continue;
                    }

                    int tick = Integer.parseInt(lines[i].split(",")[1]);
                    while (tick == ticks) {
                        String topic = topics[i];
                        long elapsedSeconds = (System.nanoTime() - startTime) / 1_000_000_000;
                        String line = lines[i] + "," + elapsedSeconds;

                        kafkaProducerService.sendMessage(topic, Integer.toString(counter++), line);

                        lines[i] = readers[i].readLine();
                        if (lines[i] == null) {
                            tick = -1;
                        } else {
                            tick = Integer.parseInt(lines[i].split(",")[1]);
                        }
                    }
                }

                ticks++;
                try {
                    Thread.sleep(ticksStepDelay);
                } catch (InterruptedException e) {
                    logger.info("Streaming interrupted");
                    break;
                }
            }

            logger.info("Finished streaming data from files: {}", Arrays.toString(logFiles));
        } catch (Exception e) {
            logger.error("Error during streaming: {}", e.getMessage(), e);
        } finally {
            // Close all readers
            if (readers != null) {
                for (BufferedReader reader : readers) {
                    try {
                        if (reader != null) {
                            reader.close();
                        }
                    } catch (IOException e) {
                        logger.error(e.getMessage());
                    }
                }
            }
        }
    }
}
