package dis;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;

public class App {

    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {

        logger.debug("using input folder: {}", args[0]);

        // Collect all CSV files in the game-logs folder
        File[] logFiles = new File(args[0]).listFiles((dir, name) -> name.endsWith(".csv"));
        if (logFiles == null || logFiles.length == 0) {
            logger.info("No CSV files found in the folder: {}", args[0]);
            return;
        }

        // Create the producer
        // Set up the producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        BufferedReader[] readers = null;
        String[] topics = new String[logFiles.length];
        long startTime = System.nanoTime();

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            readers = new BufferedReader[logFiles.length];
            for (int i = 0; i < readers.length; i++) {
                try {
                    readers[i] = new BufferedReader(new FileReader(logFiles[i]));
                    String[] parts = logFiles[i].getName().split("_");
                    topics[i] = "game." + parts[parts.length - 1].split("\\.")[0].trim();
                    logger.info("Going to stream data from file: {}, topic: {}", logFiles[i], topics[i]);
                }
                catch (IOException e) {
                    logger.error(e.getMessage());
                }
            }

            int ticks = 0;
            int ticksStepDelay = 1000 / 128;
            int counter = 0;

            // skip headers;
            for (BufferedReader reader : readers) {
                reader.readLine();
            }

            // read first line
            String[] lines = new String[readers.length];
            for (int i = 0; i < readers.length; i++) {
                lines[i] = readers[i].readLine();
            }

            do {
                for (int i = 0; i < lines.length; i++) {
                    if (lines[i] == null) {
                        continue;
                    }
                    int tick = Integer.parseInt(lines[i].split(",")[1]);
                    while (tick == ticks) {
                        String topic = topics[i];
                        long elapsedSeconds = (System.nanoTime() - startTime) / 1_000_000_000;
                        String line = lines[i] + "," + elapsedSeconds;
                        producer.send(new ProducerRecord<>(topic, Integer.toString(counter++), line));
                        logger.info("sending message to topic: {}, tick: {}, line: {}", topic, tick, line);
                        lines[i] = readers[i].readLine();
                        if (lines[i] == null) {
                            tick = -1;
                        }
                        else {
                            tick = Integer.parseInt(lines[i].split(",")[1]);
                        }
                    }
                }
                ticks++;
                Thread.sleep(ticksStepDelay);
            }
            while (Arrays.stream(lines).allMatch(Objects::nonNull));

            logger.info("Finished streaming data from files: {}", Arrays.toString(logFiles));
        }
        catch (Exception e) {
            logger.error(e.getMessage());
        }
        finally {
            if (readers != null) {
                for (BufferedReader reader : readers) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        logger.error(e.getMessage());
                    }
                }
            }
        }
    }
}
