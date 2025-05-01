package dis;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.Properties;
import java.util.stream.Stream;

public class App {

    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {

        logger.debug("using input folder: {}", args[0]);

        // Create the producer
        // Set up the producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            Path path = Paths.get(args[0]);
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
                for (Path entry : stream) {
                    if (Files.isRegularFile(entry)) {
                        logger.info("starting to stream file: {}", entry.getFileName());
                        streamFile(entry, producer);
                        logger.info("finished to stream file: {}", entry.getFileName());
                    }
                }
            }
        }
        catch (IOException | DirectoryIteratorException e) {
            logger.error(e.getMessage());
        }
    }

    private static void streamFile(Path filePath, Producer<String, String> producer) throws IOException {
        String[] parts = filePath.getFileName().toString().split("_");
        String topic = "game." + parts[parts.length - 1].split("\\.")[0].trim();
        try (Stream<String> lines = Files.lines(filePath)) {
            int counter = 0;
            for (String line : lines.toList()) {
                // skip headers.
                if (counter == 0) {
                    counter++;
                    continue;
                }
                producer.send(new ProducerRecord<>(topic, Integer.toString(counter), line));
                counter++;
            }
        }
    }
}
