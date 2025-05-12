package dis.infrastructure;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Properties;

/**
 * Service for interacting with Kafka producer.
 */
@Component
public class KafkaProducerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);
    private Producer<String, String> producer;

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * Creates a new Kafka producer with default configuration.
     * 
     * @return a configured Kafka producer
     */
    public Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        logger.info("Creating Kafka producer with bootstrap servers: {}", bootstrapServers);
        producer = new KafkaProducer<>(props);
        return producer;
    }

    /**
     * Sends a message to a Kafka topic.
     * 
     * @param topic the Kafka topic
     * @param key the message key
     * @param value the message value
     */
    public void sendMessage(String topic, String key, String value) {
        if (producer == null) {
            throw new IllegalStateException("Producer not initialized. Call createProducer() first.");
        }

        logger.info("Sending message to topic: {}, key: {}, value: {}", topic, key, value);
        producer.send(new ProducerRecord<>(topic, key, value));
    }

    /**
     * Closes the Kafka producer.
     */
    public void closeProducer() {
        if (producer != null) {
            producer.close();
            producer = null;
        }
    }
}
