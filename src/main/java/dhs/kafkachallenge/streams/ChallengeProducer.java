package dhs.kafkachallenge.streams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ChallengeProducer {

    public static void main(String... args) {
        final Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-challenge");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        streamsConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Producer<Void, String> producer = new KafkaProducer<>(streamsConfig);

        List<String> events = Arrays.asList("{\"event\": \"CREATE\", \"recordId\": 1, \"name\": \"Joe Programmer\"}",
                                            "{\"event\": \"UPDATE\", \"recordId\": 1, \"name\": \"Java Programmer\"}",
                                            "{\"event\": \"DELETE\", \"recordId\": 1}");
        events.forEach(e -> producer.send(new ProducerRecord<>("candidate-update", e)));
        producer.close();
    }
}
