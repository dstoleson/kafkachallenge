package dhs.kafkachallenge.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class ChallengeProcessor {

    public static void main(String... args) {
        final Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-challenge");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        streamsConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        streamsConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        streamsConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Producer<Void, String> producer = new KafkaProducer<>(streamsConfig);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("candidate-update",
                                                        Consumed.with(Serdes.String(),
                                                                      Serdes.String()));
        ObjectMapper jsonMapper = new ObjectMapper();

        stream.foreach((key, value) -> {
            System.out.printf("%s%n", value);
            try {
                CandidateEvent candidateEvent = jsonMapper.readValue(value, CandidateEvent.class);
                LogEvent logEvent = new LogEvent();
                logEvent.setRecordId(candidateEvent.getRecordId());
                logEvent.setEvent(candidateEvent.getEvent());
                logEvent.setStatus("SUCCESS");
                producer.send(new ProducerRecord<>("transaction-log", jsonMapper.writeValueAsString(logEvent)));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        });


        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, streamsConfig);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
