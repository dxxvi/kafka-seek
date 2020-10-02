package a;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * kafka-topics --bootstrap-server localhost:9092 --create --topic TOPIC.NAME --partitions 4
 */
public class SeekTest {
    static Logger log = LoggerFactory.getLogger(SeekTest.class);
    static Random random = new Random();
    static String bootstrapServers = "localhost:9092";
    static String topic = "TOPIC.NAME"; // I created 4 partitions for this topic

    static KafkaProducer<String, Serializable> createProducer() {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new KafkaProducer<>(producerConfig);
    }

    static KafkaConsumer<String, Serializable> createConsumer() {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, Serializable> consumer = new KafkaConsumer<>(consumerConfig);
        consumer.subscribe(Collections.singleton(topic));
        return consumer;
    }

    static byte[] createRandomByteArray() {
        return IntStream.range(10, 11 + random.nextInt(90))
                .mapToObj(__ -> UUID.randomUUID().toString())
                .collect(Collectors.joining("-"))
                .getBytes();
    }

    /**
     * posts messages to kafka and returns a set of (partition#, offset) of messages that need to be re-read.
     */
    static Set<AbstractMap.SimpleEntry<Integer, Long>> postMessages(KafkaProducer<String, Serializable> producer) {
        Set<AbstractMap.SimpleEntry<Integer, Long>> msgesNeedReread = new HashSet<>();
        for (int i = 0; i < 2000; i++) {
            try {
                RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, createRandomByteArray())).get();
                if (random.nextInt(1000) % 6 == 0) {
                    msgesNeedReread.add(new AbstractMap.SimpleEntry<>(metadata.partition(), metadata.offset()));
                }
            } catch (Exception ex) { /* ignore the exception */ }
        }
        return msgesNeedReread;
    }

    public static void main(String[] args) {
        KafkaProducer<String, Serializable> producer = createProducer();
        Set<AbstractMap.SimpleEntry<Integer, Long>> msgesNeedReread = postMessages(producer);

        // find the lowest offset in each partition of the messages that need to be re-read
        Map<Integer, Long> partitionToOffsetMap = msgesNeedReread.stream()
                .collect(Collectors.groupingBy(
                        AbstractMap.SimpleEntry::getKey,
                        Collectors.mapping(
                                AbstractMap.SimpleEntry::getValue,
                                Collectors.reducing(Long.MAX_VALUE, Math::min)
                        )
                ));
        log.info("partitionToOffsetMap: {}", partitionToOffsetMap);

        KafkaConsumer<String, Serializable> consumer = createConsumer();
        consumer.poll(Duration.ofMillis(0));

        partitionToOffsetMap.forEach((partition, offset) -> {
            consumer.seek(new TopicPartition(topic, partition), offset);
        });

        consumer.close();
        producer.close();
    }
}


