package dev.roy.parreira;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

@Slf4j
public class SimpleProducer {

  private static final String LOCAL_BOOTSTRAP_SERVER = "127.0.0.1:9092";

  public static void main(String[] args) {
    Properties producerProperties = new Properties();
    producerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, LOCAL_BOOTSTRAP_SERVER);
    producerProperties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProperties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    KafkaProducer<String, String> simpleProducer = new KafkaProducer<>(producerProperties);

    ProducerRecord<String, String> simpleProducerRecord =
        new ProducerRecord<>("simple-topic", "Hello world!!!");

    simpleProducer.send(simpleProducerRecord);
    simpleProducer.flush();
    simpleProducer.close();
  }
}
