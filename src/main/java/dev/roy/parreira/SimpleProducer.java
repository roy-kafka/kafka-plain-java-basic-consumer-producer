package dev.roy.parreira;

import lombok.Data;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

@Data
public class SimpleProducer {

  private KafkaProducer<String, String> kafkaProducer;

  public SimpleProducer() {
    setProducerWithBasicProperties("127.0.0.1:9092");
  }

  public SimpleProducer(String bootstrapServer) {
    setProducerWithBasicProperties(bootstrapServer);
  }

  private void setProducerWithBasicProperties(String bootstrapServer) {
    Properties producerProperties = new Properties();
    producerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    producerProperties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProperties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    this.kafkaProducer = new KafkaProducer<>(producerProperties);
  }
}
