package dev.roy.parreira;

import lombok.Data;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.List;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

@Data
public class SimpleConsumer {

  private KafkaConsumer<String, String> kafkaConsumer;

  public SimpleConsumer() {
    Properties properties = new Properties();
    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(GROUP_ID_CONFIG, "simple-group-id");
    properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");

    kafkaConsumer = new KafkaConsumer<>(properties);
    kafkaConsumer.subscribe(List.of("simple-topic"));
  }
}
