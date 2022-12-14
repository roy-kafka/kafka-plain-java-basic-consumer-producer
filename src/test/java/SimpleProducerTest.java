import dev.roy.parreira.SimpleProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@Slf4j
class SimpleProducerTest {

  private static final KafkaProducer<String, String> kafkaProducer = new SimpleProducer().getKafkaProducer();

  @AfterAll
  static void postActions() {
    kafkaProducer.close();
  }

  @Test
  void testProduceValueOnly() {

    ProducerRecord<String, String> simpleProducerRecord =
        new ProducerRecord<>("simple-topic", "Hello world!!!");

    Future<RecordMetadata> promise = kafkaProducer.send(simpleProducerRecord);

    assertNotNull(promise);
  }

  @Test
  void testProduceValueWithCallback() {

    ProducerRecord<String, String> simpleProducerRecord =
        new ProducerRecord<>("simple-topic", "Hello world!!!");

    kafkaProducer.send(simpleProducerRecord, (this::logCallBack));
  }

  @Test
  void testProduceValueWithCallbackSequentiallyFast() {
    for (int i = 0; i < 10; i++) {
      ProducerRecord<String, String> simpleProducerRecord =
          new ProducerRecord<>("simple-topic", String.format("Send message: %d", i));

      kafkaProducer.send(simpleProducerRecord, this::logCallBack);
    }
  }

  private void logCallBack(RecordMetadata metadata, Exception exception) {
    Optional.ofNullable(exception).ifPresentOrElse(
        (ex) -> log.error(ExceptionUtils.getStackTrace(ex)),
        () -> {
          log.info("Received new metadata");
          log.info("Topic: {}", metadata.topic());
          log.info("Partition: {}", metadata.partition());
          log.info("Offset: {}", metadata.offset());
          log.info("Timestamp: {}", metadata.timestamp());
        });
  }

  @Test
  void testProduceMessageWithKey() {
    String targetTopic = "simple-topic";
    for (int i = 0; i < 10; i++) {

      String msgKey = String.format("id_%d", i);
      String msgValue = String.format("Message %d", i);

      ProducerRecord<String, String> simpleProducerRecord =
          new ProducerRecord<>(targetTopic, msgKey, msgValue);

      kafkaProducer.send(simpleProducerRecord, (RecordMetadata metadata, Exception exception) -> {
        if (exception == null) {
          log.info("\n\n{}:{} -> on partition: {}\n", msgKey, msgValue, metadata.partition());
        } else {
          log.error(ExceptionUtils.getStackTrace(exception));
        }
      });
    }
  }

}
