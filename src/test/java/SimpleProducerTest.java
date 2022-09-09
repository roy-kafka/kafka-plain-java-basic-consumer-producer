import dev.roy.parreira.SimpleProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@Slf4j
class SimpleProducerTest {

  private final KafkaProducer<String, String> kafkaProducer = new SimpleProducer().getKafkaProducer();

  @Test
  void testProduceValueOnly() {

    ProducerRecord<String, String> simpleProducerRecord =
        new ProducerRecord<>("simple-topic", "Hello world!!!");

    Future<RecordMetadata> promise = kafkaProducer.send(simpleProducerRecord);
    kafkaProducer.flush();
    kafkaProducer.close();

    assertNotNull(promise);

  }

  @Test
  void testProduceValueWithCallback() {

    ProducerRecord<String, String> simpleProducerRecord =
        new ProducerRecord<>("simple-topic", "Hello world!!!");

    kafkaProducer.send(simpleProducerRecord, ((metadata, exception) ->
        Optional.ofNullable(exception).ifPresentOrElse(
            (ex) -> log.error(ExceptionUtils.getStackTrace(ex)),
            () -> {
              log.info("Received new metadata");
              log.info("Topic: {}", metadata.topic());
              log.info("Partition: {}", metadata.partition());
              log.info("Offset: {}", metadata.offset());
              log.info("Timestamp: {}", metadata.timestamp());
            })
    ));

    kafkaProducer.flush();
    kafkaProducer.close();

  }

}
