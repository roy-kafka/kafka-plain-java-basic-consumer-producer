import dev.roy.parreira.SimpleConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.Test;

import java.time.Duration;

@Slf4j
class SimpleConsumerTest {

  private static final KafkaConsumer<String, String> kafkaConsumer = new SimpleConsumer().getKafkaConsumer();

  @Test
  void consumeFromSimpleTopic() {

    // Getting a reference to the current thread
    Thread mainThread = Thread.currentThread();

    // Adding a shutdown hook
    Runtime.getRuntime().addShutdownHook(
        new Thread(() -> {
          log.info("Detected a shutdown, let's exit by calling consumer.wakeup()");
          kafkaConsumer.wakeup();

          // Join main thread to allow the execution of the code on main thread
          try {
            mainThread.join();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        })
    );

    try {

      while (true) {
        log.info("Polling");

        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100L));

        consumerRecords.forEach(record -> {
          log.info("Key:{} - Message: {}", record.key(), record.value());
          log.info("Partition: {} - Offset: {}", record.partition(), record.offset());
        });

      }

    } catch (WakeupException wakeupException) {
      // Expected Exception
      log.info("Wake up exception!");

    } catch (Exception exception) {

      log.error("Unexpected exception!! {}", ExceptionUtils.getStackTrace(exception));

    } finally {

      kafkaConsumer.close();
      log.info("Consumer gracefully closed!");

    }
  }
}
