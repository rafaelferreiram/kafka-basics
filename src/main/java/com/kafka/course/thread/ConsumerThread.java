package com.kafka.course.thread;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafka.course.config.ConfigConstants;

public class ConsumerThread implements Runnable {

	private Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
	private CountDownLatch latch;
	private KafkaConsumer<String, String> consumer;

	public ConsumerThread(CountDownLatch latch) {
		this.latch = latch;

		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigConstants.bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, ConfigConstants.fithGroupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ConfigConstants.earliestOffset);

		consumer = new KafkaConsumer<String, String>(properties);

		// subscribe consumer
		consumer.subscribe(Collections.singleton(ConfigConstants.fisrtTopic));
	}

	public void shutDown() {
		// interrupt the poll
		consumer.wakeup();
	}

	public void run() {
		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

				for (ConsumerRecord<String, String> record : records) {
					logger.info("Key: " + record.key() + "\n" + "Value: " + record.value() + "\n" + "Partition: "
							+ record.partition() + "\n" + "Offset: " + record.offset());
				}
			}
		} catch (WakeupException e) {
			logger.info("Received Shutdown signal.");
		} finally {
			consumer.close();
			latch.countDown(); // finishing ssaying we're done with the consumer
		}

	}
}
