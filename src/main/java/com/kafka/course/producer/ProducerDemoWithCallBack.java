package com.kafka.course.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafka.course.config.ConfigConstants;

public class ProducerDemoWithCallBack {

	public static void main(String[] args) {

		final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

		// create Producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigConstants.bootstrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		ProducerRecord<String, String> record = new ProducerRecord<String, String>(ConfigConstants.fisrtTopic,
				"Hello Kafka");

		// send Data --async
		producer.send(record, new Callback() {

			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if (exception == null) {
					logger.info("Received new metadata: \n" 
							+ "Topic: "     + metadata.topic() + "\n" 
							+ "Partition: " + metadata.partition() + "\n" 
							+ "Offsets: "   + metadata.offset() + "\n" 
							+ "Timestamp: " + metadata.timestamp());
				}else {
					logger.error("Error while producing.",exception);
				}

			}
		});
		producer.close();
	}

}
