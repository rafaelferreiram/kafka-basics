package com.kafka.course.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.kafka.course.config.ConfigConstants;

public class ProducerDemo {

	public static void main(String[] args) {

		// create Producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigConstants.bootstrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(ConfigConstants.fisrtTopic, "Hello Kafka");
		
		// send Data --async
		producer.send(record);
		producer.close();
	}
}
	