package com.kafka.course.consumer;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafka.course.thread.ConsumerThread;

public class ConsumerDemoThreads {

	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

		CountDownLatch latch = new CountDownLatch(1);
		Runnable consumerThread = new ConsumerThread(latch);

		Thread thread = new Thread(consumerThread);
		thread.start();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Caught shutdown hook");
			((ConsumerThread) consumerThread).shutDown();
		}));
		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("Application got interrupted.", e);
		} finally {
			logger.info("Application closing.");
		}
	}

}
