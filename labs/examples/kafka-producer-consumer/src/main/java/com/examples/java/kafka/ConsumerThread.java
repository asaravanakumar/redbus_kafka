package com.examples.java.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

public class ConsumerThread implements Runnable {
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private final KafkaConsumer<String, String> consumer;
	private final String topicName;

	public ConsumerThread(KafkaConsumer<String, String> consumer, String topicName) {
		this.consumer = consumer;
		this.topicName = topicName;
	}

	public void run() {
		System.out.println("Consumer thread running...");
		try {
			consumer.subscribe(Arrays.asList(topicName));
			while (!closed.get()) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
				// Handle new records
				for (ConsumerRecord<String, String> record : records)

					// print the topic, partition, offset, timestamp, key and value for the record
					System.out.printf("topic = %s, partition = %d, offset = %d, timestamp = %d, key = %s, value = %s\n",
							record.topic(), record.partition(), record.offset(), record.timestamp(), record.key(),
							record.value());
			}
		} catch (WakeupException e) {
			// Ignore exception if closing
			if (!closed.get())
				throw e;
		} finally {
			consumer.close();
		}
	}

	// Shutdown hook which can be called from a separate thread
	public void shutdown() {
		closed.set(true);
		consumer.wakeup();
	}
}
