package com.examples.java.kafka;

import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerMain {

	public static void main(String[] args) {

		// default topic
		String topicName = "test";

		if(args.length == 0){
			topicName = args[0].toString();
		}
		
		Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test-group");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

		System.out.println("Starting consumer thread...");
		ConsumerThread consumerThread = new ConsumerThread(consumer, topicName);
		consumerThread.run();

		// Registering shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				System.out.println("Shutting down consumer thread...");
				consumerThread.shutdown();
			}
		});
	}
}
