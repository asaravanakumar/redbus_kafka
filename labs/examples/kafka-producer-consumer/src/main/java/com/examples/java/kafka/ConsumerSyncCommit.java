package com.examples.java.kafka;

import java.util.Properties;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ConsumerSyncCommit {
	public static void main(String[] args) throws Exception {
		// default topic
		String topicName = "test";

		if(args.length != 0){
			topicName = args[0].toString();
		}
		
		boolean running = true;
		Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test-group");
		props.put("enable.auto.commit", "false");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		// Kafka Consumer subscribes list of topics here.
		consumer.subscribe(Arrays.asList(topicName));

		// print the topic name
		System.out.println("Subscribed to topic - " + topicName);
		// print list of partitions
		System.out.println("List of partitions - " + consumer.partitionsFor(topicName));

		// print last committed offset of given partition
		// System.out.println("Last committed offset: " + consumer.committed(new
		// TopicPartition(topicName, 0)));

		while (running) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			// AT MOST ONCE: commit the offset of all messages synchronously before
			// processing
//			 consumer.commitSync();
			for (ConsumerRecord<String, String> record : records) {

				// print the offset,key and value for the consumer records.
				System.out.printf("topic = %s, partition = %d, offset = %d, key = %s, value = %s\n", record.topic(),
						record.partition(), record.offset(), record.key(), record.value());

				// AL MOST ONCE: commit the offset of every processed message synchronously
				consumer.commitSync(Collections.singletonMap(new TopicPartition(topicName, record.partition()),
						new OffsetAndMetadata(record.offset() + 1)));
			}

			// AT LEAST ONCE: commit the offset of all processed message synchronously
			// consumer.commitSync();
	}
		
		// close the consumer
		consumer.close();		

	}
}