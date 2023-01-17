package com.examples.java.kafka;

import java.util.Properties;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ConsumerWithPartitionAssignment {
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
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

		// *******************************************************************
		// Kafka consumer subscription should be disabled for manual assignment
		// Dynamic assignment will not work when opting for manual assignment
		// consumer.subscribe(Arrays.asList(topicName));
		// ********************************************************************

		// Manual partition assignment
		// Consumer Group assignment will have no effect for manual assignment
		// When multiple consumers assigned to same partition each will act as separate
		consumer.assign(Arrays.asList(new TopicPartition(topicName, 0), new TopicPartition(topicName, 1),
				new TopicPartition(topicName, 2)));

		// print the topic name
		System.out.println("Subscribed to topic - " + topicName);
		// print list of partitions
		System.out.println("List of partitions - " + consumer.partitionsFor(topicName));
		// print list of partitions assigned
		System.out.println("Partition assigned - " + consumer.assignment());

		while (running) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : records)

				// print the topic, partition, offset, timestamp, key and value for each record
				System.out.printf("topic = %s, partition = %d, offset = %d, timestamp = %d, key = %s, value = %s\n",
						record.topic(), record.partition(), record.offset(), record.timestamp(), record.key(),
						record.value());
		}
		
		// close the consumer
		consumer.close();		
	}
}