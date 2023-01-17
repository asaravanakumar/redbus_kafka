package com.examples.java.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

public class ConsumerAsyncCommit {
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
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

		// Kafka Consumer subscribes list of topics here.
		consumer.subscribe(Arrays.asList(topicName));

		// print the topic name
		System.out.println("Subscribed to topic - " + topicName);
		System.out.println("List of partitions - " + consumer.partitionsFor(topicName));

		while (running) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : records) {

				// print the offset,key and value for the consumer records.
				System.out.printf("topic = %s, partition = %d, offset = %d, key = %s, value = %s\n", record.topic(),
						record.partition(), record.offset(), record.key(), record.value());
			}

			// commit the offset Asynchronously
			consumer.commitAsync();
		}

		// close the consumer
		consumer.close();
	}
}
