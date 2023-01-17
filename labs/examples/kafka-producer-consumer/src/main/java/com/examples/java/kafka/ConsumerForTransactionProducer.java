package com.examples.java.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerForTransactionProducer {
   public static void main(String[] args) throws Exception {
	  String topicName = "test";
	  
      if(args.length != 0){
    	 topicName = args[0].toString();
      }
      
      //Kafka consumer configuration settings
      Properties props = new Properties();
      
      props.put("bootstrap.servers", "localhost:9092");
      props.put("group.id", "test-group");
      props.put("enable.auto.commit", "true");
      props.put("auto.commit.interval.ms", "1000");
      props.put("session.timeout.ms", "30000");
      props.put("isolation.level", "read_committed");
      props.put("key.deserializer", 
         "org.apache.kafka.common.serialization.StringDeserializer");
      props.put("value.deserializer", 
         "org.apache.kafka.common.serialization.StringDeserializer");
      KafkaConsumer<String, String> consumer = new KafkaConsumer
         <String, String>(props);
      
      //Kafka Consumer subscribes list of topics here.
      consumer.subscribe(Arrays.asList(topicName));
      
      //print the topic name
      System.out.println("Subscribed to topic - " + topicName);
      
      while (true) {
         ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
         for (ConsumerRecord<String, String> record : records)
         
         // print the topic, partition, offset, timestamp, key and value for the consumer records.
         System.out.printf("topic = %s, partition = %d, offset = %d, timestamp = %d, key = %s, value = %s\n", 
            record.topic(), record.partition(), record.offset(), record.timestamp(), record.key(), record.value());
      }
   }
}