package com.examples.java.kafka;

//import util.properties packages
import java.util.Properties;
import java.util.concurrent.Future;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

// Transactional Message Producer
public class MessageProducerWithTransaction {

	public static void main(String[] args) throws Exception {

		// default topic
		String topicName = "test";

		if (args.length != 0) {
			topicName = args[0].toString();
		}

		// create instance for properties to access producer configs
		Properties props = new Properties();

		// Assign localhost id
		props.put("bootstrap.servers", "localhost:9092");

		// Set acknowledgements for producer requests.
		props.put("acks", "all");

		// If the request fails, the producer can automatically retry,
		props.put("retries", 1);

		// Specify buffer size in config
		props.put("batch.size", 16384);

		// Reduce the no of requests less than 0
		props.put("linger.ms", 1);

		// The buffer.memory controls the total amount of memory available to the
		// producer for buffering.
		props.put("buffer.memory", 33554432);

		// Unique transaction id
		props.put("transactional.id", "test-transaction-01");

		// Idempotent producer ensures no duplicate message (record) being sent
		// Idempotent be enabled by default for transactional producer with trans id
		props.put("enable.idempotence", true);

		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		// initiate transactions
		producer.initTransactions();

		try {
			producer.beginTransaction();
			for (int i = 0; i < 10; i++) {
				Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>(topicName,
						Integer.toString(i), "Test Message: " + Integer.toString(i)));

				System.out.println("Message sent to Topic - " + future.get().topic() + ", Partition - "
						+ future.get().partition() + " , Offset - " + future.get().offset());

			}
			// commit transaction
			producer.commitTransaction();
			System.out.println("Transaction committed");
			System.out.println("All Messages sent successfully");

		} catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
			// We can't recover from these exceptions, so our only option is to close the
			// producer and exit.
			producer.close();
		} catch (KafkaException ex) {
			ex.printStackTrace();
			// For all other exceptions, just abort the transaction and try again.
			producer.abortTransaction();
			System.out.println("Transaction aborted..");
		}

		// close the producer
		producer.close();
	}
}