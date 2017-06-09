package eu.proteus.consumer;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.java.Bucket;

import eu.proteus.consumer.exceptions.InvalidTaskTypeException;
import eu.proteus.consumer.model.Measurement;
import eu.proteus.consumer.serialization.ProteusSerializer;
import eu.proteus.consumer.tasks.ProteusTask;
import eu.proteus.consumer.utils.ProteusTaskType;
import eu.proteus.producer.utils.ConsumerUtils;

public class Runner implements Runnable {

	private Properties runnerProperties = new Properties();
	private Bucket proteusBucket;
	private ProteusTask task;
	private static final Logger logger = LoggerFactory.getLogger(Runner.class);

	// Kafka
	private KafkaConsumer<Integer, Measurement> kafkaConsumer;
	private ArrayList<String> topicsList;

	public Runner(Properties properties, Bucket proteusBucket) {
		this.runnerProperties = properties;
		topicsList = new ArrayList<String>();
		this.proteusBucket = proteusBucket;
	}

	@Override
	public void run() {
		setup(runnerProperties);

	}

	public void setup(Properties properties) {

		try {
			task = ProteusTaskType.from((String) properties.get("eu.proteus.kafkaTopic"));
			task.setup(properties);

			if (task != null) {
				doWork(properties, task);
			}
		} catch (InvalidTaskTypeException e) {
			e.printStackTrace();
		}

	}

	/*
	 * doWork(Properties properties, ProteusTas task)
	 * 
	 * Read from de Kafka Topic, deserialize the record and send to the
	 * correspondent ProteusTask to insert into the Couchbase.
	 */

	public void doWork(Properties properties, ProteusTask task) {

		topicsList.add(ConsumerUtils.getTopicName(runnerProperties.getProperty("eu.proteus.kafkaTopic")));
		properties.put("bootstrap.servers", properties.get("com.treelogic.proteus.kafka.bootstrapServers"));
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		properties.put("value.deserializer", ProteusSerializer.class.getName());
		properties.put("group.id",
				"proteus-" + ConsumerUtils.getTopicName(runnerProperties.getProperty("eu.proteus.kafkaTopic")));
		properties.put("max.poll.records", 100);
		properties.put("session.timeout.ms", 60000);
		properties.put("request.timeout.ms", 80000);
		properties.put("fetch.max.wati.ms", 60000);
		properties.put("auto.offset.reset", "latest");

		kafkaConsumer = new KafkaConsumer<>(properties, new IntegerDeserializer(), new ProteusSerializer());
		kafkaConsumer.subscribe(topicsList);

		try {
			while (true) {
				ConsumerRecords<Integer, Measurement> records = kafkaConsumer.poll(Long.MAX_VALUE);
				for (ConsumerRecord<Integer, Measurement> record : records) {
					logger.info("Task " + this.getClass().getSimpleName() + " doing work for coil "
							+ record.value().getCoilID() + " on topic "
							+ ConsumerUtils.getTopicName(runnerProperties.getProperty("eu.proteus.kafkaTopic")));
					task.doWork(record.key(), record.value(), proteusBucket, topicsList);
				}

			}
		} finally {
			System.out.println("Cerrariamos la ejecución del hilo < "
					+ this.runnerProperties.getProperty("eu.proteus.kafkaTopic") + " >");
		}

	}

}
