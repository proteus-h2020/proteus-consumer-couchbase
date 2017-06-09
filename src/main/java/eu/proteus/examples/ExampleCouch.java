package eu.proteus.examples;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import eu.proteus.consumer.model.Measurement;
import eu.proteus.consumer.serialization.ProteusSerializer;
import eu.proteus.couchbase.utils.CouchbaseUtils;

public class ExampleCouch {

	private static final Logger logger = LoggerFactory.getLogger(ExampleCouch.class);
	private static Cluster clusterCouchbase;
	private static Bucket proteusBucket;
	private static int documentCounter;

	public static void main(String[] args) {

		CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder().build();

		clusterCouchbase = CouchbaseCluster.create(env, "192.168.4.246", "192.168.4.247", "192.168.4.248");
		proteusBucket = clusterCouchbase.openBucket("proteus-testing");
		documentCounter = 15;

		ArrayList<String> topicsList = new ArrayList<String>();

		HashMap<String, Object> kafkaProperties = new HashMap<String, Object>();

		topicsList.add("proteus-realtime");
		kafkaProperties.put("bootstrap.servers", "192.168.4.246:6667,192.168.4.247:6667,192.168.4.248:6667");
		kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
		kafkaProperties.put("value.deserializer", ProteusSerializer.class.getName());
		kafkaProperties.put("group.id", "proteus");

		KafkaConsumer<Integer, Measurement> kafkaConsumer = new KafkaConsumer<Integer, Measurement>(kafkaProperties);

		ProteusSerializer myValueDeserializer = new ProteusSerializer();
		IntegerDeserializer keyDeserializer = new IntegerDeserializer();

		kafkaConsumer = new KafkaConsumer<>(kafkaProperties, keyDeserializer, myValueDeserializer);
		kafkaConsumer.subscribe(topicsList);

		try {
			while (true) {
				ConsumerRecords<Integer, Measurement> records = kafkaConsumer.poll(1);
				for (ConsumerRecord<Integer, Measurement> record : records) {
					System.out.println("Key: " + record.key());
					if (!CouchbaseUtils.checkIfDocumentExists(String.valueOf(record.key()), proteusBucket)) {
						CouchbaseUtils.createDocumentFirstTime(String.valueOf(record.key()), record.value(), topicsList,
								proteusBucket);
					} else {
						CouchbaseUtils.updateDocument(proteusBucket, topicsList, record.value());
					}
				}
			}

		} finally {
			kafkaConsumer.close();
		}

	}

}
