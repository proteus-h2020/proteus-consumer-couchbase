package eu.proteus.examples;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import eu.proteus.consumer.model.Measurement;
import eu.proteus.consumer.serialization.ProteusSerializer;
import eu.proteus.couchbase.utils.CouchbaseUtils;

public class ExampleFlatness {

	private static Cluster clusterCouchbase;
	private static Bucket proteusBucket;
	private static int documentCounter;

	public static void main(String[] args) {

		CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder().build();

		clusterCouchbase = CouchbaseCluster.create(env, "192.168.4.246", "192.168.4.247", "192.168.4.248");
		proteusBucket = clusterCouchbase.openBucket("proteus-testing");

		ArrayList<String> topicsList = new ArrayList<String>();

		HashMap<String, Object> kafkaProperties = new HashMap<String, Object>();

		topicsList.add("proteus-hsm");
		kafkaProperties.put("bootstrap.servers", "192.168.4.246:6667,192.168.4.247:6667,192.168.4.248:6667");
		kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
		kafkaProperties.put("value.deserializer", ProteusSerializer.class.getName());
		kafkaProperties.put("group.id", "proteus");

		KafkaConsumer<Integer, Measurement> kafkaConsumer;

		kafkaConsumer = new KafkaConsumer<Integer, Measurement>(kafkaProperties, new IntegerDeserializer(),
				new ProteusSerializer());
		kafkaConsumer.subscribe(topicsList);

		try {
			while (true) {
				ConsumerRecords<Integer, Measurement> records = kafkaConsumer.poll(1);
				for (ConsumerRecord<Integer, Measurement> record : records) {
					System.out.println("coilId: " + record.value().getCoilID());
					System.out.println(record.value().toString());
					if (record.value().getCoilID() == 40101001) {
						System.out.println(record.value().getCoilID());

						if (CouchbaseUtils.checkIfDocumentExists(String.valueOf(record.value().getCoilID()),
								proteusBucket)) {
							System.out.println("Update en " + record.value().getCoilID());
							CouchbaseUtils.updateDocument(proteusBucket, topicsList, record.value());
						} else {
							CouchbaseUtils.createDocumentFirstTime(String.valueOf(record.value().getCoilID()),
									record.value(), topicsList, proteusBucket);
						}

					}
				}

			}
		} finally {
			kafkaConsumer.close();
		}

	}
}
