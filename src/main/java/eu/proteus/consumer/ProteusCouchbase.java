package eu.proteus.consumer;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import eu.proteus.consumer.exceptions.InvalidTaskTypeException;
import eu.proteus.consumer.utils.KafkaTopics;
import eu.proteus.producer.utils.ConsumerUtils;

public class ProteusCouchbase {

	private static final String PROPERTIES_FILE = "src/main/resources/config.properties";

	private static final Logger logger = LoggerFactory.getLogger(Runner.class);
	private List<Runner> runners = new LinkedList<Runner>();
	private static ExecutorService service = Executors.newFixedThreadPool(3);
	private InputStream inputStream;

	// Couchbase Connection
	private static Cluster clusterCouchbase;
	private static Bucket proteusBucket;
	private static CouchbaseEnvironment couchbaseEnvironment;
	private static List<String> nodes;

	// Kafka Connection
	private ArrayList<String> topicsList;

	private void run(String[] args) throws InterruptedException, InvalidTaskTypeException {

		nodes = Arrays.asList("192.168.4.246", "192.168.4.247", "192.168.4.248");
		couchbaseEnvironment = DefaultCouchbaseEnvironment.builder().build();
		clusterCouchbase = CouchbaseCluster.create(couchbaseEnvironment, nodes);
		proteusBucket = clusterCouchbase.openBucket("proteus");

		for (KafkaTopics topic : KafkaTopics.values()) {
			Properties runnerProperties = new Properties();
			runnerProperties = ConsumerUtils.loadPropertiesFromFile(PROPERTIES_FILE);
			runnerProperties.put("eu.proteus.kafkaTopic", topic.name());

			runners.add(new Runner(runnerProperties, proteusBucket));
		}

		if (!service.isShutdown()) {
			for (Runner runner : runners) {
				Thread t = new Thread(runner);
				t.start();
				// service.execute(runner);
			}
		}

		service.shutdownNow();

	}

	public static void main(String[] args) throws Exception {
		new ProteusCouchbase().run(args);
	}

}
