package eu.proteus.consumer;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import eu.proteus.consumer.exceptions.InvalidTaskTypeException;
import eu.proteus.consumer.utils.ConsumerUtils;
import eu.proteus.consumer.utils.KafkaTopics;
import eu.proteus.consumer.utils.ProteusBuckets;

public class ProteusCouchbase {

    private static final String PROPERTIES_FILE = "src/main/resources/config.properties";

    private static final Logger logger = LoggerFactory.getLogger(Runner.class);
    private List<Runner> runners = new LinkedList<Runner>();

    /*
     * Couchbase Connection
     */
    private static Cluster clusterCouchbase;
    private static Bucket proteusBucket;
    private static CouchbaseEnvironment couchbaseEnvironment;
    private static List<String> nodes;

    private void run(String[] args)
            throws InterruptedException, InvalidTaskTypeException {

        /*
         * The connection to the Couchbase is initialized
         */
        nodes = Arrays.asList("192.168.4.246", "192.168.4.247",
                "192.168.4.248");
        couchbaseEnvironment = DefaultCouchbaseEnvironment.builder().build();
        clusterCouchbase = CouchbaseCluster.create(couchbaseEnvironment, nodes);

        /*
         * One Runner is created for each Kafka Topic. The Runner gets it´s own
         * properties, where the Bucket is included.
         *
         */
        for (KafkaTopics topic : KafkaTopics.values()) {
            /*
             * The Bucket name its taken from ProteusBuckets.class.
             */
            for (ProteusBuckets bucket : ProteusBuckets.values()) {
                proteusBucket = clusterCouchbase
                        .openBucket(bucket.toString().toLowerCase());
            }
            proteusBucket = clusterCouchbase.openBucket("proteus");
            Properties runnerProperties = new Properties();
            runnerProperties = ConsumerUtils
                    .loadPropertiesFromFile(PROPERTIES_FILE);
            runnerProperties.put("eu.proteus.kafkaTopic", topic.name());
            runners.add(new Runner(runnerProperties, proteusBucket));
        }

        /*
         * The Threads are launched from the Runner list.
         *
         */

        for (Runner runner : runners) {
            Thread t = new Thread(runner);
            t.start();
        }

    }

    public static void main(String[] args) throws Exception {
        new ProteusCouchbase().run(args);
    }

}
