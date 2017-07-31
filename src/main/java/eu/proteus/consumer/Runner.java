package eu.proteus.consumer;

import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.java.Bucket;

import eu.proteus.consumer.exceptions.InvalidTaskTypeException;
import eu.proteus.consumer.serialization.ProteusSerializer;
import eu.proteus.consumer.tasks.ProteusTask;
import eu.proteus.consumer.utils.ConsumerUtils;
import eu.proteus.consumer.utils.ProteusTaskType;

public class Runner implements Runnable {

    // Logger
    private static final Logger logger = LoggerFactory.getLogger(Runner.class);

    // General
    private Properties runnerProperties = new Properties();
    private Bucket proteusBucket;
    private ProteusTask task;

    // Kafka
    private KafkaConsumer<Integer, Object> kafkaConsumer;
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
            /*
             * The Runner creates its own Task. This Task it´s selected by the
             * Kafka Topic name and starts putting the data into the Bucket.
             *
             */

            task = ProteusTaskType
                    .from((String) properties.get("eu.proteus.kafkaTopic"));
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

        topicsList.add(ConsumerUtils.getTopicName(
                runnerProperties.getProperty("eu.proteus.kafkaTopic")));
        properties.put("bootstrap.servers",
                properties.get("com.treelogic.proteus.kafka.bootstrapServers"));
        properties.put("key.deserializer",
                "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.deserializer", ProteusSerializer.class.getName());
        properties.put("group.id", UUID.randomUUID().toString());
        properties.put("max.poll.records", 100);
        properties.put("session.timeout.ms", 60000);
        properties.put("request.timeout.ms", 80000);
        properties.put("fetch.max.wati.ms", 60000);
        properties.put("auto.offset.reset", "latest");

        kafkaConsumer = new KafkaConsumer<>(properties,
                new IntegerDeserializer(), new ProteusSerializer());
        kafkaConsumer.subscribe(topicsList);

        try {
            while (true) {
                ConsumerRecords<Integer, Object> records = kafkaConsumer
                        .poll(Long.MAX_VALUE);
                for (ConsumerRecord<Integer, Object> record : records) {
                    if (topicsList.contains("simple-moments")) {
                        task.doWork(0, record.value(), proteusBucket,
                                topicsList);
                    } else
                        task.doWork(record.key(), record.value(), proteusBucket,
                                topicsList);
                }
            }
        } finally {
            logger.error("Kill thread for topic:  < "
                    + this.runnerProperties.getProperty("eu.proteus.kafkaTopic")
                    + " >");
        }
    }
}
