package eu.proteus.consumer.tasks;

import java.util.ArrayList;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.java.Bucket;

import eu.proteus.consumer.model.Measurement;
import eu.proteus.couchbase.utils.CouchbaseUtils;
import eu.proteus.producer.utils.ConsumerUtils;

public class ProteusStreamingTask implements ProteusTask {

	private static final Logger logger = LoggerFactory.getLogger(ProteusStreamingTask.class);

	public ProteusStreamingTask() {
	}

	@Override
	public void doWork(int coil, Measurement record, Bucket proteusBucket, ArrayList<String> topicList) {

		if (CouchbaseUtils.checkIfDocumentExists(String.valueOf(coil), proteusBucket)) {
			CouchbaseUtils.updateDocument(proteusBucket, topicList, record);
		} else {
			CouchbaseUtils.createDocumentFirstTime(String.valueOf(record.getCoilID()), record, topicList,
					proteusBucket);
		}
	}

	@Override
	public void setup(Properties runnerProperties) {
		logger.info("Task: < " + this.getClass().getName() + "  > launched over topic "
				+ ConsumerUtils.getTopicName(runnerProperties.getProperty("eu.proteus.kafkaTopic")));
	}

}
