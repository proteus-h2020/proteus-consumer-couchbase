package eu.proteus.consumer.tasks;

import java.util.ArrayList;
import java.util.Properties;

import com.couchbase.client.java.Bucket;

import eu.proteus.consumer.model.Measurement;

public interface ProteusTask {

	void doWork(int coil, Measurement record, Bucket proteusBucket, ArrayList<String> topicsList);

	void setup(Properties properties);

	void cleanUp();

}
