package eu.proteus.consumer.tasks;

import java.util.ArrayList;
import java.util.Properties;

import com.couchbase.client.java.Bucket;

public interface ProteusTask {

	void doWork(int coil, Object object, Bucket proteusBucket, ArrayList<String> topicsList);

	void setup(Properties properties);

}
