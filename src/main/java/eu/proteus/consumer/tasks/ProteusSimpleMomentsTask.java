package eu.proteus.consumer.tasks;

import java.util.ArrayList;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.java.Bucket;

import eu.proteus.consumer.model.MomentsResult;
import eu.proteus.consumer.utils.ConsumerUtils;
import eu.proteus.couchbase.utils.CouchbaseSimpleMomentsUtils;

public class ProteusSimpleMomentsTask implements ProteusTask {

    private static final Logger logger = LoggerFactory
            .getLogger(ProteusSimpleMomentsTask.class);

    public ProteusSimpleMomentsTask() {
    }

    @Override
    public void doWork(int coil, Object record, Bucket proteusBucket,
            ArrayList<String> topicList) {

        logger.debug("Simple Moments: "
                + String.valueOf(((MomentsResult) record).getX()) + "//"
                + String.valueOf(((MomentsResult) record).getY()) + "//"
                + String.valueOf(((MomentsResult) record).getVarId()));

        if (CouchbaseSimpleMomentsUtils.checkIfDocumentExists(
                ((MomentsResult) record).getCoilId(), proteusBucket)) {
            CouchbaseSimpleMomentsUtils.updateSimpleMomentsDocument(
                    proteusBucket, topicList, record);
        } else {
            CouchbaseSimpleMomentsUtils.createSimpleMomentsFirstTime(
                    ((MomentsResult) record).getCoilId(), record, topicList,
                    proteusBucket);
        }

    }

    @Override
    public void setup(Properties runnerProperties) {
        logger.info("Task: < " + this.getClass().getName()
                + "  > launched over topic " + ConsumerUtils.getTopicName(
                        runnerProperties.getProperty("eu.proteus.kafkaTopic")));
    }
}
