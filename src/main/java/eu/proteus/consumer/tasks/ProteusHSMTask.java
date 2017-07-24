package eu.proteus.consumer.tasks;

import java.util.ArrayList;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.java.Bucket;

import eu.proteus.consumer.model.Measurement;
import eu.proteus.consumer.utils.ConsumerUtils;
import eu.proteus.couchbase.utils.CouchbaseSimulationTopicsUtils;

public class ProteusHSMTask implements ProteusTask {

    private static final Logger logger = LoggerFactory
            .getLogger(ProteusHSMTask.class);

    public ProteusHSMTask() {
    }

    @Override
    public void doWork(int coil, Object record, Bucket proteusBucket,
            ArrayList<String> topicList) {

        logger.info("< " + this.getClass().getName() + " > - Flatness for coil "
                + ((Measurement) record).getCoilID() + " inserted");

        if (CouchbaseSimulationTopicsUtils.checkIfDocumentExists(coil,
                proteusBucket)) {
            CouchbaseSimulationTopicsUtils.updateDocument(proteusBucket,
                    topicList, record);
        } else {
            CouchbaseSimulationTopicsUtils.createDocumentFirstTime(
                    ((Measurement) record).getCoilID(), record, topicList,
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
