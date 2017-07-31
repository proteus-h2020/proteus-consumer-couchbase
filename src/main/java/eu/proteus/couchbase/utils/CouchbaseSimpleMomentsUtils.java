package eu.proteus.couchbase.utils;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

import eu.proteus.consumer.model.MomentsResult;

public class CouchbaseSimpleMomentsUtils {

    private static final Logger logger = LoggerFactory.getLogger("updates");
    private static final Logger exceptionsLogger = LoggerFactory
            .getLogger("exceptions");

    public static void createSimpleMomentsFirstTime(int coilID, Object record,
            ArrayList<String> topicLIst, Bucket proteusBucket) {

        try {
            List<JsonObject> proteusSimpleMoments = new ArrayList<>();
            JsonObject simpleMoments = JsonObject.create();
            simpleMoments = CouchbaseCommons
                    .createObjectforInsertIntoCalculationsTopics(record);
            proteusSimpleMoments.add(simpleMoments);

            JsonObject structureProteusDocument = JsonObject.empty()
                    .put("coilID",
                            Integer.toString(
                                    ((MomentsResult) record).getCoilId()))
                    .put("simple-moments", proteusSimpleMoments);

            StringBuilder documentName = new StringBuilder()
                    .append(String.valueOf(coilID));

            JsonDocument doc = JsonDocument.create(documentName.toString(),
                    structureProteusDocument);
            proteusBucket.upsert(doc);
            logger.debug("New document for Coil {} triggered by Simple Moments",
                    coilID);
        } catch (Exception e) {
            exceptionsLogger.debug(
                    "Exception creating Document triggered by Simple Moments");
        }

    }

    public static void updateSimpleMomentsDocument(Bucket proteusBucket,
            ArrayList<String> topicsList, Object record) {

        JsonObject simpleMoments = JsonObject.create();

        try {

            simpleMoments = CouchbaseCommons
                    .createObjectforInsertIntoCalculationsTopics(record);
        } catch (Exception e) {
            exceptionsLogger.debug(
                    "Exception creating Calculations Object" + e.toString());
        }

        if (!CouchbaseCommons.checkIfPropertyExists(proteusBucket,
                ((MomentsResult) record).getCoilId(), topicsList.get(0))) {
            try {

                CouchbaseCommons.createPropertyAndInsertValueforSimpleMoments(
                        proteusBucket, record, topicsList, simpleMoments);
            } catch (Exception e) {
                exceptionsLogger
                        .debug("Exception creating Property and inserting the first value"
                                + e.toString());
            }
        } else {
            try {
                CouchbaseCommons.updateSimpleMomentsValue(proteusBucket, record,
                        topicsList, simpleMoments);
            } catch (Exception e) {
                exceptionsLogger
                        .debug("Exception updating Document. Entry point -> Simple Moments"
                                + e.toString());
            }

        }
    }

}
