package eu.proteus.couchbase.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

import eu.proteus.consumer.model.HSMMeasurement;
import eu.proteus.consumer.model.Measurement;

public class CouchbaseSimulationTopicsUtils {

    private static final Logger logger = LoggerFactory.getLogger("updates");
    private static final Logger exceptionsLogger = LoggerFactory
            .getLogger("exceptions");

    public static void createDocumentFirstTime(int coilID, Object record,
            ArrayList<String> topicList, Bucket proteusBucket) {

        List<JsonObject> proteusRealtime = new ArrayList<>();
        List<JsonObject> proteusFlatness = new ArrayList<>();

        JsonObject row = JsonObject.create();

        row = CouchbaseCommons
                .createObjectforInsertIntoSimulationTopics(record);

        try {
            if (CouchbaseCommons.getKafkaTopic(topicList)
                    .equals("proteus-realtime")) {
                if (!row.isEmpty())
                    proteusRealtime.add(row);
            }
            if (CouchbaseCommons.getKafkaTopic(topicList)
                    .equals("proteus-flatness")) {
                if (!row.isEmpty())
                    proteusFlatness.add(row);
            }
        } catch (NullPointerException e) {
            exceptionsLogger.debug("Invalid Kafka Topics.");
        }

        Map<String, Object> proteusHSM = ((Measurement) record)
                .getHSMVariables();

        /*
         * Only create the coilID and the Property where the row is allocated.
         */

        if (!proteusRealtime.isEmpty()) {
            JsonObject structureProteusDocument = JsonObject.empty()
                    .put("coilID", coilID)
                    .put("proteus-realtime", proteusRealtime);
            JsonDocument doc = JsonDocument.create(String.valueOf(coilID),
                    structureProteusDocument);
            proteusBucket.upsert(doc);
        }

        if (!proteusFlatness.isEmpty()) {
            JsonObject structureProteusDocument = JsonObject.empty()
                    .put("coilID", coilID)
                    .put("proteus-flatness", proteusFlatness);
            JsonDocument doc = JsonDocument.create(String.valueOf(coilID),
                    structureProteusDocument);
            proteusBucket.upsert(doc);
        }

        if (!proteusHSM.isEmpty()) {
            if (!proteusRealtime.isEmpty()) {
                JsonObject structureProteusDocument = JsonObject.empty()
                        .put("coilID", coilID).put("proteus-hsm", proteusHSM);
                JsonDocument doc = JsonDocument.create(String.valueOf(coilID),
                        structureProteusDocument);
                proteusBucket.upsert(doc);
            }
        }

        logger.debug("New Document for Coil ID: {}", coilID);

    }

    public static void updateDocument(Bucket proteusBucket,
            ArrayList<String> topicsList, Object record) {

        JsonObject row = JsonObject.create();
        row = CouchbaseCommons
                .createObjectforInsertIntoSimulationTopics(record);

        if ((!record.getClass().equals(HSMMeasurement.class))) {

            try {

                if (CouchbaseCommons.checkIfPropertyExists(proteusBucket,
                        ((Measurement) record).getCoilID(),
                        topicsList.get(0))) {
                    CouchbaseCommons.updateValue(proteusBucket, record,
                            topicsList, row);
                } else {
                    CouchbaseCommons.createPropertyAndInsertValue(proteusBucket,
                            record, topicsList, row);

                }
            } catch (Exception e) {
                exceptionsLogger.debug(
                        "Exception while updating Row on Topic: {} for Coil {} ---> "
                                + e.toString(),
                        topicsList.get(0), ((Measurement) record).getCoilID());

            }

        }

        if (record.getClass().equals(HSMMeasurement.class))

        {
            JsonObject hsm = JsonObject.create();
            hsm = JsonObject.empty().put("variables",
                    ((Measurement) record).getHSMVariables());

            logger.debug("HSM Record: {} --- topicsList {}", hsm, topicsList);

            try {
                if (!CouchbaseCommons.checkIfPropertyExists(proteusBucket,
                        ((Measurement) record).getCoilID(),
                        topicsList.get(0))) {

                    proteusBucket
                            .mutateIn(((Measurement) record).getStringCoilID())
                            .upsert(topicsList.get(0),
                                    ((Measurement) record).getHSMVariables())
                            .execute();
                    logger.debug(
                            "Updating Row2D for first time no Topic: {} for Coil {} ",
                            topicsList.get(0),
                            ((Measurement) record).getCoilID());
                }
            } catch (Exception e) {
                exceptionsLogger.debug(
                        "Exception while updating Row on Topic: {} for Coil {} ---> "
                                + e.toString(),
                        topicsList.get(0), ((Measurement) record).getCoilID());

            }
        }

    }

}
