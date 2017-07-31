package eu.proteus.couchbase.utils;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.subdoc.DocumentFragment;

import eu.proteus.consumer.model.Measurement;
import eu.proteus.consumer.model.MomentsResult;

public class CouchbaseCommons {

    private static final Logger logger = LoggerFactory.getLogger("updates");
    private static final Logger exceptionsLogger = LoggerFactory
            .getLogger("exceptions");

    /*
     * This method checks if the document exists. Returns true, if exists, or
     * false.
     *
     */

    public static boolean checkIfDocumentExists(int coil,
            Bucket proteusBucket) {
        JsonDocument checkExists = proteusBucket.get(String.valueOf(coil));
        if (checkExists == null)
            return false;
        else
            return true;
    }

    /*
     * Get the topic name where the data is originated. This name it´s used to
     * put the data in the properly bucket.
     *
     */

    public static String getKafkaTopic(ArrayList<String> topicList) {
        return topicList.get(0);
    }

    /*
     * This function checks if the property ( proteus-realtime,
     * proteus-flatness, proteus-hsm or simple-moments) exists in the document.
     * 
     */

    public static boolean checkIfPropertyExists(Bucket proteusBucket,
            int coilId, String path) {

        DocumentFragment<Lookup> resultado = proteusBucket
                .lookupIn(String.valueOf(coilId)).exists(path).execute();
        boolean pathExist = resultado.content(path, Boolean.class);
        return pathExist;
    }

    /*
     * This method creates JsonObjets, either 1D row, 2D row or HSM row, to
     * insert into the simulation topics.
     * 
     */

    public static JsonObject createObjectforInsertIntoSimulationTopics(
            Object record) {

        if (((Measurement) record).getType() == 0x00) {
            JsonObject row1d = JsonObject.empty()
                    .put("x", ((Measurement) record).getPositionX())
                    .put("value", ((Measurement) record).getValue())
                    .put("varId", ((Measurement) record).getVarName());
            return row1d;
        }

        if (((Measurement) record).getType() == 0x01) {
            JsonObject row2d = JsonObject.empty()
                    .put("x", ((Measurement) record).getPositionX())
                    .put("y", ((Measurement) record).getPositionY())
                    .put("value", ((Measurement) record).getValue())
                    .put("varId", ((Measurement) record).getVarName());
            return row2d;
        }

        return null;

    }

    /*
     * This method creates JsonObjets to insert into the simple moments topics.
     *
     */

    public static JsonObject createObjectforInsertIntoCalculationsTopics(
            Object record) {
        JsonObject simpleMoments = JsonObject.create();

        simpleMoments = JsonObject.empty()
                .put("varId", ((MomentsResult) record).getVarId())
                .put("mean", ((MomentsResult) record).getMean())
                .put("variance", ((MomentsResult) record).getVariance())
                .put("counter", ((MomentsResult) record).getCounter())
                .put("stdDeviation",
                        Math.sqrt(((MomentsResult) record).getVariance()))
                .put("x", ((MomentsResult) record).getX())
                .put("y", ((MomentsResult) record).getY());

        return simpleMoments;
    }

    /*
     * Updates streaming topics values.
     *
     */

    public static void updateValue(Bucket proteusBucket, Object record,
            ArrayList<String> topicsList, JsonObject row) {
        proteusBucket.mutateIn(((Measurement) record).getStringCoilID())
                .arrayAppend(topicsList.get(0), row).execute();
        logger.debug(
                "Updating row on {} path: {} for Coil "
                        + ((Measurement) record).getCoilID(),
                row, topicsList.get(0));

    }

    /*
     * Updates simple moments values.
     *
     */

    public static void updateSimpleMomentsValue(Bucket proteusBucket,
            Object record, ArrayList<String> topicsList, JsonObject row) {
        proteusBucket
                .mutateIn(String.valueOf(((MomentsResult) record).getCoilId()))
                .arrayAppend(topicsList.get(0), row).execute();
        logger.debug(
                "Updating row on {} path: {} for Coil "
                        + ((MomentsResult) record).getCoilId(),
                row, topicsList.get(0));

    }

    /*
     * Create a new simulation property and inserts the first value into it.
     *
     */

    public static void createPropertyAndInsertValue(Bucket proteusBucket,
            Object record, ArrayList<String> topicsList, JsonObject row) {
        proteusBucket.mutateIn(((Measurement) record).getStringCoilID())
                .upsert(topicsList.get(0), new ArrayList())
                .arrayAppend(topicsList.get(0), row).execute();
        logger.debug(
                "Updating row on {} path: {} for Coil "
                        + ((Measurement) record).getCoilID(),
                row, topicsList.get(0));

    }

    /*
     * Create a new simple moments property and inserts the first value into it
     *
     */

    public static void createPropertyAndInsertValueforSimpleMoments(
            Bucket proteusBucket, Object record, ArrayList<String> topicsList,
            JsonObject row) {
        proteusBucket
                .mutateIn(String.valueOf(((MomentsResult) record).getCoilId()))
                .upsert(topicsList.get(0), new ArrayList())
                .arrayAppend(topicsList.get(0), row).execute();
        logger.debug(
                "Updating row on {} path: {} for Coil "
                        + String.valueOf(((MomentsResult) record).getCoilId()),
                row, topicsList.get(0));

    }

}
