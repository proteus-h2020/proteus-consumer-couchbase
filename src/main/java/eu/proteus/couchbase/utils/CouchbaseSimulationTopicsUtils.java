package eu.proteus.couchbase.utils;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.subdoc.DocumentFragment;

import eu.proteus.consumer.model.HSMMeasurement;
import eu.proteus.consumer.model.Measurement;

public class CouchbaseSimulationTopicsUtils {

    private static final Logger logger = LoggerFactory.getLogger("updates");

    public static boolean checkIfDocumentExists(int coil,
            Bucket proteusBucket) {
        JsonDocument checkExists = proteusBucket.get(String.valueOf(coil));
        if (checkExists == null)
            return false;
        else
            return true;
    }

    public static void createDocumentFirstTime(int coilID, Object record,
            ArrayList<String> topicList, Bucket proteusBucket) {
        List<JsonObject> proteusRealtime = new ArrayList<>();
        List<JsonObject> proteusFlatness = new ArrayList<>();

        JsonObject row1d = JsonObject.create();
        JsonObject row2d = JsonObject.create();

        if (((Measurement) record).getType() == 0x00) {
            row1d = JsonObject.empty()
                    .put("x", ((Measurement) record).getPositionX())
                    .put("value", ((Measurement) record).getValue())
                    .put("varId", ((Measurement) record).getVarName());

        }

        if (((Measurement) record).getType() == 0x01) {
            row2d = JsonObject.empty()
                    .put("x", ((Measurement) record).getPositionX())
                    .put("y", ((Measurement) record).getPositionY())
                    .put("value", ((Measurement) record).getValue())
                    .put("varId", ((Measurement) record).getVarName());
        }

        try {

            if (getKafkaTopic(topicList).equals("proteus-realtime")) {
                if (!row1d.isEmpty())
                    proteusRealtime.add(row1d);
                if (!row2d.isEmpty())
                    proteusRealtime.add(row2d);
            }
            if (getKafkaTopic(topicList).equals("proteus-flatness")) {
                if (!row1d.isEmpty())
                    proteusFlatness.add(row1d);
                if (!row2d.isEmpty())
                    proteusFlatness.add(row2d);
            }
        } catch (NullPointerException e) {
            System.out.println(e);
        }

        JsonObject structureProteusDocument = JsonObject.empty()
                .put("coilID", coilID).put("proteus-realtime", proteusRealtime)
                .put("proteus-flatness", proteusFlatness)
                .put("proteus-hsm", ((Measurement) record).getHSMVariables())
                .put("calculations", new ArrayList());
        JsonDocument doc = JsonDocument.create(String.valueOf(coilID),
                structureProteusDocument);
        proteusBucket.upsert(doc);

        logger.debug("New Document - Coil ID: " + coilID
                + " - Proteus-Realtime: " + proteusRealtime
                + ", Proteus-Flatness: " + proteusFlatness + ", Proteus HSM: + "
                + ((Measurement) record).getHSMVariables());

    }

    public static String getKafkaTopic(ArrayList<String> topicList) {
        return topicList.get(0);
    }

    public static void updateDocument(Bucket proteusBucket,
            ArrayList<String> topicsList, Object record) {

        if ((((Measurement) record).getType() == 0x00)
                && (!record.getClass().equals(HSMMeasurement.class))) {
            JsonObject row1d = JsonObject.create();
            row1d = JsonObject.empty()
                    .put("x", ((Measurement) record).getPositionX())
                    .put("value", ((Measurement) record).getValue())
                    .put("varId", ((Measurement) record).getVarName());

            logger.debug("row1d Record: {} --- topicsList {}", row1d,
                    topicsList);

            try {
                proteusBucket.mutateIn(((Measurement) record).getStringCoilID())
                        .arrayAppend(topicsList.get(0), row1d).execute();
                logger.debug("Updating Row1D on Topic: {} for Coil {} ",
                        topicsList.get(0), ((Measurement) record).getCoilID());
                logger.debug("--- Coil: {}",
                        ((Measurement) record).getCoilID());
                logger.debug("--- Position X: {}",
                        ((Measurement) record).getPositionX());
                logger.debug("--- Value: {}",
                        ((Measurement) record).getValue());
                logger.debug("--- Variable: {}",
                        ((Measurement) record).getVarName());
                logger.debug("--- Row1D: {}", row1d);
            } catch (Exception e) {
                logger.debug(
                        "Exception while updating Row1D on Topic: {} for Coil {} ",
                        topicsList.get(0), ((Measurement) record).getCoilID());
                logger.debug("--- Coil: {}",
                        ((Measurement) record).getCoilID());
                logger.debug("--- Position X: {}",
                        ((Measurement) record).getPositionX());
                logger.debug("--- Value: {}",
                        ((Measurement) record).getValue());
                logger.debug("--- Variable: {}",
                        ((Measurement) record).getVarName());
                logger.debug("--- Row1D: {}", row1d);
                logger.debug("Exception updating Row1D:" + e.toString());
            }

        }

        if (((Measurement) record).getType() == 0x01) {
            JsonObject row2d = JsonObject.create();
            row2d = JsonObject.empty()
                    .put("x", ((Measurement) record).getPositionX())
                    .put("y", ((Measurement) record).getPositionY())
                    .put("value", ((Measurement) record).getValue())
                    .put("varId", ((Measurement) record).getVarName());

            logger.debug("row2d Record: {} --- topicsList {}", row2d,
                    topicsList);

            if (!row2d.isEmpty()) {

                try {
                    proteusBucket
                            .mutateIn(((Measurement) record).getStringCoilID())
                            .arrayAppend(topicsList.get(0), row2d).execute();
                    logger.debug("Updating Row2D on Topic: {} for Coil {} ",
                            topicsList.get(0),
                            ((Measurement) record).getCoilID());
                    logger.debug("--- Coil: {}",
                            ((Measurement) record).getCoilID());
                    logger.debug("--- Position X: {}",
                            ((Measurement) record).getPositionX());
                    logger.debug("---- Position Y: {}",
                            ((Measurement) record).getPositionY());
                    logger.debug("--- Value: {}",
                            ((Measurement) record).getValue());
                    logger.debug("--- Variable: {}",
                            ((Measurement) record).getVarName());
                    logger.debug("--- Row2D: {}", row2d);
                } catch (Exception e) {
                    logger.debug(
                            "Exception while updating Row2D on Topic: {} for Coil {} ",
                            topicsList.get(0),
                            ((Measurement) record).getCoilID());
                    logger.debug("--- Coil: {}",
                            ((Measurement) record).getCoilID());
                    logger.debug("--- Position X: {}",
                            ((Measurement) record).getPositionX());
                    logger.debug("---- Position Y: {}",
                            ((Measurement) record).getPositionY());
                    logger.debug("--- Value: {}",
                            ((Measurement) record).getValue());
                    logger.debug("--- Variable: {}",
                            ((Measurement) record).getVarName());
                    logger.debug("--- Row2D: {}", row2d);
                    logger.debug("Exception updating Row2D:" + e.toString());

                }

            }

        }

        if (record.getClass().equals(HSMMeasurement.class)) {
            JsonObject hsm = JsonObject.create();
            hsm = JsonObject.empty().put("variables",
                    ((Measurement) record).getHSMVariables());

            logger.debug("HSM Record: {} --- topicsList {}", hsm, topicsList);

            try {
                proteusBucket.mutateIn(((Measurement) record).getStringCoilID())
                        .replace("proteus-hsm",
                                ((Measurement) record).getHSMVariables())
                        .execute();
                logger.debug("Updating HSM for Coil {} ",
                        ((Measurement) record).getCoilID());
                logger.debug("--- Coil: {}",
                        ((Measurement) record).getCoilID());
                logger.debug("--- HSM: {} ",
                        ((Measurement) record).getHSMVariables());

            } catch (Exception e) {
                logger.debug("Exception while updating HSM for Coil {} ",
                        ((Measurement) record).getCoilID());
                logger.debug("--- Coil: {}",
                        ((Measurement) record).getCoilID());
                logger.debug("--- HSM: {} ",
                        ((Measurement) record).getHSMVariables());
                logger.debug("Exception updating HSM: {}" + e.toString());
            }
        }

    }

    public static void checkDataInserted(String documentNameCoilID,
            Bucket proteusBucket) {
        DocumentFragment<Lookup> resultHSM = proteusBucket
                .lookupIn(documentNameCoilID).get("proteus-hsm").execute();
        DocumentFragment<Lookup> resultFlatness = proteusBucket
                .lookupIn(documentNameCoilID).get("proteus-realtime").execute();
        DocumentFragment<Lookup> resultRealtime = proteusBucket
                .lookupIn(documentNameCoilID).get("proteus-flatness").execute();
        System.out.println("Resultado para la bobina < " + documentNameCoilID
                + " > | < HSM > " + resultHSM.size() + " | < Flantess > "
                + resultFlatness.size() + " | < Realtime > "
                + resultRealtime.size());
    }

    public static JsonObject getSensorMeasurement1D(Measurement record) {
        return null;
    }

    public static JsonObject getSensorMeasurement2D(Measurement record) {
        return null;
    }

    public static JsonObject getSensorHSMMeasurement(Measurement record) {
        return null;
    }

}
