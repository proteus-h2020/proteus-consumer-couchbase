package eu.proteus.couchbase.utils;

import java.util.ArrayList;
import java.util.List;

import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.subdoc.DocumentFragment;

import eu.proteus.consumer.model.HSMMeasurement;
import eu.proteus.consumer.model.Measurement;
import eu.proteus.consumer.model.MomentsResult;

public class CouchbaseSimulationTopicsUtils {

    public static boolean checkIfDocumentExists(int coil,
            Bucket proteusBucket) {
        JsonDocument checkExists = proteusBucket.get(String.valueOf(coil));
        if (checkExists == null)
            return false;
        else
            return true;
    }

    public static void createSimpleMomentsFirstTime(int coilID, Object record,
            ArrayList<String> topicLIst, Bucket proteusBucket) {

        List<JsonObject> proteusSimpleMoments = new ArrayList<>();
        JsonObject simpleMoments = JsonObject.create();
        simpleMoments = JsonObject.empty()
                .put("coil", ((MomentsResult) record).getCoilId())
                .put("var-id", ((MomentsResult) record).getVarId())
                .put("mean", ((MomentsResult) record).getMean())
                .put("variance", ((MomentsResult) record).getVariance())
                .put("counter", ((MomentsResult) record).getCounter())
                .put("stdDeviation",
                        ((MomentsResult) record).getStdDeviation());
        proteusSimpleMoments.add(simpleMoments);
        JsonObject structureProteusDocument = JsonObject.empty()
                .put("simple-moments-coilID", coilID)
                .put("simple-moments", simpleMoments);

        StringBuilder head = new StringBuilder().append("Simple-Moments")
                .append("-").append(coilID);

        JsonDocument doc = JsonDocument.create(head.toString(),
                structureProteusDocument);
        proteusBucket.upsert(doc);

    }

    public static void updateSimpleMomentsDocument(Bucket proteusBucket,
            ArrayList<String> topicsList, Object record) {

        System.out.println("Update SimpleMoments");

        StringBuilder head = new StringBuilder().append("Simple-Moments")
                .append("-").append(((MomentsResult) record).getCoilId());

        JsonObject simpleMoments = JsonObject.create();
        simpleMoments = JsonObject.empty()
                .put("coil", ((MomentsResult) record).getCoilId())
                .put("var-id", ((MomentsResult) record).getVarId())
                .put("mean", ((MomentsResult) record).getMean())
                .put("variance", ((MomentsResult) record).getVariance())
                .put("counter", ((MomentsResult) record).getCounter())
                .put("stdDeviation",
                        ((MomentsResult) record).getStdDeviation());
        proteusBucket.mutateIn(head.toString())
                .arrayAppend(topicsList.get(0), simpleMoments).execute();
    }

    public static void createDocumentFirstTime(int coilID, Object record,
            ArrayList<String> topicList, Bucket proteusBucket) {
        List<JsonObject> proteusRealtime = new ArrayList<>();
        List<JsonObject> proteusHSM = new ArrayList<>();
        List<JsonObject> proteusFlatness = new ArrayList<>();

        JsonObject row1d = JsonObject.create();
        JsonObject row2d = JsonObject.create();
        JsonObject hsm = JsonObject.create();

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

        if (record.getClass().equals(HSMMeasurement.class)) {
            hsm = JsonObject.empty().put("variables",
                    ((Measurement) record).getHSMVariables());

        }

        try {

            if (getKafkaTopic(topicList).equals("proteus-realtime")) {
                if (!row1d.isEmpty())
                    proteusRealtime.add(row1d);
                if (!row2d.isEmpty())
                    proteusRealtime.add(row2d);
            }
            if (getKafkaTopic(topicList).equals("proteus-hsm")) {
                if (!hsm.isEmpty())
                    proteusHSM.add(hsm);
            }
            if (getKafkaTopic(topicList).equals("proteus-flatness")) {
                if (!row1d.isEmpty())
                    proteusRealtime.add(row1d);
                if (!row2d.isEmpty())
                    proteusRealtime.add(row2d);
            }
        } catch (NullPointerException e) {
            System.out.println(e);
        }

        JsonObject structureProteusDocument = JsonObject.empty()
                .put("coilID", coilID).put("proteus-realtime", proteusRealtime)
                .put("proteus-flatness", proteusFlatness)
                .put("proteus-hsm", proteusHSM);
        JsonDocument doc = JsonDocument.create(String.valueOf(coilID),
                structureProteusDocument);
        proteusBucket.upsert(doc);

    }

    public static String getKafkaTopic(ArrayList<String> topicList) {
        return topicList.get(0);
    }

    public static void updateDocument(Bucket proteusBucket,
            ArrayList<String> topicsList, Object record) {

        JsonObject row1d = JsonObject.create();
        JsonObject row2d = JsonObject.create();
        JsonObject hsm = JsonObject.create();

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

        if (record.getClass().equals(HSMMeasurement.class)) {
            hsm = JsonObject.empty().put("variables",
                    ((Measurement) record).getHSMVariables());
        }

        if (!row1d.isEmpty()) {
            proteusBucket.mutateIn(((Measurement) record).getStringCoilID())
                    .arrayAppend(topicsList.get(0), row1d).execute();
        }

        if (!row2d.isEmpty()) {
            proteusBucket.mutateIn(((Measurement) record).getStringCoilID())
                    .arrayAppend(topicsList.get(0), row2d).execute();
        }

        if (!hsm.isEmpty()) {
            proteusBucket.mutateIn(((Measurement) record).getStringCoilID())
                    .arrayAppend(topicsList.get(0), hsm).execute();
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
