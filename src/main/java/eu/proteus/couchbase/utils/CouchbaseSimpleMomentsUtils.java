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

import eu.proteus.consumer.model.Measurement;
import eu.proteus.consumer.model.MomentsResult;

public class CouchbaseSimpleMomentsUtils {

    private static final Logger logger = LoggerFactory.getLogger("updates");

    public static boolean checkIfDocumentExists(int coilID,
            Bucket proteusBucket) {

        JsonDocument checkExists = proteusBucket.get(String.valueOf(coilID));
        if (checkExists == null) {
            return false;
        }

        else {
            return true;
        }

    }

    public static void createSimpleMomentsFirstTime(int coilID, Object record,
            ArrayList<String> topicLIst, Bucket proteusBucket) {

        try {
            logger.debug("New Document. Enter Point -> Simple Moments");
            List<JsonObject> proteusSimpleMoments = new ArrayList<>();
            JsonObject simpleMoments = JsonObject.create();
            simpleMoments = JsonObject.empty()
                    .put("varID", ((MomentsResult) record).getVarId())
                    .put("mean", ((MomentsResult) record).getMean())
                    .put("variance", ((MomentsResult) record).getVariance())
                    .put("counter", ((MomentsResult) record).getCounter())
                    .put("stdDeviation",
                            Math.sqrt(((MomentsResult) record).getVariance()))
                    .put("x", ((MomentsResult) record).getX())
                    .put("y", ((MomentsResult) record).getY());

            proteusSimpleMoments.add(simpleMoments);

            JsonObject structureProteusDocument = JsonObject.empty()
                    .put("coilID",
                            Integer.toString(
                                    ((MomentsResult) record).getCoilId()))
                    .put("proteus-realtime", new ArrayList())
                    .put("proteus-flatness", new ArrayList())
                    .put("proteus-hsm",
                            ((Measurement) record).getHSMVariables())
                    .put("calculations", proteusSimpleMoments);

            StringBuilder documentName = new StringBuilder()
                    .append(String.valueOf(coilID));

            JsonDocument doc = JsonDocument.create(documentName.toString(),
                    structureProteusDocument);
            proteusBucket.upsert(doc);
        } catch (Exception e) {
            logger.debug("Exception creating Document -> Simple Moments");
        }

    }

    public static void updateSimpleMomentsDocument(Bucket proteusBucket,
            ArrayList<String> topicsList, Object record) {

        try {
            logger.debug("Updating Document. Entry point -> Simple Moments");
            JsonObject simpleMoments = JsonObject.create();

            simpleMoments = JsonObject.empty()
                    .put("varID", ((MomentsResult) record).getVarId())
                    .put("mean", ((MomentsResult) record).getMean())
                    .put("variance", ((MomentsResult) record).getVariance())
                    .put("counter", ((MomentsResult) record).getCounter())
                    .put("stdDeviation",
                            Math.sqrt(((MomentsResult) record).getVariance()))
                    .put("x", ((MomentsResult) record).getX())
                    .put("y", ((MomentsResult) record).getY());

            logger.debug("Value {}", simpleMoments);

            proteusBucket
                    .mutateIn(String
                            .valueOf(((MomentsResult) record).getCoilId()))
                    .arrayAppend("calculations", simpleMoments).execute();

        }

        catch (Exception e) {
            logger.debug(
                    "Exception updating Document. Entry point -> Simple Moments");
        }
    }

    public static String getKafkaTopic(ArrayList<String> topicList) {
        return topicList.get(0);
    }

    public static void checkDataInserted(String coilID, Bucket proteusBucket) {
        DocumentFragment<Lookup> resultHSM = proteusBucket.lookupIn(coilID)
                .get("proteus-hsm").execute();
        DocumentFragment<Lookup> resultFlatness = proteusBucket.lookupIn(coilID)
                .get("proteus-realtime").execute();
        DocumentFragment<Lookup> resultRealtime = proteusBucket.lookupIn(coilID)
                .get("proteus-flatness").execute();
        System.out.println("Resultado para la bobina < " + coilID
                + " > | < HSM > " + resultHSM.size() + " | < Flantess > "
                + resultFlatness.size() + " | < Realtime > "
                + resultRealtime.size());
    }

}
