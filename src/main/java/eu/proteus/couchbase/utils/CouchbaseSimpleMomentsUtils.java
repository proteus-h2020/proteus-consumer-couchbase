package eu.proteus.couchbase.utils;

import java.util.ArrayList;
import java.util.List;

import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.subdoc.DocumentFragment;

import eu.proteus.consumer.model.MomentsResult;

public class CouchbaseSimpleMomentsUtils {

    private static String DOCUMENT_HEADER = "simple-moments";
    private static String DOCUMENT_SEPARATOR = "-";

    public static boolean checkIfDocumentExists(int coilID,
            Bucket proteusBucket) {

        StringBuilder documentName = new StringBuilder().append(DOCUMENT_HEADER)
                .append(DOCUMENT_SEPARATOR).append(String.valueOf(coilID));

        JsonDocument checkExists = proteusBucket.get(documentName.toString());
        if (checkExists == null) {
            return false;
        }

        else {
            return true;
        }

    }

    public static void createSimpleMomentsFirstTime(int coilID, Object record,
            ArrayList<String> topicLIst, Bucket proteusBucket) {

        List<JsonObject> proteusSimpleMoments = new ArrayList<>();
        JsonObject simpleMoments = JsonObject.create();
        simpleMoments = JsonObject.empty()
                .put("var-id", ((MomentsResult) record).getVarId())
                .put("mean", ((MomentsResult) record).getMean())
                .put("variance", ((MomentsResult) record).getVariance())
                .put("counter", ((MomentsResult) record).getCounter())
                .put("stdDeviation", ((MomentsResult) record).getStdDeviation())
                .put("x", ((MomentsResult) record).getX())
                .put("y", ((MomentsResult) record).getY());

        proteusSimpleMoments.add(simpleMoments);

        JsonObject structureProteusDocument = JsonObject.empty()
                .put("coilID",
                        Integer.toString(((MomentsResult) record).getCoilId()))
                .put("values", proteusSimpleMoments);

        StringBuilder documentName = new StringBuilder().append(DOCUMENT_HEADER)
                .append(DOCUMENT_SEPARATOR).append(String.valueOf(coilID));

        JsonDocument doc = JsonDocument.create(documentName.toString(),
                structureProteusDocument);
        proteusBucket.upsert(doc);

    }

    public static void updateSimpleMomentsDocument(Bucket proteusBucket,
            ArrayList<String> topicsList, Object record) {

        StringBuilder documentName = new StringBuilder().append(DOCUMENT_HEADER)
                .append(DOCUMENT_SEPARATOR)
                .append(((MomentsResult) record).getCoilId());

        JsonObject simpleMoments = JsonObject.create();

        simpleMoments = JsonObject.empty()
                .put("varId", ((MomentsResult) record).getVarId())
                .put("mean", ((MomentsResult) record).getMean())
                .put("variance", ((MomentsResult) record).getVariance())
                .put("counter", ((MomentsResult) record).getCounter())
                .put("stdDeviation", ((MomentsResult) record).getStdDeviation())
                .put("x", ((MomentsResult) record).getX())
                .put("y", ((MomentsResult) record).getY());

        proteusBucket.mutateIn(documentName.toString())
                .arrayAppend("values", simpleMoments).execute();
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
