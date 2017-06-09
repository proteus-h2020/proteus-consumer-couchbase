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

public class CouchbaseUtils {

	public static boolean checkIfDocumentExists(String coilID, Bucket proteusBucket) {
		JsonDocument checkExists = proteusBucket.get(coilID);
		if (checkExists == null)
			return false;
		else
			return true;
	}

	public static void createDocumentFirstTime(String coilID, Measurement record, ArrayList<String> topicList,
			Bucket proteusBucket) {
		List<JsonObject> proteusRealtime = new ArrayList<>();
		List<JsonObject> proteusHSM = new ArrayList<>();
		List<JsonObject> proteusFlatness = new ArrayList<>();

		JsonObject row1d = JsonObject.create();
		JsonObject row2d = JsonObject.create();
		JsonObject hsm = JsonObject.create();

		if (record.getType() == 0x00) {
			row1d = JsonObject.empty().put("position-x", record.getPositionX()).put("value", record.getValue())
					.put("var", record.getVarName());

		}

		if (record.getType() == 0x01) {
			row2d = JsonObject.empty().put("position-x", record.getPositionX()).put("position-y", record.getPositionY())
					.put("value", record.getValue()).put("var", record.getVarName());
		}

		if (record.getClass().equals(HSMMeasurement.class)) {

			hsm = JsonObject.empty().put("variables-counter", record.getHSMVarCounter()).put("variables",
					record.getHSMVariables());

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

		JsonObject structureProteusDocument = JsonObject.empty().put("coilID", coilID)
				.put("proteus-realtime", proteusRealtime).put("proteus-flatness", proteusFlatness)
				.put("proteus-hsm", proteusHSM);
		JsonDocument doc = JsonDocument.create(coilID, structureProteusDocument);
		proteusBucket.upsert(doc);

	}

	public static String getKafkaTopic(ArrayList<String> topicList) {
		return topicList.get(0);
	}

	public static void updateDocument(Bucket proteusBucket, ArrayList<String> topicsList, Measurement record) {

		JsonObject row1d = JsonObject.create();
		JsonObject row2d = JsonObject.create();
		JsonObject hsm = JsonObject.create();

		if (record.getType() == 0x00) {

			row1d = JsonObject.empty().put("position-x", record.getPositionX()).put("value", record.getValue())
					.put("var", record.getVarName());
		}

		if (record.getType() == 0x01) {

			row2d = JsonObject.empty().put("position-x", record.getPositionX()).put("position-y", record.getPositionY())
					.put("value", record.getValue()).put("var", record.getVarName());
		}

		if (record.getClass().equals(HSMMeasurement.class)) {

			hsm = JsonObject.empty().put("variables-counter", record.getHSMVarCounter()).put("variables",
					record.getHSMVariables());

		}

		if (!row1d.isEmpty()) {
			proteusBucket.mutateIn(record.getStringCoilID()).arrayAppend(topicsList.get(0), row1d).execute();
		}

		if (!row2d.isEmpty()) {
			proteusBucket.mutateIn(record.getStringCoilID()).arrayAppend(topicsList.get(0), row2d).execute();
		}

		if (!hsm.isEmpty()) {
			proteusBucket.mutateIn(record.getStringCoilID()).arrayAppend(topicsList.get(0), hsm).execute();
		}

	}

	public static void checkDataInserted(String coilID, Bucket proteusBucket) {
		DocumentFragment<Lookup> resultHSM = proteusBucket.lookupIn(coilID).get("proteus-hsm").execute();
		DocumentFragment<Lookup> resultFlatness = proteusBucket.lookupIn(coilID).get("proteus-realtime").execute();
		DocumentFragment<Lookup> resultRealtime = proteusBucket.lookupIn(coilID).get("proteus-flatness").execute();
		System.out.println("Resultado para la bobina < " + coilID + " > | < HSM > " + resultHSM.size()
				+ " | < Flantess > " + resultFlatness.size() + " | < Realtime > " + resultRealtime.size());
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
