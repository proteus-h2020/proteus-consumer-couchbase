package eu.proteus.couchbase.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

public class CouchbaseUtilsTest {

    @Test
    public void testCreateDocument() {

        List<JsonObject> proteusDocument = new ArrayList<>();
        JsonObject row = JsonObject.create();
        row = JsonObject.empty().put("x", randomInt());
        proteusDocument.add(row);
        JsonObject structureProteusDocument = JsonObject.empty()
                .put("test-document", "test-document")
                .put("proteus-document", proteusDocument);
        JsonDocument document = JsonDocument.create("test-document",
                structureProteusDocument);
        // proteusBucket.upsert(doc);

    }

    @Test
    public void testUpdateDocument() {

    }

    private double randomDouble() {
        return ThreadLocalRandom.current().nextDouble(0, 30000D);
    }

    private int randomInt() {
        return ThreadLocalRandom.current().nextInt(0, 2000);
    }

    private int randomVarIdentifier() {
        return ThreadLocalRandom.current().nextInt(1, 54);
    }
}
