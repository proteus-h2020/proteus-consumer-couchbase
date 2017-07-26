package eu.proteus.couchbase.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.subdoc.DocumentFragment;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CouchbaseUtilsTest {

    // Couchbase Connection

    private static Bucket proteusBucket;
    private static CouchbaseEnvironment couchbaseEnvironment = DefaultCouchbaseEnvironment
            .builder().build();
    private static List<String> nodes = Arrays.asList("192.168.4.246",
            "192.168.4.247", "192.168.4.248");
    private static Cluster clusterCouchbase = CouchbaseCluster
            .create(couchbaseEnvironment, nodes);

    @Test
    public void testCreateDocument() {

        proteusBucket = clusterCouchbase.openBucket("proteus-test");
        JsonObject structureProteusDocument = JsonObject.empty()
                .put("proteus-hsm", "hsm-values")
                .put("proteus-realtime", new ArrayList())
                .put("calculations", new ArrayList());
        JsonDocument document = JsonDocument.create("test-document",
                structureProteusDocument);
        proteusBucket.upsert(document);
        proteusBucket.close();

    }

    @Test
    public void checkPath() {
        proteusBucket = clusterCouchbase.openBucket("proteus-test");
        System.out.println("Calculations: " + proteusBucket
                .lookupIn("test-document").exists("calculations").execute());
        System.out.println("Calculaciones: " + proteusBucket
                .lookupIn("test-document").exists("calculaciones").execute());

        DocumentFragment<Lookup> resultado = proteusBucket
                .lookupIn("test-document").exists("calculations")
                .exists("calculaciones").execute();

        boolean calculationsExist = resultado.content("calculations",
                Boolean.class);
        boolean calculacionesExist = resultado.content("calculaciones",
                Boolean.class);

        System.out.println("Calculations Exist: " + calculationsExist);
        System.out.println("Calculaciones Exist: " + calculacionesExist);

        proteusBucket.close();
    }

    @Test
    public void testUpdateDocument() {
        proteusBucket = clusterCouchbase.openBucket("proteus-test");
        System.out.println("Null");
        for (int i = 0; i < 10; i++) {
            proteusBucket.mutateIn("test-document")
                    .arrayAppend("calculations", "calculations" + i).execute();
        }
        proteusBucket.close();

    }

    @Test
    public void testUpdateDocumentWithNewFieldWhenNullReceived() {
        proteusBucket = clusterCouchbase.openBucket("proteus-test");
        Object record = null;
        if (record == null) {
            proteusBucket.mutateIn("test-document")
                    .upsert("proteus-flatness", new ArrayList()).execute();
        }
        proteusBucket.close();
    }

    @Test
    public void testUpdateDocumentWithNewField() {
        proteusBucket = clusterCouchbase.openBucket("proteus-test");
        proteusBucket.mutateIn("test-document")
                .upsert("proteus-flatness-notnull", new ArrayList())
                .arrayAppend("proteus-flatness-notnull", "value-1").execute();
        proteusBucket.close();
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
