package eu.proteus.examples;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.subdoc.DocumentFragment;

import eu.proteus.couchbase.utils.CouchbaseUtils;

public class ExampleQueryCouch {

	private static final Logger logger = LoggerFactory.getLogger(ExampleCouch.class);
	private static Cluster clusterCouchbase;
	private static Bucket proteusBucket;
	private static int documentCounter;

	public static void main(String[] args) throws InterruptedException {

		CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder().build();

		clusterCouchbase = CouchbaseCluster.create(env, "192.168.4.246", "192.168.4.247", "192.168.4.248");
		proteusBucket = clusterCouchbase.openBucket("proteus-testing");

		String coil = "40101001";

		DocumentFragment<Lookup> resultHSM = proteusBucket.lookupIn(coil).get("proteus-hsm").execute();
		DocumentFragment<Lookup> resultFlatness = proteusBucket.lookupIn(coil).get("proteus-flatness").execute();
		DocumentFragment<Lookup> resultRealtime = proteusBucket.lookupIn(coil).get("proteus-realtime").execute();
		System.out.println("Resultado para la bobina < " + "40101022" + " > | < HSM > " + resultHSM.size()
				+ " | < Flantess > " + resultFlatness.size() + " | < Realtime > " + resultRealtime.size());

		// System.out.println(resultRealtime.toString());
		// Thread.sleep(3000);
		// System.out.println(resultFlatness.toString());
		// Thread.sleep(3000);
		System.out.println(resultHSM.toString());
		Thread.sleep(3000);

		ArrayList<String> coils = new ArrayList<String>();

		coils.add("40101022");
		coils.add("40101018");

		for (String bobina : coils) {
			CouchbaseUtils.checkDataInserted(bobina, proteusBucket);
		}

	}

}
