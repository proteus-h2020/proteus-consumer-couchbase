package eu.proteus.consumer.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;

public class ConsumerUtils {

    public static Properties loadPropertiesFromFile(String propertiesFile) {

        Properties properties = new Properties();
        try {
            InputStream inputStream = new FileInputStream(propertiesFile);
            properties.load(inputStream);
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

    public static String getTopicName(String name) {
        String ret = "";
        if (name.equals(KafkaTopics.PROTEUS_HSM.toString()))
            ret = "proteus-hsm";
        if (name.equals(KafkaTopics.PROTEUS_REALTIME.toString()))
            ret = "proteus-realtime";
        if (name.equals(KafkaTopics.PROTEUS_FLATNESS.toString()))
            ret = "proteus-flatness";
        if (name.equals(KafkaTopics.SIMPLE_MOMENTS.toString()))
            ret = "simple-moments";
        return ret;
    }

    public static Bucket selectCouchbaseBucket(Cluster cluster, String topic) {
        String myBucket = "proteus";
        for (ProteusBuckets bucket : ProteusBuckets.values()) {
            if (!topic.contains("PROTEUS"))
                myBucket = bucket.name();
        }
        return cluster.openBucket(myBucket.toLowerCase());
    }

}
