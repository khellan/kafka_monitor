package net.zedge.event_monitor;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.HashMap;
import java.util.Map;

public final class EventMonitor {
    private static final String GROUP_ID = "EventMonitorSpark";

    /*
     * Parameters
     * 0: Master URL: spark://<localhost>:7077
     * 1: Topic
     * 2: Number of partitions
     * 3: Zookeeper connect url: <server0>:<port>,<server1>:<port>,<server2>:<port>/<path>
     * 4: Duration (batch period)
     * 5: Max rate (number of messages per batch)
    */
    public static void main(String[] args) {
        String master = args[0];
        String topic = args[1];
        String partitionCount = args[2];
        String zkServers = args[3];
        String duration = args[4];
        String maxRate = args[5];

        System.out.println("Master: " + master);
        System.out.println("Topic: " + topic);
        System.out.println("Partitions: " + partitionCount);
        System.out.println("Zookeeper: " + zkServers);
        System.out.println("Duration: " + duration);
        System.out.println("Max Rate: " + maxRate);

        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put(topic, Integer.parseInt(partitionCount));

        // Create the context with a 1 second batch size
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster(master);
        sparkConf.setAppName("EventMonitor");
        sparkConf.setSparkHome(System.getenv("SPARK_HOME"));
        sparkConf.setJars(JavaStreamingContext.jarOfClass(EventMonitor.class));
        sparkConf.set("spark.streaming.receiver.maxRate", maxRate);

        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(Integer.parseInt(duration)));

        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(ssc, zkServers, GROUP_ID, topicMap);
        messages.print();
        ssc.start();
        ssc.awaitTermination();
    }
}