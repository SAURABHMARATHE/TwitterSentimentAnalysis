package edu.sjsu.cs185c;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.util.hash.Hash;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.mesos.containerizer.Protos.Wait;

import com.google.common.io.Resources;

public class Visualizer {

	public static void main(String[] args) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		HashMap<String, HashMap<String, Integer>> hm = new HashMap<String, HashMap<String, Integer>>();

		KafkaConsumer<String, String> consumer;
		try (InputStream props = Resources.getResource("consumer.props").openStream()) {
			Properties properties = new Properties();
			properties.load(props);
			if (properties.getProperty("group.id") == null) {
				properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
			}
			consumer = new KafkaConsumer<>(properties);
			consumer.subscribe(Arrays.asList("visualizer"));
			int timeouts = 0;
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(200);
				if (records.count() == 0) {
					timeouts++;
				} else {
					System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
					timeouts = 0;
				}
				for (ConsumerRecord<String, String> record : records) {
					System.out.println(record.value());
					String[] tokens = record.value().split(",");
					String hashTag = tokens[0];
					String sentiment = tokens[2];
					if (hm.containsKey(hashTag)) {
						switch (sentiment) {
						case "positive":
							hm.get(hashTag).put("positive", hm.get(hashTag).get("positive") + 1);
							break;
						case "negative":
							hm.get(hashTag).put("negative", hm.get(hashTag).get("negative") + 1);
							break;
						case "neutral":
							hm.get(hashTag).put("neutral", hm.get(hashTag).get("neutral") + 1);
							break;
						default:
							break;
						}//end of if/switch
		
					}//end of if
					
					else {
						HashMap<String, Integer> hm2 = new HashMap<String, Integer>();
						hm.put(hashTag, hm2);
						switch (sentiment) {
						case "positive":
							hm.get(hashTag).put("positive",  1);
							hm.get(hashTag).put("negative",  0);
							hm.get(hashTag).put("neutral",  0);
							break;
						case "negative":
							hm.get(hashTag).put("positive",  0);
							hm.get(hashTag).put("negative",  1);
							hm.get(hashTag).put("neutral",  0);
							break;
						case "neutral":
							hm.get(hashTag).put("positive",  0);
							hm.get(hashTag).put("negative",  0);
							hm.get(hashTag).put("neutral",  1);
							break;
						default:
							break;
						}//end of else/switch
		
					}//end of else
				}
				System.out.println(hm.toString());
				Thread.sleep(1000);
			}
			
		}

	}
}
