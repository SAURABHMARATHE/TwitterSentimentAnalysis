package edu.sjsu.cs185c;

import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLEncoder;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;

public class SentimentAnalyzer {
	public static void main(String[] args) throws IOException {
		// error-check the command line
		if (args.length != 2) {
			System.err.println("usage: Consumer <topic> <threshold> <file>");
			System.exit(1);
		}
		// parse the command-line
		String topic = args[0];
		// Double threshold = Double.parseDouble(args[1]);
		// String file = args[2];
		// File f = new File(file);
		// if(!f.exists())
		// f.createNewFile();

		// setup consumer
		KafkaConsumer<String, String> consumer;
		try (InputStream consumerProps = Resources.getResource("consumer.props").openStream()) {
			Properties consumerProperties = new Properties();
			consumerProperties.load(consumerProps);
			if (consumerProperties.getProperty("group.id") == null) {
				consumerProperties.setProperty("group.id", "group-" + new Random().nextInt(100000));
			}
			consumer = new KafkaConsumer<>(consumerProperties);
			consumer.subscribe(Arrays.asList(topic));
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
					// TODO: pull out the vibration delta from record
					// System.out.println(record.value());
					String tweetRecord = record.value();
					String[] tokens = tweetRecord.split(",;,");
					String hashTag = tokens[0];
					String tweet = tokens[1];
					JSONObject responseObject = null;
					JSONParser parser = new JSONParser();

					InputStream input = new URL("http://access.alchemyapi.com/calls/text/TextGetTextSentiment?"
							+ "outputMode=json&apikey=5c1b555cf29ca3a7e0952489807c3a3f519be5de&text=" + URLEncoder.encode(  tweet,"UTF-8"))
									.openStream();
					
					Scanner scanner = new Scanner(input);
					String responseBody = scanner.useDelimiter("\\A").next();
					responseObject = (JSONObject) parser.parse(responseBody);
					System.out.println(tweet);
					String sentiment = (String) ((JSONObject) responseObject.get("docSentiment")).get("type");
					System.out.println();

					// Block: publish the tweet with sentiment
					KafkaProducer<String, String> producer = null;
					try (InputStream props = Resources.getResource("producer.props").openStream()) {
						Properties properties = new Properties();
						properties.load(props);
						producer = new KafkaProducer<>(properties);

						String message = tweetRecord + ",;," + sentiment;
						ProducerRecord<String, String> rec = new ProducerRecord<String, String>(args[1], message);
						// TODO: publish message to topic
						producer.send(rec);
						// TODO: flush producer
						producer.flush();

					} catch (Exception e) {
						System.err.println(e.toString());
					} // endBlock: publish the tweet with sentiment

				}
			}
		} catch (org.json.simple.parser.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
