package guru.bonacci.kafka.twitter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import guru.bonacci.kafka.twitter.serde.GsonDeserializer;
import guru.bonacci.kafka.twitter.serde.GsonSerializer;

public class TweetStreams {

	static final String TOPIC_IN = "demo.twitter.json";
	static final String TOPIC_OUT = "demo.twitter.string";
	
	public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("Please provide command line arguments: configPath");
            System.exit(1);
        }

        final Properties props = loadConfig(args[0]);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "twitter-streams-app");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);

        final StreamsBuilder builder = new StreamsBuilder();
		Serde<Map<String, Object>> gsonSerde = Serdes.serdeFrom(new GsonSerializer(), new GsonDeserializer());
		KStream<Map<String, Object>, Map<String, Object>> tweetStream = builder.stream(TOPIC_IN, Consumed.with(gsonSerde, gsonSerde));

		tweetStream
			.peek((k,v) -> System.out.printf("tweet: %s \n", v))
			.map((k,v) -> KeyValue.pair("", v.get("Text")))
			.to(TOPIC_OUT);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
		streams.start();

		// shutdown hook to correctly close the streams application
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}


	public static Properties loadConfig(String configFile) throws IOException {
		if (!Files.exists(Paths.get(configFile))) {
			throw new IOException(configFile + " not found.");
		}
		final Properties cfg = new Properties();
		try (InputStream inputStream = new FileInputStream(configFile)) {
			cfg.load(inputStream);
		}
		return cfg;
	}
}
