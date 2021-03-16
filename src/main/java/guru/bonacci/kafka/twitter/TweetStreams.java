package guru.bonacci.kafka.twitter;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import guru.bonacci.kafka.twitter.serde.GsonDeserializer;
import guru.bonacci.kafka.twitter.serde.GsonSerializer;

public class TweetStreams {

	static final String TOPIC_IN = "twitter_json_01";
	static final String TOPIC_OUT = "twitter_string";
	
	public static void main(String[] args) {
		final TweetStreams fooStreams = new TweetStreams();
		final KafkaStreams streams = 
				new KafkaStreams(fooStreams.topology(), fooStreams.configure());
		
		streams.cleanUp();
		streams.start();

		// prints the topology
		streams.localThreadsMetadata().forEach(data -> System.out.println(data));

		// shutdown hook to correctly close the streams application
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

	public TweetStreams() {
	}

	Properties configure() {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "twitter-streams-app");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
		return props;
	}

	Topology topology() {
		StreamsBuilder builder = new StreamsBuilder();
		
		Serde<Map<String, Object>> gsonSerde = Serdes.serdeFrom(new GsonSerializer(), new GsonDeserializer());
		KStream<Map<String, Object>, Map<String, Object>> tweetStream = builder.stream(TOPIC_IN, Consumed.with(gsonSerde, gsonSerde));

		tweetStream
			.peek((k,v) -> System.out.printf("tweet: %s \n", v))
			.map((k,v) -> KeyValue.pair("", v.get("Text")))
			.to(TOPIC_OUT);

		return builder.build();
	}
}
