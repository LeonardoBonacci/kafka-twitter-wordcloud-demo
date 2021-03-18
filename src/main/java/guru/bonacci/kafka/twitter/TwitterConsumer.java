package guru.bonacci.kafka.twitter;

import java.awt.Color;
import java.awt.Dimension;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.kennycason.kumo.CollisionMode;
import com.kennycason.kumo.WordCloud;
import com.kennycason.kumo.WordFrequency;
import com.kennycason.kumo.bg.RectangleBackground;
import com.kennycason.kumo.font.scale.LinearFontScalar;
import com.kennycason.kumo.nlp.FrequencyAnalyzer;
import com.kennycason.kumo.palette.ColorPalette;

public class TwitterConsumer {

	static final String TOPIC = "demo.twitter.string";
	static final String EXPORT_DIR = "C:\\tmp\\";

	public static void main(final String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println("Please provide command line arguments: configPath");
			System.exit(1);
		}

		final Properties props = loadConfig(args[0]);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "tweet-consumer-app");

		final Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList(TOPIC));

		final FrequencyAnalyzer frequencyAnalyzer = new FrequencyAnalyzer();
		frequencyAnalyzer.setWordFrequenciesToReturn(300);
		frequencyAnalyzer.setMinWordLength(4);

		List<String> input = new ArrayList<>();
		System.out.println("ready to go...");
		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100l));
				for (ConsumerRecord<String, String> record : records) {
					System.out.println("receiving: " + record.value());
					input.add(record.value().replace("twg_tribe_showcase", ""));
					final List<WordFrequency> wordFrequencies = frequencyAnalyzer.load(input);
					final Dimension dimension = new Dimension(600, 600);
					final WordCloud wordCloud = new WordCloud(dimension, CollisionMode.PIXEL_PERFECT);
					wordCloud.setPadding(2);
					wordCloud.setBackground(new RectangleBackground(dimension));
					wordCloud.setColorPalette(new ColorPalette(new Color(0xFFE91E), new Color(0xadd8e6), new Color(0xBCD123), new Color(0x36AD32), new Color(0xFFAD2A), new Color(0x8B0000), new Color(0xFFFFFF)));
					wordCloud.setFontScalar(new LinearFontScalar(10, 40));
					wordCloud.build(wordFrequencies);
					wordCloud.writeToFile(EXPORT_DIR + System.currentTimeMillis() + ".png");
				}
			}
		} finally {
			consumer.close();
		}
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