package guru.bonacci.kafka.twitter.serde;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.google.gson.Gson;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GsonDeserializer implements Deserializer<Map<String, Object>> {

	private final Gson gson = new Gson();
	private final StringDeserializer byteToString = new StringDeserializer();

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> deserialize(String topic, byte[] data) {
		final String message = byteToString.deserialize(topic, data);
		log.debug("Deserializing {}", message);
		return gson.fromJson(message, Map.class);
	}
}