package guru.bonacci.kafka.twitter.serde;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.gson.Gson;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GsonSerializer implements Serializer<Map<String, Object>> {

	private final Gson gson = new Gson();
	private final StringSerializer stringToByte = new StringSerializer();

	@Override
	public byte[] serialize(String topic, Map<String, Object> data) {
		String message = gson.toJson(data);
		log.debug("Serializing {}", message);
		return stringToByte.serialize(topic, message);
	}
}