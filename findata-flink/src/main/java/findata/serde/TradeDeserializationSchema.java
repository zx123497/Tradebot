package findata.serde;

import findata.model.Trade;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Parses FinData Kafka trade JSON:
 * {"symbol","price","timestamp_ms","volume",...}
 */
public class TradeDeserializationSchema implements DeserializationSchema<Trade> {

	private static final long serialVersionUID = 1L;

	private transient ObjectMapper mapper;

	@Override
	public void open(DeserializationSchema.InitializationContext context) {
		mapper = new ObjectMapper();
	}

	@Override
	public Trade deserialize(byte[] message) throws IOException {
		if (message == null) {
			return null;
		}
		if (mapper == null) {
			mapper = new ObjectMapper();
		}
		JsonNode node = mapper.readTree(message);
		Trade trade = new Trade();
		trade.symbol = node.get("symbol").asText();
		trade.price = node.get("price").asDouble();
		trade.volume = node.get("volume").asDouble();
		trade.timestamp = node.get("timestamp_ms").asLong();
		return trade;
	}

	@Override
	public boolean isEndOfStream(Trade nextElement) {
		return false;
	}

	@Override
	public TypeInformation<Trade> getProducedType() {
		return TypeInformation.of(Trade.class);
	}
}
