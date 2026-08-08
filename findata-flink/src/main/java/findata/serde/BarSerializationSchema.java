package findata.serde;

import findata.model.BarOutput;
import org.apache.flink.api.common.serialization.SerializationSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Emits JSON matching FinData consumer / ClickHouse bar schema:
 * symbol, window_start, open_price, high_price, low_price, close_price,
 * volume, trade_count, vwap
 */
public class BarSerializationSchema implements SerializationSchema<BarOutput> {

	private static final long serialVersionUID = 1L;

	private static final DateTimeFormatter WINDOW_FORMAT =
			DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

	private transient ObjectMapper mapper;

	@Override
	public void open(SerializationSchema.InitializationContext context) {
		mapper = new ObjectMapper();
	}

	@Override
	public byte[] serialize(BarOutput bar) {
		if (mapper == null) {
			mapper = new ObjectMapper();
		}
		ObjectNode node = mapper.createObjectNode();
		node.put("symbol", bar.symbol);
		node.put("window_start", WINDOW_FORMAT.format(Instant.ofEpochMilli(bar.windowStart)));
		node.put("open_price", bar.open);
		node.put("high_price", bar.high);
		node.put("low_price", bar.low);
		node.put("close_price", bar.close);
		node.put("volume", bar.volume);
		node.put("trade_count", bar.tradeCount);
		node.put("vwap", bar.vwap);
		return node.toString().getBytes(StandardCharsets.UTF_8);
	}
}
