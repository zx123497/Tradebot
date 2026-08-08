package findata;

import findata.aggregator.TradeAggregator;
import findata.lineage.OpenLineageEmitter;
import findata.model.BarOutput;
import findata.model.Trade;
import findata.serde.BarSerializationSchema;
import findata.serde.TradeDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		String brokers = envOrDefault("FLINK_KAFKA_BOOTSTRAP",
				envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"));
		String tradesTopic = envOrDefault("KAFKA_TOPIC", "sp500.trades");
		String barsTopic = envOrDefault("KAFKA_BARS_TOPIC", "sp500.bars.1m");
		String groupId = envOrDefault("FLINK_KAFKA_GROUP", "flink-bars-1m");

		OpenLineageEmitter lineage = new OpenLineageEmitter("flink_ohlcv_1m");
		lineage.start(tradesTopic, barsTopic, brokers);

		KafkaSource<Trade> source = KafkaSource.<Trade>builder()
				.setBootstrapServers(brokers)
				.setTopics(tradesTopic)
				.setGroupId(groupId)
				.setStartingOffsets(OffsetsInitializer.latest())
				.setValueOnlyDeserializer(new TradeDeserializationSchema())
				.build();

		DataStream<Trade> trades = env.fromSource(
				source,
				WatermarkStrategy.<Trade>forBoundedOutOfOrderness(Duration.ofSeconds(5))
						.withTimestampAssigner((event, timestamp) -> event.timestamp),
				"Trade Source");

		DataStream<BarOutput> aggregatedBars = trades
				.keyBy(trade -> trade.symbol)
				.window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
				.aggregate(new TradeAggregator(), new EnrichWindowStart());

		KafkaSink<BarOutput> sink = KafkaSink.<BarOutput>builder()
				.setBootstrapServers(brokers)
				.setRecordSerializer(
						KafkaRecordSerializationSchema.builder()
								.setTopic(barsTopic)
								.setValueSerializationSchema(new BarSerializationSchema())
								.build())
				.build();

		aggregatedBars.sinkTo(sink);
		aggregatedBars.print();

		try {
			env.execute("1-Minute OHLCV and VWAP Job");
			lineage.complete(tradesTopic, barsTopic, brokers);
		} catch (Exception e) {
			lineage.fail(tradesTopic, barsTopic, brokers, e.getMessage());
			throw e;
		}
	}

	/** Attach tumbling-window start millis onto the aggregated bar. */
	private static final class EnrichWindowStart
			extends ProcessWindowFunction<BarOutput, BarOutput, String, TimeWindow> {

		@Override
		public void process(
				String symbol,
				Context context,
				Iterable<BarOutput> elements,
				Collector<BarOutput> out) {
			for (BarOutput bar : elements) {
				bar.windowStart = context.window().getStart();
				out.collect(bar);
			}
		}
	}

	private static String envOrDefault(String key, String defaultValue) {
		String value = System.getenv(key);
		return (value == null || value.isBlank()) ? defaultValue : value;
	}
}
