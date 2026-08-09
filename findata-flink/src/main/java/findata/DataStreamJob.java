package findata;

import findata.aggregator.ThresholdBarFunction;
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
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Consumes tick trades and emits:
 * <ul>
 *   <li>1-minute and 5-minute time bars</li>
 *   <li>volume bars (~50,000 shares)</li>
 *   <li>dollar bars (~$5,000,000 notional)</li>
 * </ul>
 */
public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		String brokers = envOrDefault("FLINK_KAFKA_BOOTSTRAP",
				envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"));
		String tradesTopic = envOrDefault("KAFKA_TOPIC", "sp500.trades");
		String bars1mTopic = envOrDefault("KAFKA_BARS_1M_TOPIC",
				envOrDefault("KAFKA_BARS_TOPIC", "sp500.bars.1m"));
		String bars5mTopic = envOrDefault("KAFKA_BARS_5M_TOPIC", "sp500.bars.5m");
		String barsVolumeTopic = envOrDefault("KAFKA_BARS_VOLUME_TOPIC", "sp500.bars.volume");
		String barsDollarTopic = envOrDefault("KAFKA_BARS_DOLLAR_TOPIC", "sp500.bars.dollar");
		String groupId = envOrDefault("FLINK_KAFKA_GROUP", "flink-bars-multi");

		double volumeThreshold = Double.parseDouble(
				envOrDefault("FLINK_VOLUME_BAR_THRESHOLD", "50000"));
		double dollarThreshold = Double.parseDouble(
				envOrDefault("FLINK_DOLLAR_BAR_THRESHOLD", "5000000"));

		OpenLineageEmitter lineage = new OpenLineageEmitter("flink_multi_bars");
		lineage.start(tradesTopic, bars1mTopic, brokers);

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

		KeyedStream<Trade, String> keyed = trades.keyBy(trade -> trade.symbol);

		DataStream<BarOutput> bars1m = keyed
				.window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
				.aggregate(new TradeAggregator(), new EnrichWindowStart())
				.name("bars-1m");

		DataStream<BarOutput> bars5m = keyed
				.window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
				.aggregate(new TradeAggregator(), new EnrichWindowStart())
				.name("bars-5m");

		DataStream<BarOutput> barsVolume = keyed
				.process(new ThresholdBarFunction(
						ThresholdBarFunction.Metric.VOLUME, volumeThreshold))
				.name("bars-volume");

		DataStream<BarOutput> barsDollar = keyed
				.process(new ThresholdBarFunction(
						ThresholdBarFunction.Metric.NOTIONAL, dollarThreshold))
				.name("bars-dollar");

		sinkBars(bars1m, brokers, bars1mTopic);
		sinkBars(bars5m, brokers, bars5mTopic);
		sinkBars(barsVolume, brokers, barsVolumeTopic);
		sinkBars(barsDollar, brokers, barsDollarTopic);

		bars1m.print("1m");
		bars5m.print("5m");
		barsVolume.print("vol");
		barsDollar.print("usd");

		try {
			env.execute("Multi-Bar OHLCV Job (1m/5m/volume/dollar)");
			lineage.complete(tradesTopic, bars1mTopic, brokers);
		} catch (Exception e) {
			lineage.fail(tradesTopic, bars1mTopic, brokers, e.getMessage());
			throw e;
		}
	}

	private static void sinkBars(DataStream<BarOutput> stream, String brokers, String topic) {
		KafkaSink<BarOutput> sink = KafkaSink.<BarOutput>builder()
				.setBootstrapServers(brokers)
				.setRecordSerializer(
						KafkaRecordSerializationSchema.builder()
								.setTopic(topic)
								.setValueSerializationSchema(new BarSerializationSchema())
								.build())
				.build();
		stream.sinkTo(sink).name("sink-" + topic);
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
