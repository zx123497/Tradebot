package findata.aggregator;

import findata.model.BarAccumulator;
import findata.model.BarOutput;
import findata.model.Trade;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Forms OHLCV bars when a metric (volume or notional) reaches {@code threshold}.
 */
public class ThresholdBarFunction extends KeyedProcessFunction<String, Trade, BarOutput> {

	public enum Metric {
		VOLUME,
		NOTIONAL
	}

	private final Metric metric;
	private final double threshold;
	private transient ValueState<BarAccumulator> accState;

	public ThresholdBarFunction(Metric metric, double threshold) {
		this.metric = metric;
		this.threshold = threshold;
	}

	@Override
	public void open(OpenContext openContext) throws Exception {
		accState = getRuntimeContext().getState(
				new ValueStateDescriptor<>("bar-acc", BarAccumulator.class));
	}

	@Override
	public void processElement(Trade trade, Context ctx, Collector<BarOutput> out)
			throws Exception {
		BarAccumulator acc = accState.value();
		if (acc == null) {
			acc = new BarAccumulator();
		}
		acc.add(trade);

		double metricValue = metric == Metric.VOLUME ? acc.volume : acc.cumulativePriceVolume;
		if (metricValue >= threshold) {
			out.collect(acc.toBar());
			acc.reset();
		}
		accState.update(acc);
	}
}
