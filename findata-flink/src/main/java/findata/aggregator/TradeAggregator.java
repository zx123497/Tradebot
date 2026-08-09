package findata.aggregator;

import findata.model.BarAccumulator;
import findata.model.BarOutput;
import findata.model.Trade;
import org.apache.flink.api.common.functions.AggregateFunction;

public class TradeAggregator implements AggregateFunction<Trade, BarAccumulator, BarOutput> {

	@Override
	public BarAccumulator createAccumulator() {
		return new BarAccumulator();
	}

	@Override
	public BarAccumulator add(Trade value, BarAccumulator acc) {
		acc.add(value);
		return acc;
	}

	@Override
	public BarOutput getResult(BarAccumulator acc) {
		return acc.toBar();
	}

	@Override
	public BarAccumulator merge(BarAccumulator a, BarAccumulator b) {
		if (a.open < 0) {
			return b;
		}
		if (b.open < 0) {
			return a;
		}
		if (a.firstTimestamp < 0 || (b.firstTimestamp >= 0 && b.firstTimestamp < a.firstTimestamp)) {
			a.firstTimestamp = b.firstTimestamp;
			a.open = b.open;
		}
		a.volume += b.volume;
		a.tradeCount += b.tradeCount;
		a.cumulativePriceVolume += b.cumulativePriceVolume;
		a.high = Math.max(a.high, b.high);
		a.low = Math.min(a.low, b.low);
		a.close = b.close;
		if (a.symbol == null) {
			a.symbol = b.symbol;
		}
		return a;
	}
}
