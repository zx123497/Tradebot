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
		acc.symbol = value.symbol;
		acc.volume += value.volume;
		acc.tradeCount += 1;
		acc.cumulativePriceVolume += value.price * value.volume;
		if (acc.open < 0) {
			acc.open = value.price;
		}
		if (value.price > acc.high) {
			acc.high = value.price;
		}
		if (value.price < acc.low) {
			acc.low = value.price;
		}
		acc.close = value.price;
		return acc;
	}

	@Override
	public BarOutput getResult(BarAccumulator acc) {
		BarOutput out = new BarOutput();
		out.symbol = acc.symbol;
		out.open = acc.open;
		out.high = acc.high;
		out.low = acc.low;
		out.close = acc.close;
		out.volume = acc.volume;
		out.tradeCount = acc.tradeCount;
		out.vwap = acc.volume == 0 ? 0.0 : acc.cumulativePriceVolume / acc.volume;
		return out;
	}

	@Override
	public BarAccumulator merge(BarAccumulator a, BarAccumulator b) {
		if (a.open < 0) {
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
