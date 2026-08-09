package findata.model;

import java.io.Serializable;

public class BarAccumulator implements Serializable {
	private static final long serialVersionUID = 1L;

	public String symbol;
	public long firstTimestamp = -1L;
	public double open = -1.0;
	public double high = -Double.MAX_VALUE;
	public double low = Double.MAX_VALUE;
	public double close = 0.0;
	public double volume = 0.0;
	public long tradeCount = 0;
	public double cumulativePriceVolume = 0.0;

	public void reset() {
		symbol = null;
		firstTimestamp = -1L;
		open = -1.0;
		high = -Double.MAX_VALUE;
		low = Double.MAX_VALUE;
		close = 0.0;
		volume = 0.0;
		tradeCount = 0;
		cumulativePriceVolume = 0.0;
	}

	public void add(Trade trade) {
		symbol = trade.symbol;
		if (firstTimestamp < 0) {
			firstTimestamp = trade.timestamp;
		}
		volume += trade.volume;
		tradeCount += 1;
		cumulativePriceVolume += trade.price * trade.volume;
		if (open < 0) {
			open = trade.price;
		}
		if (trade.price > high) {
			high = trade.price;
		}
		if (trade.price < low) {
			low = trade.price;
		}
		close = trade.price;
	}

	public BarOutput toBar() {
		BarOutput out = new BarOutput();
		out.symbol = symbol;
		out.windowStart = firstTimestamp;
		out.open = open;
		out.high = high;
		out.low = low;
		out.close = close;
		out.volume = volume;
		out.tradeCount = tradeCount;
		out.vwap = volume == 0 ? 0.0 : cumulativePriceVolume / volume;
		out.notional = cumulativePriceVolume;
		return out;
	}
}
