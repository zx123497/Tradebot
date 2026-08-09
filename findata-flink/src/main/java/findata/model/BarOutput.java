package findata.model;

import java.io.Serializable;

public class BarOutput implements Serializable {
	private static final long serialVersionUID = 1L;

	public String symbol;
	public long windowStart;
	public double open;
	public double high;
	public double low;
	public double close;
	public double volume;
	public long tradeCount;
	public double vwap;
	/** Cumulative price*volume for the bar (useful for dollar/volume bars). */
	public double notional;
}
